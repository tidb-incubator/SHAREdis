package server

import (
	"bytes"
	"context"
	"errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sharedis/go/util"
	. "sharedis/thrift/gen-go/sharestore"
	"strconv"
	"time"
)

func (s *CmdHandler) getMembersScores(segment string, key string, members []string) (map[string]int64, error) {
	if 0 == len(members) {
		return nil, nil
	}

	retMap := make(map[string]int64)
	idxkeys := make([][]byte, len(members))
	for idx, zm := range members {
		idxkeys[idx] = EncodeZSetIdxKey(segment, key, zm)
	}

	scores, err := s.tdb.RawMGet(idxkeys)
	if nil != err {
		log.Error("ZS get member-score error",
			zap.String("segment", segment),
			zap.String("key", key),
			zap.Error(err))
		return nil, err
	}

	if len(scores) != len(members) {
		log.Error("ZS get member-scores invalid",
			zap.String("segment", segment),
			zap.String("key", key))
		return nil, errors.New("get member-scores invalid")
	}

	for idx, zm := range members {
		if 0 == len(scores[idx]) {
			continue
		}
		score, _ := util.BytesToInt64(scores[idx])
		retMap[zm] = score
	}
	return retMap, nil
}

func (s *CmdHandler) getRangeMembersScores(segment string, key string,
	sr *ScoreRange, maxNums int32) (map[string]int64, error) {
	retMap := make(map[string]int64)
	// zset store scores by desc
	startKey := EncodeZSetKeyWithScorePrefix(segment, key, sr.Max)
	endKey := s.tdb.GetIncOneKeys(EncodeZSetKeyWithScorePrefix(segment, key, sr.Min))
	keys, _, err := s.tdb.Scan(startKey, endKey, int(maxNums))
	if nil != err {
		log.Error("ZS get scan range key error",
			zap.String("segment", segment),
			zap.String("key", key),
			zap.Error(err))
		return nil, err
	}
	if 0 == len(keys) {
		return retMap, nil
	}
	for _, dkey := range keys {
		ret, okey, member, score := DecodeZSetKey(dkey)
		if !ret {
			continue
		}
		if okey != key {
			log.Error("decoded key is error",
				zap.String("okey", key),
				zap.String("dkey", string(dkey)))
			continue
		}
		retMap[member] = score
	}
	return retMap, nil
}

// Parameters:
//  - Request
func verifyZsetAddParams(request *ZSetAddRequest, response *ZSetAddResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if 0 == len(request.Members) {
		response.Error = NewSharestoreException()
		response.Error.Message = "members size is zero"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if len(request.Members) != len(request.Scores) {
		response.Error = NewSharestoreException()
		response.Error.Message = "members size is not equal to scores"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if request.IsSetTTLSecs() && len(request.Members) != len(request.TTLSecs) {
		response.Error = NewSharestoreException()
		response.Error.Message = "members size is not equal to ttls"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	for score := range request.Scores{
		if score < 0 {
			response.Error = NewSharestoreException()
			response.Error.Message = "all scores must be greater than or equal to zero"
			response.Error.Code = ErrorCode_REQUEST_INVALID
			log.Error(response.Error.Message)
			return false
		}
	}
	return true
}
func (s *CmdHandler) ZsetAddValue(ctx context.Context, request *ZSetAddRequest) (*ZSetAddResponse, error) {
	apiName := "api_zset_add"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(ZSetAddResponse)
	if !verifyZsetAddParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	ksMap := make(map[string]int64)
	for idx, zm := range request.Members {
		ksMap[zm] = request.Scores[idx]
	}
	member2scores, err := s.getMembersScores(request.Segment, request.Key, request.Members)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		return r, err
	}

	bothkeys := make([][]byte, len(request.Members) * 2)
	bothvalues := make([][]byte, len(request.Members) * 2)
	var ttls []uint64 = nil
	if request.IsSetTTLSecs() {
		ttls = make([]uint64, len(request.Members) * 2)
	}
	for idx, zm := range request.Members {
		bothkeys[idx * 2] = EncodeZSetIdxKey(request.Segment, request.Key, zm)
		bothvalues[idx * 2], _ = util.Int64ToBytes(request.Scores[idx])
		bothkeys[idx * 2 + 1] = EncodeZSetKey(request.Segment, request.Key,
			zm, score_type(request.Scores[idx]))
		// tikv doesn't support empty value, so we must set a space
		// although zset doesn't need value
		bothvalues[idx * 2 + 1] = []byte(" ")
		ttls[idx * 2] = uint64(request.TTLSecs[idx])
		ttls[idx * 2 + 1] = uint64(request.TTLSecs[idx])
	}

	delkeys := make([][]byte, 0)
	for zm, score := range member2scores {
		if score == ksMap[zm] {
			continue
		}
		delkeys = append(delkeys, EncodeZSetKey(request.Segment, request.Key,
			zm, score_type(score)))
	}

	if len(delkeys) > 0 {
		err = s.tdb.RawMDel(delkeys)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			log.Error("ZS rm old score error",
				zap.String("segment", request.Segment),
				zap.String("key", request.Key),
				zap.Error(err))
			return r, err
		}
	}

	err = s.tdb.RawMSet(bothkeys, bothvalues, ttls)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("ZS set error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyZsetRemParams(request *ZSetRemRequest, response *ZSetRemResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) ZsetRemValue(ctx context.Context, request *ZSetRemRequest) (*ZSetRemResponse, error) {
	apiName := "api_zset_rem"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(ZSetRemResponse)
	if !verifyZsetRemParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	if 0 == len(request.Members) {
		idxKeyPrefix := EncodeZSetIdxKeyPrefix(request.Segment, request.Key)
		err := s.tdb.DeleteKeysByPrefix(idxKeyPrefix)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			log.Error("ZS del idxKey error",
				zap.String("segment", request.Segment),
				zap.String("key", request.Key),
				zap.Error(err))
			return r, err
		}
		scoreKeyPrefix := EncodeZSetKeyPrefix(request.Segment, request.Key)
		err = s.tdb.DeleteKeysByPrefix(scoreKeyPrefix)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			log.Error("ZS del scoreKey error",
				zap.String("segment", request.Segment),
				zap.String("key", request.Key),
				zap.Error(err))
			return r, err
		}
	} else {
		member2scores, err := s.getMembersScores(request.Segment, request.Key, request.Members)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}

		delkeys := make([][]byte, 0)
		for zm, score := range member2scores {
			delkeys = append(delkeys, EncodeZSetKey(request.Segment, request.Key,
				zm, score_type(score)))
		}

		for _, zm := range request.Members {
			delkeys = append(delkeys, EncodeZSetIdxKey(request.Segment, request.Key,
				zm))
		}

		err = s.tdb.RawMDel(delkeys)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			log.Error("ZS rm old score error",
				zap.String("segment", request.Segment),
				zap.String("key", request.Key),
				zap.Error(err))
			return r, err
		}
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyZsetGetParams(request *ZSetGetRequest, response *ZSetGetResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) ZsetGetValue(ctx context.Context, request *ZSetGetRequest) (*ZSetGetResponse, error) {
	apiName := "api_zset_get"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(ZSetGetResponse)
	if !verifyZsetGetParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	if 0 == len(request.Members) {
		idxKeyPrefix := EncodeZSetIdxKeyPrefix(request.Segment, request.Key)
		keys, values, err := s.tdb.GetKeysByPrefix(idxKeyPrefix, int(request.MaxNums))
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			log.Error("ZS get scan key error",
				zap.String("segment", request.Segment),
				zap.String("key", request.Key),
				zap.Error(err))
			return r, err
		}
		if 0 == len(keys) {
			return r, nil
		}
		if 0 == len(values) {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = "ZS get scan key get values failed"
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}
		if len(keys) != len(values) {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = "ZS get scan key keys's size is not equal to values's"
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}
		r.Members = make([]string, 0)
		if request.WithScores {
			r.Scores = make([]int64, 0)
		}
		for idx, dkey := range keys {
			ret, okey, member := DecodeZSetIdxKey(dkey)
			if !ret {
				continue
			}

			if okey != request.Key {
				log.Error("decoded key is error",
					zap.String("okey", request.Key),
					zap.String("dkey", string(dkey)))
				continue
			}

			r.Members = append(r.Members, member)
			if request.WithScores {
				score, _ := util.BytesToInt64(values[idx])
				r.Scores = append(r.Scores, score)
			}
		}
	} else {
		member2scores, err := s.getMembersScores(request.Segment, request.Key, request.Members)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}

		r.Members = make([]string, 0)
		if request.WithScores {
			r.Scores = make([]int64, 0)
		}
		for zm, score := range member2scores {
			r.Members = append(r.Members, zm)
			if request.WithScores {
				r.Scores = append(r.Scores, score)
			}
		}
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyZsetGetRangeParams(request *ZSetGetRangeRequest, response *ZSetGetRangeResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if !request.IsSetScoreRange() {
		response.Error = NewSharestoreException()
		response.Error.Message = "ZSetGetRange must set score_range "
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if request.ScoreRange.Min > request.ScoreRange.Max {
		response.Error = NewSharestoreException()
		response.Error.Message = "ZSetGetRange score_range mix is greater than max"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) ZsetGetRangeValue(ctx context.Context, request *ZSetGetRangeRequest) (*ZSetGetRangeResponse, error) {
	apiName := "api_zset_get_range"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(ZSetGetRangeResponse)
	if !verifyZsetGetRangeParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	member2scores, err := s.getRangeMembersScores(request.Segment, request.Key,
		request.ScoreRange, request.MaxNums)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		return r, err
	}

	r.Members = make([]string, 0)
	if request.WithScores {
		r.Scores = make([]int64, 0)
	}
	for zm, score := range member2scores {
		r.Members = append(r.Members, zm)
		if request.WithScores {
			r.Scores = append(r.Scores, score)
		}
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyZsetRemRangeParams(request *ZSetRemRangeRequest, response *ZSetRemRangeResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if !request.IsSetScoreRange() {
		response.Error = NewSharestoreException()
		response.Error.Message = "ZSetGetRange must set score_range "
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if request.ScoreRange.Min > request.ScoreRange.Max {
		response.Error = NewSharestoreException()
		response.Error.Message = "ZSetGetRange score_range mix is greater than max"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) ZsetRemRangeValue(ctx context.Context, request *ZSetRemRangeRequest) (*ZSetRemRangeResponse, error) {
	apiName := "api_zset_rem_range"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(ZSetRemRangeResponse)
	if !verifyZsetRemRangeParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}
	member2scores, err := s.getRangeMembersScores(request.Segment, request.Key,
		request.ScoreRange, 5000)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		return r, err
	}

	delkeys := make([][]byte, 0)
	for zm, score := range member2scores {
		delkeys = append(delkeys, EncodeZSetKey(request.Segment, request.Key,
			zm, score_type(score)))
		delkeys = append(delkeys, EncodeZSetIdxKey(request.Segment, request.Key,
			zm))
	}
	if 0 == len(delkeys) {
		return r, nil
	}
	err = s.tdb.RawMDel(delkeys)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("ZS rm range score error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyZsetCountParams(request *ZSetCountRequest, response *ZSetCountResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) ZsetCountValue(ctx context.Context, request *ZSetCountRequest) (*ZSetCountResponse, error) {
	apiName := "api_zset_count"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(ZSetCountResponse)
	if !verifyZsetCountParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	keyPre := EncodeZSetIdxKeyPrefix(request.Segment, request.Key)
	keys, _, err := s.tdb.GetKeysByPrefix(keyPre, int(request.MaxNums))
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("ZSET count error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	r.Count = int32(len(keys))
	return r, nil
}

// Parameters:
//  - Request
func verifyZsetScanParams(request *ZSetScanRequest, response *ZSetScanResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) ZsetScanValue(ctx context.Context, request *ZSetScanRequest) (*ZSetScanResponse, error) {
	apiName := "api_zset_scan"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(ZSetScanResponse)
	if !verifyZsetScanParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	var keys [][]byte
	var err error
	var cursor []byte
	var cursorEnd []byte
	if request.IsSetCursor() && len(request.Cursor) > 0 {
		cursor = request.Cursor
	} else {
		cursor = EncodeZSetKeyPrefix(request.Segment, request.Key)
		if request.IsReverse {
			cursor = s.tdb.GetIncOneKeys(cursor)
		}
	}
	if request.IsReverse {
		cursorEnd = EncodeZSetKeyPrefix(request.Segment, request.Key)
	} else {
		cursorEnd = s.tdb.GetIncOneKeys(EncodeZSetKeyPrefix(request.Segment, request.Key))
	}

	if request.IsReverse {
		keys, _, err = s.tdb.ReverseScan(cursor, cursorEnd, int(request.MaxNums))
	} else {
		keys, _, err = s.tdb.Scan(cursor, cursorEnd, int(request.MaxNums))
	}
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("ZSET scan error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	if 0 == len(keys) {
		return r, nil
	}
	var keysMax []byte = nil
	var keysMix []byte = nil
	r.Members = make([]string, 0)
	if request.WithScores {
		r.Scores = make([]int64, 0)
	}
	for _, dkey := range keys {
		ret, okey, member, score := DecodeZSetKey(dkey)
		if !ret {
			continue
		}
		if okey != request.Key {
			log.Error("decoded key is error",
				zap.String("okey", request.Key),
				zap.String("dkey", string(dkey)))
			continue
		}
		r.Members = append(r.Members, member)
		if request.WithScores {
			r.Scores = append(r.Scores, score)
		}
		if nil == keysMax || bytes.Compare(keysMax, dkey) < 0 {
			keysMax = dkey
		}
		if nil == keysMix || bytes.Compare(keysMix, dkey) > 0 {
			keysMix = dkey
		}
	}
	if request.IsReverse {
		r.Cursor = keysMix
	} else if len(keysMax) > 0 {
		buf := make([]byte, len(keysMax) + 1)
		copy(buf[0:], keysMax)
		buf[len(keysMax)] = 0
		r.Cursor = buf
	}
	return r, nil
}
