package server

import (
	"context"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	. "sharedis/thrift/gen-go/sharestore"
	"strconv"
	"time"
)

func verifyDsSetValueParams(request *DSSetRequest, response *DSSetResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if 0 == len(request.Members) {
		response.Error = NewSharestoreException()
		response.Error.Message = "members is empty"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if len(request.Members) != len(request.TTLSecs) {
		response.Error = NewSharestoreException()
		response.Error.Message = "ttls size is not equal to members"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}

// Parameters:
//  - Request
func (s *CmdHandler) DsSetValue(ctx context.Context, request *DSSetRequest) (*DSSetResponse, error) {
	apiName := "api_ds_set"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(DSSetResponse)
	if !verifyDsSetValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	keys := make([][]byte, len(request.Members))
	values := make([][]byte, len(request.Members))
	ttls := make([]uint64, len(request.Members))
	for idx, dm := range request.Members {
        keys[idx] = EncodeDedupSetKey(request.Segment, request.Key, dm)
        // tikv doesn't support empty value, so we must set a space
        // although dset doesn't need value
        values[idx] = []byte(" ")
		ttls[idx] = uint64(request.TTLSecs[idx])
	}

	err := s.tdb.RawMSet(keys, values, ttls)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("DS set error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	return r, nil
}

func verifyDsRemValueParams(request *DSRemRequest, response *DSRemResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}

	if 0 == len(request.Members) {
		response.Error = NewSharestoreException()
		response.Error.Message = "members is empty"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}

// Parameters:
//  - Request
func (s *CmdHandler) DsRemValue(ctx context.Context, request *DSRemRequest) (*DSRemResponse, error) {
	apiName := "api_ds_rem"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(DSRemResponse)
	if !verifyDsRemValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	delKeys := make([][]byte, len(request.Members))
	for idx, dm := range request.Members {
		delKeys[idx] = EncodeDedupSetKey(request.Segment, request.Key, dm)
	}

	err := s.tdb.RawMDel(delKeys)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("DS rem error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}

	return r, nil
}

func verifyDsGetValueParams(request *DSGetRequest, response *DSGetResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}

// Parameters:
//  - Request
func (s *CmdHandler) DsGetValue(ctx context.Context, request *DSGetRequest) (*DSGetResponse, error) {
	apiName := "api_ds_get"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(DSGetResponse)
	if !verifyDsGetValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	keyPre := EncodeDedupSetKeyPrefix(request.Segment, request.Key)
	keys, _, err := s.tdb.GetKeysByPrefix(keyPre, int(request.MaxNums))
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("DS get error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	if nil == keys {
		return r, nil
	}
	for _, encodeKey := range keys {
		ret, key, member := DecodeDedupSetKey(encodeKey)
		if !ret {
			continue
		}
		if key != request.Key {
			log.Error("decoded key is error",
				zap.String("okey", request.Key),
				zap.String("dkey", string(encodeKey)))
			continue
		}
		r.Members = append(r.Members, member)
	}

	if request.WithTtls {
		// TODO: to be optimized, mget ttls
		for _, encodeKey := range keys {
			ttl, err := s.tdb.RawGetKeyTTL(encodeKey)
			if nil != err {
				apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
				r.Error = NewSharestoreException()
				r.Error.Message = err.Error()
				r.Error.Code = ErrorCode_ROCKSDB_ERROR
				return r, err
			}
			r.TTLSecs = append(r.TTLSecs, ttl)
		}
	}

	return r, nil
}

func verifyDsCountValueParams(request *DSCountRequest, response *DSCountResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}

// Parameters:
//  - Request
func (s *CmdHandler) DsCountValue(ctx context.Context, request *DSCountRequest) (*DSCountResponse, error) {
	apiName := "api_ds_count"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(DSCountResponse)
	if !verifyDsCountValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	keyPre := EncodeDedupSetKeyPrefix(request.Segment, request.Key)
	keys, _, err := s.tdb.GetKeysByPrefix(keyPre, int(request.MaxNums))
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("DS count error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	r.Count = int32(len(keys))

	return r, nil
}

func verifyDsDelValueParams(request *DSDelRequest, response *DSDelResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}

// Parameters:
//  - Request
func (s *CmdHandler) DsDelValue(ctx context.Context, request *DSDelRequest) (*DSDelResponse, error) {
	apiName := "api_ds_del"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(DSDelResponse)
	if !verifyDsDelValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	keyPre := EncodeDedupSetKeyPrefix(request.Segment, request.Key)
	err := s.tdb.DeleteKeysByPrefix(keyPre)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("DS del error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}

	return r, nil
}

func verifyDsIsMemberParams(request *DSIsMemberRequest, response *DSIsMemberResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}

// Parameters:
//  - Request
func (s *CmdHandler) DsIsMember(ctx context.Context, request *DSIsMemberRequest) (*DSIsMemberResponse, error) {
	apiName := "api_ds_is_member"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd-tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(DSIsMemberResponse)
	if !verifyDsIsMemberParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	encodeKey := EncodeDedupSetKey(request.Segment, request.Key, request.Member)
	v, err := s.tdb.RawGet(encodeKey)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		log.Error("DS ismember error",
			zap.String("segment", request.Segment),
			zap.String("key", request.Key),
			zap.Error(err))
		return r, err
	}
	if nil == v {
		r.Yes = false
	} else {
		r.Yes = true
	}

	return r, nil
}
