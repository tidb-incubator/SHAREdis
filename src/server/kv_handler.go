package server

import (
	"context"
	"github.com/pingcap/log"
	. "sharedis/thrift/gen-go/sharestore"
	"strconv"
	"time"
)

const KEY_MAX_SIZE = 4096

// Parameters:
//  - Request
func verifyGetValueParams(request *GetRequest, response *GetResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) GetValue(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	apiName := "api_get"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(GetResponse)
	if !verifyGetValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	encodeKey := EncodeKVKey(request.Segment, request.Key)
	v, err := s.tdb.RawGet(encodeKey)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		return r, err
	}

	if nil == v {
		apiCounterNFVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = "Key= " + request.Key
		r.Error.Code = ErrorCode_KEY_NOT_FOUND
		return r, nil
	}

	r.Value = v
	return r, nil
}

// Parameters:
//  - Request
func verifyTtlValueParams(request *TtlRequest, response *TtlResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) TTL(ctx context.Context, request *TtlRequest) (*TtlResponse, error) {
	apiName := "api_ttl"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(TtlResponse)
	if !verifyTtlValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	encodeKey := EncodeKVKey(request.Segment, request.Key)
	v, err := s.tdb.RawGetKeyTTL(encodeKey)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		return r, err
	}

	if -2 == v {
		apiCounterNFVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = "Key= " + request.Key
		r.Error.Code = ErrorCode_KEY_NOT_FOUND
		return r, nil
	}

	r.TTLSec = v
	return r, nil
}

// Parameters:
//  - Request
func verifySetValueParams(request *SetRequest, response *SetResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) SetValue(ctx context.Context, request *SetRequest) (*SetResponse, error) {
	apiName := "api_set"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(SetResponse)
	if !verifySetValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	encodeKey := EncodeKVKey(request.Segment, request.Key)
	err := s.tdb.RawSet(encodeKey, request.Value, request.TTLSec)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		return r, err
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyDelValueParams(request *DelRequest, response *DelResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) DelValue(ctx context.Context, request *DelRequest) (*DelResponse, error) {
	apiName := "api_del"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(DelResponse)
	if !verifyDelValueParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	encodeKey := EncodeKVKey(request.Segment, request.Key)
	err := s.tdb.RawDel(encodeKey)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		r.Error = NewSharestoreException()
		r.Error.Message = err.Error()
		r.Error.Code = ErrorCode_ROCKSDB_ERROR
		return r, err
	}
	return r, nil
}

// Parameters:
//  - Mrequest
func verifyMGetValueParams(mrequest *MultiGetRequest, response *MultiGetResponse) int {
	ret := 0
	for i, key := range mrequest.Keys {
		if len(key) > KEY_MAX_SIZE {
			response.Responses[i] = new(GetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "Key is too large " + strconv.Itoa(len(key))
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
			log.Error(response.Responses[i].Error.Message)
			ret ++
		}
	}
	return ret
}
func (s *CmdHandler) MultiGetValue(ctx context.Context, mrequest *MultiGetRequest) (*MultiGetResponse, error) {
	apiName := "api_multi_get"
	apiCounterVec.WithLabelValues(apiName, mrequest.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, mrequest.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(MultiGetResponse)
	r.Responses = make([]*GetResponse, len(mrequest.Keys))
	num := verifyMGetValueParams(mrequest, r)
	if num > 0 {
		apiCounterFailedVec.WithLabelValues(apiName, mrequest.Segment).Add(float64(num))
	}
	if len(mrequest.Keys) == num {
		return nil, nil
	}

	keys := make([][]byte, len(mrequest.Keys) - num)
	v_idx := 0
	for i, key := range mrequest.Keys {
		if nil == r.Responses[i] {
			keys[v_idx] = EncodeKVKey(mrequest.Segment, key)
			v_idx ++
		}
	}
	values, err := s.tdb.RawMGet(keys)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, mrequest.Segment).Inc()
	}
	v_idx = 0
	for i, key := range mrequest.Keys {
		if nil != r.Responses[i] {
		    continue
		}
		r.Responses[i] = new(GetResponse)
		if nil != err {
			r.Responses[i].Error = NewSharestoreException()
			r.Responses[i].Error.Message = err.Error()
			r.Responses[i].Error.Code = ErrorCode_ROCKSDB_ERROR
			v_idx ++
		} else if nil == values || v_idx >= len(values) {
			r.Responses[i].Error = NewSharestoreException()
			r.Responses[i].Error.Message = "rocksdb error"
			r.Responses[i].Error.Code = ErrorCode_ROCKSDB_ERROR
			v_idx ++
		} else {
			if nil == values[v_idx] {
				apiCounterNFVec.WithLabelValues(apiName, mrequest.Segment).Inc()
				r.Responses[i].Error = NewSharestoreException()
				r.Responses[i].Error.Message = "Key= " + key
				r.Responses[i].Error.Code = ErrorCode_KEY_NOT_FOUND
				v_idx ++
				continue
			}

			r.Responses[i].Value = values[v_idx]
			v_idx ++
		}
	}
	
	return r, nil
}

// Parameters:
//  - Msequest
func verifyMSetValueParams(mrequest *MultiSetRequest, response *MultiSetResponse) int {
	ret := 0
	for i, key := range mrequest.Keys {
		if len(key) > KEY_MAX_SIZE {
			response.Responses[i] = new(SetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "Key is too large " + strconv.Itoa(len(key))
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
			log.Error(response.Responses[i].Error.Message)
			ret ++
		} else if len(mrequest.Keys) != len(mrequest.Values) {
			response.Responses[i] = new(SetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "values size is not equal keys "
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
			ret ++
		} else if mrequest.IsSetTTLSecs() && len(mrequest.Keys) != len(mrequest.TTLSecs) {
			response.Responses[i] = new(SetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "ttls size is not equal keys "
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
			ret ++
		}
	}
	return ret
}
func (s *CmdHandler) MultiSetValue(ctx context.Context, msequest *MultiSetRequest) (*MultiSetResponse, error) {
	apiName := "api_multi_set"
	apiCounterVec.WithLabelValues(apiName, msequest.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, msequest.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)}()

	r := new(MultiSetResponse)
	r.Responses = make([]*SetResponse, len(msequest.Keys))
	num := verifyMSetValueParams(msequest, r)
	if num > 0 {
		apiCounterFailedVec.WithLabelValues(apiName, msequest.Segment).Add(float64(num))
	}
	if len(msequest.Keys) <= num {
		return nil, nil
	}

	/*keys := make([][]byte, len(msequest.Keys) - num)
	values := make([][]byte, len(msequest.Keys) - num)
	v_idx := 0
	for i, key := range msequest.Keys {
		if nil == r.Responses[i] {
			keys[v_idx] = EncodeKVkey(mrequest.Segment, key)
			values[v_idx] = EncodeValue(msequest.Values[i], msequest.TTLSecs[i])
			v_idx ++
		}
	}
	err := s.tdb.RawMSet(keys, values)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, msequest.Segment).Inc()
	}
	for i, _ := range msequest.Keys {
		if nil != r.Responses[i] {
			continue
		}
		r.Responses[i] = new(SetResponse)
		if nil != err {
			r.Responses[i].Error = NewSharestoreException()
			r.Responses[i].Error.Message = err.Error()
			r.Responses[i].Error.Code = ErrorCode_ROCKSDB_ERROR
		}
	}*/
	/*for i := 0; i < len(msequest.Keys); i++ {
		if nil == r.Responses[i] {
			go func() {
				setR := &SetRequest{Segment: msequest.Segment, Key: msequest.Keys[i], Value: msequest.Values[i]}
				if len(msequest.TTLSecs) > i {
					setR.TTLSec = msequest.TTLSecs[i]
				}
				r.Responses[i], _ = s.SetValue(ctx, setR)
			}()
		}
	}*/
	keys := make([][]byte, len(msequest.Keys))
	values := make([][]byte, len(msequest.Keys))
	var ttls []uint64 = nil
	if msequest.IsSetTTLSecs() {
		ttls = make([]uint64, len(msequest.Keys))
	}
	for idx, km := range msequest.Keys {
		keys[idx] = EncodeKVKey(msequest.Segment, km)
		values[idx] = msequest.Values[idx]
		if msequest.IsSetTTLSecs() {
			ttls[idx] = uint64(msequest.TTLSecs[idx])
		}
	}

	err := s.tdb.RawMSet(keys, values, ttls)
	if nil != err {
		apiCounterFailedVec.WithLabelValues(apiName, msequest.Segment).Inc()
	}
	for i, _ := range msequest.Keys {
		if nil != r.Responses[i] {
			continue
		}
		r.Responses[i] = new(SetResponse)
		if nil != err {
			r.Responses[i].Error = NewSharestoreException()
			r.Responses[i].Error.Message = err.Error()
			r.Responses[i].Error.Code = ErrorCode_ROCKSDB_ERROR
		}
	}
	return r, nil
}
