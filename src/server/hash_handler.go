package server

import (
	"context"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	. "sharedis/thrift/gen-go/sharestore"
	"strconv"
	"time"
)

// Parameters:
//  - Request
func verifyHashSetParams(request *HashSetRequest, response *HashSetResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if len(request.Members) != len(request.Values) {
		response.Error = NewSharestoreException()
		response.Error.Message = "values size is not equal to keys"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	if request.IsSetTTLSecs() && len(request.Members) != len(request.TTLSecs) {
		response.Error = NewSharestoreException()
		response.Error.Message = "ttls size is not equal to keys"
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) HashSetValue(ctx context.Context, request *HashSetRequest) (*HashSetResponse, error) {
	apiName := "hash_set"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(HashSetResponse)
	if !verifyHashSetParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	keys := make([][]byte, len(request.Members))
	values := make([][]byte, len(request.Members))
	var ttls []uint64 = nil
	if request.IsSetTTLSecs() {
		ttls = make([]uint64, len(request.Members))
	}
	for idx, dm := range request.Members {
		keys[idx] = EncodeHashKey(request.Segment, request.Key, dm)
		if request.Values[idx].IsSetStrVal() {
			values[idx] = []byte(request.Values[idx].GetStrVal())
		} else if request.Values[idx].IsSetBufVal() {
			values[idx] = request.Values[idx].GetBufVal()
		}

		if request.IsSetTTLSecs() {
			ttls[idx] = uint64(request.TTLSecs[idx])
		}
	}

	err := s.tdb.RawMSet(keys, values, ttls)
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
func verifyMultiHashSetParams(request *MultiHashSetRequest, response *MultiHashSetResponse) bool {
	if len(request.Members) != len(request.Keys) {
		for i := 0; i < len(request.Keys); i ++ {
			response.Responses[i] = new(HashSetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "memberss size is not equal to keyss"
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
		}
		log.Error("memberss size is not equal to keyss")
		return false
	}
	if len(request.Values) != len(request.Keys) {
		for i := 0; i < len(request.Keys); i ++ {
			response.Responses[i] = new(HashSetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "valuess size is not equal to keyss"
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
		}
		log.Error("valuess size is not equal to keyss")
		return false
	}
	if request.IsSetTTLSecs() && len(request.TTLSecs) != len(request.Keys) {
		for i := 0; i < len(request.Keys); i ++ {
			response.Responses[i] = new(HashSetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "ttlss size is not equal to keyss"
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
		}
		log.Error("ttlss size is not equal to keyss")
		return false
	}
	return true
}
func (s *CmdHandler) MultiHashSetValue(ctx context.Context, request *MultiHashSetRequest) (*MultiHashSetResponse, error) {
	apiName := "multi_hash_set"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(MultiHashSetResponse)
	r.Responses = make([]*HashSetResponse, len(request.Keys))
	if !verifyMultiHashSetParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}
    var ch = make(chan int)
	for i := 0; i < len(request.Keys); i++ {
		go func(idx int) {
			setR := &HashSetRequest{Segment: request.Segment, Key: request.Keys[idx],
				Members: request.Members[idx], Values: request.Values[idx]}
			if request.IsSetTTLSecs() && len(request.TTLSecs) > idx {
				setR.TTLSecs = request.TTLSecs[idx]
			}
			r.Responses[idx], _ = s.HashSetValue(ctx, setR)
            <- ch
		}(i)
        ch <- i
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyHashRemParams(request *HashRemRequest, response *HashRemResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) HashRemValue(ctx context.Context, request *HashRemRequest) (*HashRemResponse, error) {
	apiName := "hash_rem"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(HashRemResponse)
	if !verifyHashRemParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	if 0 == len(request.Members) {
		keyPre := EncodeHashKeyPrefix(request.Segment, request.Key)
		err := s.tdb.DeleteKeysByPrefix(keyPre)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}
	} else {
		del_keys := make([][]byte, len(request.Members))
		for idx, dm := range request.Members {
			del_keys[idx] = EncodeHashKey(request.Segment, request.Key, dm)
		}

		err := s.tdb.RawMDel(del_keys)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}
	}

	return r, nil
}

// Parameters:
//  - Request
func verifyMultiHashRemParams(request *MultiHashRemRequest, response *MultiHashRemResponse) bool {
	if request.IsSetTypes() && len(*request.Types) != len(request.Keys) {
		for i := 0; i < len(request.Keys); i ++ {
			response.Responses[i] = new(HashRemResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "valuess size is not equal to keys"
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
		}
		log.Error("valuess size is not equal to keys")
		return false
	}
	if request.IsSetMembers() && len(request.Members) != len(request.Keys) {
		for i := 0; i < len(request.Keys); i ++ {
			response.Responses[i] = new(HashRemResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "ttlss size is not equal to keys"
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
		}
		log.Error("ttlss size is not equal to keys")
		return false
	}
	return true
}
func (s *CmdHandler) MultiHashRemValue(ctx context.Context, request *MultiHashRemRequest) (*MultiHashRemResponse, error) {
	apiName := "multi_hash_rem"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(MultiHashRemResponse)
	r.Responses = make([]*HashRemResponse, len(request.Keys))
	if !verifyMultiHashRemParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}
    var ch = make(chan int)
	for i := 0; i < len(request.Keys); i++ {
		go func(idx int) {
			remR := &HashRemRequest{Segment: request.Segment, Key: request.Keys[idx],
				Members: request.Members[idx]}
			if request.IsSetMembers() && len(request.Members) > idx {
				remR.Members = request.Members[idx]
			}
			if request.IsSetTypes() && len(*request.Types) > idx {
				remR.Type = (*request.Types)[idx]
			}
			r.Responses[idx], _ = s.HashRemValue(ctx, remR)
            <- ch
		}(i)
        ch <- i
	}
	return r, nil
}

// Parameters:
//  - Request
func verifyHashGetParams(request *HashGetRequest, response *HashGetResponse) bool {
	if len(request.Key) > KEY_MAX_SIZE {
		response.Error = NewSharestoreException()
		response.Error.Message = "Key is too large " + strconv.Itoa(len(request.Key))
		response.Error.Code = ErrorCode_REQUEST_INVALID
		log.Error(response.Error.Message)
		return false
	}
	return true
}
func (s *CmdHandler) HashGetValue(ctx context.Context, request *HashGetRequest) (*HashGetResponse, error) {
	apiName := "hash_get"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(HashGetResponse)
	if !verifyHashGetParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}

	if 0 == len(request.Members) {
		keyPre := EncodeHashKeyPrefix(request.Segment, request.Key)
		keys, values, err := s.tdb.GetKeysByPrefix(keyPre, int(request.MaxNums))
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}

		if 0 == len(keys) {
			return r, nil
		}
		if 0 == len(values) {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = "HASH get scan key get values failed"
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}
		if len(keys) != len(values) {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = "HASH get scan key keys's size is not equal to values's"
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}

        r.Members = make([]string, len(keys))
        r.Values = make([]*HashValue, len(keys))
		for idx, dkey := range keys {
			ret, okey, member := DecodeHashKey(dkey)
			if !ret {
				continue
			}

			if okey != request.Key {
				log.Error("decoded key is erroe",
					zap.String("okey", request.Key),
					zap.String("dkey", string(dkey)))
				continue
			}

			r.Members[idx] = member
			hv := new(HashValue)
			if HashType_STR == request.Type {
                hv.StrVal = new(string)
				*hv.StrVal = string(values[idx])
			} else if HashType_BUF == request.Type {
				hv.BufVal = values[idx]
			}
			r.Values[idx] = hv
		}
	} else {
		keys := make([][]byte,len(request.Members))
		for idx, dm := range request.Members {
			keys[idx] = EncodeHashKey(request.Segment, request.Key, dm)
		}

		values, err := s.tdb.RawMGet(keys)
		if nil != err {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = err.Error()
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}

		if nil == values || len(request.Members) != len(values) {
			apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
			r.Error = NewSharestoreException()
			r.Error.Message = "values is invalid"
			r.Error.Code = ErrorCode_ROCKSDB_ERROR
			return r, err
		}
        
        r.Members = make([]string, len(request.Members))
        r.Values = make([]*HashValue, len(request.Members))
		for idx, dm := range request.Members {
			r.Members[idx] = dm
			hv := new(HashValue)
			if HashType_STR == request.Type {
                hv.StrVal = new(string)
				*hv.StrVal = string(values[idx])
			} else if HashType_BUF == request.Type {
				hv.BufVal = values[idx]
			}
			r.Values[idx] = hv
		}
	}

	return r, nil
}

// Parameters:
//  - Request
func verifyMultiHashGetParams(request *MultiHashGetRequest, response *MultiHashGetResponse) bool {
	if len(request.Types) != len(request.Keys) {
		for i := 0; i < len(request.Keys); i ++ {
			response.Responses[i] = new(HashGetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "valuess size is not equal to keys"
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
		}
		log.Error("valuess size is not equal to keys")
		return false
	}
	if request.IsSetMembers() && len(request.Members) != len(request.Keys) {
		for i := 0; i < len(request.Keys); i ++ {
			response.Responses[i] = new(HashGetResponse)
			response.Responses[i].Error = NewSharestoreException()
			response.Responses[i].Error.Message = "ttlss size is not equal to keys"
			response.Responses[i].Error.Code = ErrorCode_REQUEST_INVALID
		}
		log.Error("ttlss size is not equal to keys")
		return false
	}
	return true
}
func (s *CmdHandler) MultiHashGetValue(ctx context.Context, request *MultiHashGetRequest) (*MultiHashGetResponse, error) {
	apiName := "multi_hash_get"
	apiCounterVec.WithLabelValues(apiName, request.Segment).Inc()
	apiCounterVec.WithLabelValues(apiName, "sum").Inc()
	tStart := time.Now().UnixNano()
	defer func() {
		tEnd := time.Now().UnixNano()
		cost := float64(tEnd - tStart) / 1000000.0
		apiMs.WithLabelValues(apiName, request.Segment).Observe(cost)
		apiMs.WithLabelValues(apiName, "sum").Observe(cost)
	}()

	r := new(MultiHashGetResponse)
	r.Responses = make([]*HashGetResponse, len(request.Keys))
	if !verifyMultiHashGetParams(request, r) {
		apiCounterFailedVec.WithLabelValues(apiName, request.Segment).Inc()
		return r, nil
	}
    var ch = make(chan int)
	for i := 0; i < len(request.Keys); i++ {
		go func(idx int) {
			getR := &HashGetRequest{Segment: request.Segment, Key: request.Keys[idx],
				Type: request.Types[idx], MaxNums: request.MaxNums}
			if request.IsSetMembers() && len(request.Members) > idx {
				getR.Members = request.Members[idx]
			}
			r.Responses[idx], _ = s.HashGetValue(ctx, getR)
            <- ch
		}(i)
        ch <- i
	}
	return r, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) HashCountValue(ctx context.Context, request *HashCountRequest) (*HashCountResponse, error) {
	return nil, nil
}

