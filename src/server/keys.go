package server

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"math"
	"sharedis/go/util"
	. "sharedis/thrift/gen-go/sharestore"
)

type ttl_flag_type uint8
type ttl_ts_type uint32
type counter_type int64
type score_type int64
type size_t uint64

const TTL_TS_SIZE size_t = 4
const TTL_FLAG_SIZE size_t = 1
const TTL_FLAG_ENABLED ttl_flag_type = 1
const TTL_FLAG_DISABLED ttl_flag_type = 0
const kFlagSize size_t = 1
const kSegmentSize size_t = 1
const kKeyMetaSize size_t = 2
const kKeySizeLengthInBits size_t = 12
const kScoreSize size_t = 8
const kKeySize size_t = 1 << kKeySizeLengthInBits
const kZSetCountName string = "##ZSETCOUNT##"

func EncodeKey(segment string, ktype StoreType, key string, member []byte, score score_type) []byte {
	segment_size := uint8(len(segment))
	meta_size := uint16(len(key))
	member_size := uint32(len(member))
	row_key_size := kSegmentSize + size_t(segment_size) + kKeyMetaSize + size_t(meta_size) + size_t(member_size)
	if StoreType_ZSET == ktype {
		row_key_size += kScoreSize
	}
	buf := make([]byte, row_key_size)

	// encode key meta data
	buf[0] = segment_size
	copy(buf[kSegmentSize:], segment)
	meta_offset := kSegmentSize + size_t(segment_size)
	store_type := uint8(ktype)
	flags := (uint16(store_type&0x0F) << kKeySizeLengthInBits) | (meta_size & 0x0FFF)
	util.Uint16ToBytes1(buf[meta_offset:], flags)

	// encode key, score and member
	var key_offset = meta_offset + kKeyMetaSize
	var score_offset = key_offset + size_t(meta_size)
	copy(buf[key_offset:], key)
    key_offset += size_t(meta_size)
	if StoreType_ZSET == ktype {
		EncodeScore(score, score_offset, buf)
		key_offset += kScoreSize
	}

	if nil == member || 0 == len(member) {
		return buf
	}

	copy(buf[key_offset:], member)
	return buf
}

func EncodeKeyPrefix(segment string, ktype StoreType, key string) []byte {
	segment_size := uint8(len(segment))
	meta_size := uint16(len(key))
	row_key_size := kSegmentSize + size_t(segment_size) + kKeyMetaSize + size_t(meta_size)
	buf := make([]byte, row_key_size)

	buf[0] = segment_size
	copy(buf[kSegmentSize:], segment)
	meta_offset := kSegmentSize + size_t(segment_size)
	store_type := uint8(ktype)
	flags := (uint16(store_type&0x0F) << kKeySizeLengthInBits) | (meta_size & 0x0FFF)
	util.Uint16ToBytes1(buf[meta_offset:], flags)

	// encode key, score and member
	var key_offset = meta_offset + kKeyMetaSize
	copy(buf[key_offset:], key)

	return buf
}

func DecodeKey(row_key []byte) (bool, StoreType, string, string, score_type) {
	segment_size := row_key[0]
	flags, err := util.BytesToUint16(row_key[kSegmentSize+size_t(segment_size):])
	if nil != err {
		log.Error("BytesToUint32 failed")
		return false, -1, "", "", 0
	}

	key_offset := kSegmentSize + size_t(segment_size) + kKeyMetaSize
	key_size := flags & 0x0FFF
	store_type := StoreType((flags >> kKeySizeLengthInBits) & 0x0F)
	key_buf := make([]byte, key_size)
	copy(key_buf, row_key[key_offset:])
	key := string(key_buf)

	var score score_type = 0
	key_offset += size_t(key_size)
	member_size := size_t(len(row_key)) - kSegmentSize - size_t(segment_size) - kKeyMetaSize - size_t(key_size)
	if StoreType_ZSET == store_type {
		score = DecodeScore(row_key, key_offset)
		key_offset += kScoreSize
		member_size -= kScoreSize
	}

	member_buf := make([]byte, member_size)
	copy(member_buf, row_key[key_offset:])
	member := string(member_buf)
	return true, store_type, key, member, score
}

func EncodeScore(score score_type, idx size_t, buf []byte) {
	score = math.MaxInt64 - score
	buf[idx] = byte(score>>56) & 0xFF
	buf[idx+1] = byte(score>>48) & 0xFF
	buf[idx+2] = byte(score>>40) & 0xFF
	buf[idx+3] = byte(score>>32) & 0xFF
	buf[idx+4] = byte(score>>24) & 0xFF
	buf[idx+5] = byte(score>>16) & 0xFF
	buf[idx+6] = byte(score>>8) & 0xFF
	buf[idx+7] = byte(score) & 0xFF
}

func DecodeScore(buffer []byte, idx size_t) score_type {
	var score score_type = 0
	score |= score_type(buffer[idx]) << 56
	score |= score_type(buffer[idx+1]) << 48
	score |= score_type(buffer[idx+2]) << 40
	score |= score_type(buffer[idx+3]) << 32
	score |= score_type(buffer[idx+4]) << 24
	score |= score_type(buffer[idx+5]) << 16
	score |= score_type(buffer[idx+6]) << 8
	score |= score_type(buffer[idx+7])
	return math.MaxInt64 - score
}

/*func EncodeValue(value []byte, ttl_sec int32) []byte {
	var val_len = size_t(len(value))
	var ttl_ts ttl_ts_type = 0
	var buf_begin size_t = 0
	var buf_end size_t = val_len
	total_sz := val_len + TTL_FLAG_SIZE
	if ttl_sec > 0 {
		/* valid TTL *
		now := time.Now().Unix()
		if -1 == now {
			log.Errorf("time() failed")
		} else {
			total_sz  += TTL_TS_SIZE
			buf_begin += TTL_TS_SIZE
			buf_end   += TTL_TS_SIZE
			ttl_ts     = ttl_ts_type(now) + ttl_ts_type(ttl_sec)
		}
	}

	buf := make([]byte, total_sz)
	if val_len > 0 {
		copy(buf[buf_begin:], value)
	}

	if ttl_ts > 0 {
		util.Uint32ToBytes1(buf[0:], uint32(ttl_ts))
		buf[buf_end] = byte(TTL_FLAG_ENABLED)
	}

	return buf
}*/

/*func DecodeValue(value []byte) []byte {
	var val_len = size_t(len(value))
	if val_len < TTL_FLAG_SIZE {
		log.Errorf("Corrupted data in DB")
		return nil
	}

	/* TTL flag disabled *
	if value[val_len - TTL_FLAG_SIZE] == byte(TTL_FLAG_DISABLED) {
		buf := make([]byte, val_len - TTL_FLAG_SIZE)
		copy(buf[0:], value)
		return buf
	}

	/* TTL flag enabled *
	if val_len < TTL_TS_SIZE + TTL_FLAG_SIZE {
		log.Errorf("Corrupted data in DB")
		return nil
	}

	/* get current time *
	now := time.Now().Unix()
	if -1 == now {
		log.Errorf("time() failed")
		return nil
	}

	/* has this key expired? *
	ret, err := util.BytesToUint32(value)
	if nil != err {
		log.Errorf("BytesToUint32 failed")
		return nil
	}
	ttl_ts := ttl_ts_type(ret)

	if ttl_ts_type(now) > ttl_ts {
		return nil
	}

	/* if not, return the value *
	buf := make([]byte, val_len - TTL_FLAG_SIZE - TTL_TS_SIZE)
	copy(buf, value[TTL_TS_SIZE:])
	return buf
}*/

/*func DecodeTtlValue(value []byte) ttl_ts_type {
	var val_len = size_t(len(value))
	if val_len < TTL_FLAG_SIZE {
		log.Errorf("Corrupted data in DB")
		return 0
	}

	/* TTL flag disabled *
	if value[val_len - TTL_FLAG_SIZE] == byte(TTL_FLAG_DISABLED) {
		return math.MaxUint32
	}

	/* TTL flag enabled *
	if val_len < TTL_TS_SIZE + TTL_FLAG_SIZE {
		log.Errorf("Corrupted data in DB")
		return 0
	}

	/* has this key expired? *
	ret, err := util.BytesToUint32(value)
	if nil != err {
		log.Errorf("BytesToUint32 failed")
		return 0
	}
	ttl_ts := ttl_ts_type(ret)

	/* get current time *
	now := time.Now().Unix()
	if -1 == now {
		log.Errorf("time() failed")
		return 0
	}

	if ttl_ts_type(now) > ttl_ts {
		return 0
	}

	return ttl_ts - ttl_ts_type(now)
}*/

func EncodeKVKey(segment string, key string) []byte {
	return EncodeKey(segment, StoreType_KV, key, nil, 0)
}

func EncodeHashKey(segment string, key string, member string) []byte {
	return EncodeKey(segment, StoreType_HASH, key, []byte(member), 0)
}

func EncodeHashKeyPrefix(segment string, key string) []byte {
	return EncodeKeyPrefix(segment, StoreType_HASH, key)
}

func DecodeHashKey(row_key []byte, ) (bool, string, string) {
	ret, store_type, key, member, _ := DecodeKey(row_key)
	if !ret {
		return false, "", ""
	}

	if StoreType_HASH != store_type {
		log.Error("store type error",
			zap.Int64("store_type", int64(store_type)))
		return false, "", ""
	}

	return true, key, member
}

func EncodeDedupSetKey(segment string, key string, member string) []byte {
	return EncodeKey(segment, StoreType_DEDUP_SET, key, []byte(member), 0)
}

func EncodeDedupSetKeyPrefix(segment string, key string) []byte {
	return EncodeKeyPrefix(segment, StoreType_DEDUP_SET, key)
}

func DecodeDedupSetKey(rowKey []byte, ) (bool, string, string) {
	ret, storeType, key, member, _ := DecodeKey(rowKey)
	if !ret {
		return false, "", ""
	}

	if StoreType_DEDUP_SET != storeType {
		log.Error("store type error",
			zap.Int64("store_type", int64(storeType)))
		return false, "", ""
	}
	return true, key, member
}

func EncodeZSetKey(segment string, key string, member string, score score_type) []byte {
	return EncodeKey(segment, StoreType_ZSET, key, []byte(member), score)
}

func EncodeZSetIdxKey(segment string, key string, member string) []byte {
	return EncodeKey(segment, StoreType_ZSET_IDX, key, []byte(member), 0)
}

func EncodeZSetKeyPrefix(segment string, key string) []byte {
	return EncodeKeyPrefix(segment, StoreType_ZSET, key)
}

func EncodeZSetKeyWithScorePrefix(segment string, key string, score int64) []byte {
	buf := EncodeKeyPrefix(segment, StoreType_ZSET, key)
	bufSize := size_t(len(buf))
	bufWithScore := make([]byte, bufSize + kScoreSize)
	copy(bufWithScore[0:], buf)
	EncodeScore(score_type(score), bufSize, bufWithScore)
	return bufWithScore
}

func EncodeZSetIdxKeyPrefix(segment string, key string) []byte {
	return EncodeKeyPrefix(segment, StoreType_ZSET_IDX, key)
}

func DecodeZSetKey(rowKey []byte, ) (bool, string, string, int64) {
	ret, storeType, key, member, score := DecodeKey(rowKey)
	if !ret {
		return false, "", "", 0
	}

	if StoreType_ZSET != storeType {
		log.Error("store type error",
			zap.Int64("store_type", int64(storeType)))
		return false, "", "", 0
	}
	return true, key, member, int64(score)
}

func DecodeZSetIdxKey(rowKey []byte, ) (bool, string, string) {
	ret, storeType, key, member, _ := DecodeKey(rowKey)
	if !ret {
		return false, "", ""
	}

	if StoreType_ZSET_IDX != storeType {
		log.Error("store type error",
			zap.Int64("store_type", int64(storeType)))
		return false, "", ""
	}
	return true, key, member
}
