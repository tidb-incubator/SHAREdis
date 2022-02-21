package sharedis

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sharedis/terror"
)

func (sharedis *Sharedis) RawGet(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}
	v, err := sharedis.dbRaw.Get(key)

	if err != nil {
		return nil, err
	}
	// key not exist, return asap
	if v == nil {
		return nil, nil
	}

	return v, nil
}

func (sharedis *Sharedis) RawGetKeyTTL(key []byte) (int32, error) {
	if len(key) == 0 {
		return -1, terror.ErrKeyEmpty
	}
	return sharedis.dbRaw.GetKeyTTL(key)
}

func (sharedis *Sharedis) RawMGet(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}
	v, err := sharedis.dbRaw.MGet(keys)

	if err != nil {
		return nil, err
	}
	// key not exist, return asap
	if v == nil {
		return nil, nil
	}

	return v, nil
}

func (sharedis *Sharedis) RawSet(key []byte, value []byte, ttl int32) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	err := sharedis.dbRaw.Set(key, value, ttl)
	if err != nil {
		log.Error("set error", zap.Error(err))
		return err
	}
	return nil
}

func (sharedis *Sharedis) RawMSet(keys [][]byte, values [][]byte, ttls []uint64) error {
	if len(keys) == 0 {
		return terror.ErrKeyEmpty
	}

	if len(keys) != len(values) {
		return terror.ErrCmdParams
	}

	err := sharedis.dbRaw.MSet(keys, values, ttls)
	return err
}

func (sharedis *Sharedis) RawDel(key []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	err := sharedis.dbRaw.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (sharedis *Sharedis) RawMDel(keys [][]byte) error {
	if len(keys) == 0 {
		return terror.ErrKeyEmpty
	}

	err := sharedis.dbRaw.MDelete(keys)
	return err
}

func (sharedis *Sharedis) GetKeysByPrefix(keyPrefix []byte, max_cnt int) ([][]byte, [][]byte, error) {
	if len(keyPrefix) == 0 {
		return nil, nil, terror.ErrKeyEmpty
	}

	keys, values, err := sharedis.dbRaw.Scan(keyPrefix, sharedis.GetIncOneKeys(keyPrefix), max_cnt)
	if nil != err {
		return nil, nil, err
	}

	return keys, values, nil
}

func (sharedis *Sharedis) DeleteKeysByPrefix(keyPrefix []byte) error {
	if len(keyPrefix) == 0 {
		return terror.ErrKeyEmpty
	}

	err := sharedis.dbRaw.RangeDelete(keyPrefix, sharedis.GetIncOneKeys(keyPrefix))
	if nil != err {
		return err
	}

	return nil
}

func (sharedis *Sharedis) Scan(startKey []byte, endKey []byte, max_cnt int) ([][]byte, [][]byte, error) {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil, nil, terror.ErrKeyEmpty
	}

	keys, values, err := sharedis.dbRaw.Scan(startKey, endKey, max_cnt)
	if nil != err {
		return nil, nil, err
	}

	return keys, values, nil
}

func (sharedis *Sharedis) ReverseScan(startKey []byte, endKey []byte, max_cnt int) ([][]byte, [][]byte, error) {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil, nil, terror.ErrKeyEmpty
	}

	keys, values, err := sharedis.dbRaw.ReverseScan(startKey, endKey, max_cnt)
	if nil != err {
		return nil, nil, err
	}

	return keys, values, nil
}

func (sharedis *Sharedis) GetIncOneKeys(keyPrefix []byte) []byte {
	if 0 == len(keyPrefix) {
		return nil
	}
	keyLen := len(keyPrefix)
	buf := make([]byte, keyLen)
	copy(buf, keyPrefix)
	for ; keyLen > 0; keyLen-- {
		var value = buf[keyLen - 1]
		if 255 == value {
			buf[keyLen - 1] = 0
		} else {
			buf[keyLen - 1] = value + 1
			break
		}
	}
	return buf
}
