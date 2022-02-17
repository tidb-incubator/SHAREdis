package tikv

import (
	"context"
	"github.com/pingcap/log"
	tk_config "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"
	sd_config "sharedis/config"
	"strings"
)

type TikvRaw struct {
	store *rawkv.Client
}

func OpenRaw(conf *sd_config.Config) (*TikvRaw, error) {
	cli, err := rawkv.NewClient(context.TODO(),
		strings.Split(conf.Backend.Pds, ","),
		tk_config.DefaultConfig().Security)
	if err != nil {
		return nil, err
	}
	return &TikvRaw{store: cli}, nil
}

func (tikv *TikvRaw) Close() error {
	return tikv.store.Close()
}

func (tikv *TikvRaw) Get(key []byte) ([]byte, error) {
	val, err := tikv.store.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (tikv *TikvRaw) MGet(keys [][]byte) ([][]byte, error) {
	vals, err := tikv.store.BatchGet(context.TODO(), keys)
	if err != nil {
		return nil, err
	}
	return vals, nil
}

func (tikv *TikvRaw) GetKeyTTL(key []byte) (int32, error) {
	val, err := tikv.store.GetKeyTTL(context.TODO(), key)
	if err != nil {
		log.Error("set error", zap.Error(err))
		return -1, err
	}
	if nil == val {
		return -2, nil
	}
	return int32(*val), nil
}

func (tikv *TikvRaw) Set(key []byte, value []byte, ttl int32) error {
	return tikv.store.PutWithTTL(context.TODO(), key, value, uint64(ttl))
}

func (tikv *TikvRaw) MSet(keys [][]byte, values [][]byte, ttls []uint64) error {
	return tikv.store.BatchPut(context.TODO(), keys, values, ttls)
}

func (tikv *TikvRaw) Delete(key []byte) (error) {
	return tikv.store.Delete(context.TODO(), key)
}

func (tikv *TikvRaw) MDelete(keys [][]byte) error {
	return tikv.store.BatchDelete(context.TODO(), keys)
}

func (tikv *TikvRaw) Scan(startKey []byte, endKey []byte, max_cnt int) ([][]byte, [][]byte, error)  {
	return tikv.store.Scan(context.TODO(), startKey, endKey, max_cnt)
}

func (tikv *TikvRaw) ReverseScan(startKey []byte, endKey []byte, max_cnt int) ([][]byte, [][]byte, error)  {
	return tikv.store.ReverseScan(context.TODO(), startKey, endKey, max_cnt)
}

func (tikv *TikvRaw) RangeDelete(startKey []byte, endKey []byte) error {
	return tikv.store.DeleteRange(context.TODO(), startKey, endKey)
}
