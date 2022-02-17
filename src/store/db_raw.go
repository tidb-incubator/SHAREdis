package store

type DBRaw interface {
	Close() error
	Get(key []byte) ([]byte, error)
	MGet(keys [][]byte) ([][]byte, error)
	GetKeyTTL(key []byte) (int32, error)
	Set(key []byte, value []byte, ttl int32) error
	MSet(keys [][]byte, values [][]byte, ttls []uint64) error
	Delete(key []byte) error
	MDelete(keys [][]byte) error
	Scan(startKey []byte, endKey []byte, max_cnt int) ([][]byte, [][]byte, error)
	ReverseScan(startKey []byte, endKey []byte, max_cnt int) ([][]byte, [][]byte, error)
	RangeDelete(startKey []byte, endKey []byte) error
}
