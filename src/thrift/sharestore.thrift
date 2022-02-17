# This is the interface for a real time sharestore service

namespace go sharestore

enum ErrorCode {
  OTHER = 0,
  DB_NOT_FOUND = 1,
  ROCKSDB_ERROR = 2,
  SEGMENT_NOT_FOUND = 3,
  KEY_NOT_FOUND = 4,
  ROUTING_ERROR = 5,
  WRITE_TO_SLAVE = 6,
  NUM_OVERFLOW = 7,
  REQUEST_INVALID = 8,
  THROTTLED = 9,
  VERSION_EXPIRE = 10,
}

exception SharestoreException {
  1: required string message,
  2: required ErrorCode code,
}

enum StoreType {
  KV = 0,
  COUNTER = 1,
  DEDUP_SET = 2,
  SET = 3,
  HASH = 4,
  ZSET = 5,
  ZSET_IDX = 6,
  HASHEX = 7,
  OTHER = 8,
}

const i32 DEFAULT_MAX_NUMS = 5000

typedef binary (cpp.type = "std::unique_ptr<folly::IOBuf>") IOBufPtr

# Get
struct GetRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool need_routing = 1,
}

struct GetResponse {
  1: required IOBufPtr value,
  2: optional SharestoreException error,
}

# MultiGet
struct MultiGetRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
# the follow fields are only used internally
  3: optional list<i32> index,
  4: optional bool need_routing = 1,
}

struct MultiGetResponse {
  1: required list<GetResponse> responses,
  2: optional list<i32> sub_request_indices,
}

#Ttl 
struct TtlRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool need_routing = 1,
}

struct TtlResponse {
  1: required i32 ttl_sec,
  2: optional SharestoreException error,
}

# Set
struct SetRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required IOBufPtr value,
  4: optional i32 ttl_sec = 0,
  5: optional bool need_routing = 1,
}

struct SetResponse {
  1: optional SharestoreException error,
}

#MultiSet
struct MultiSetRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: required list<IOBufPtr> values,
  4: optional list<i32> ttl_secs,
  # the follow fields are only used internally
  5: optional list<i32> index,
  6: optional bool need_routing = 1,
}

struct MultiSetResponse {
  1: required list<SetResponse> responses,
  2: optional list<i32> sub_request_indices,
}

# Del
struct DelRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool need_routing = 1,
}

struct DelResponse {
  1: optional SharestoreException error,
}

# DedupSet

# DSSet
struct DSSetRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required list<string> members,
  4: required list<i32> ttl_secs,
  5: optional bool need_routing = 1,
}

struct DSSetResponse {
  1: optional SharestoreException error,
}

# DSRem
struct DSRemRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required list<string> members,
  4: optional bool need_routing = 1,
}

struct DSRemResponse {
  1: optional SharestoreException error,
}

# DSGet
struct DSGetRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool with_ttls = 0,
  4: optional bool need_routing = 1,
  5: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct DSGetResponse {
  1: required list<string> members,
  2: optional list<i32> ttl_secs,
  3: optional SharestoreException error,
}

# DSCount
struct DSCountRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool need_routing = 1,
  4: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct DSCountResponse {
  1: required i32 count = 0,
  2: optional SharestoreException error,
}

# DSDel
struct DSDelRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool need_routing = 1,
}

struct DSDelResponse {
  1: optional SharestoreException error,
}

# DSIsMember
struct DSIsMemberRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required string member,
  4: optional bool need_routing = 1,
}

struct DSIsMemberResponse {
  1: required bool yes,
  2: optional SharestoreException error,
}

# IncrCounter
struct IncrCounterRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required i64 value = 1,
  4: optional bool need_routing = 1,
}

struct IncrCounterTtlRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required i64 value = 1,
  4: required i32 ttl_sec,
  5: optional bool need_routing = 1,
}

struct IncrCounterResponse {
  1: optional SharestoreException error,
}

# MultiIncrCounter
struct MultiIncrCounterRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: required list<i64> values,
  # the follow fields are only used internally
  4: optional list<i32> index,
  5: optional bool need_routing = 1,
}

struct MultiIncrCounterTtlRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: required list<i64> values,
  4: required list<i32> ttl_secs,
  # the follow fields are only used internally
  5: optional list<i32> index,
  6: optional bool need_routing = 1,
}

struct MultiIncrCounterResponse {
  1: required list<IncrCounterResponse> responses,
  2: optional list<i32> sub_request_indices,
}

# GetCounter
struct GetCounterRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool need_routing = 1,
}

struct GetCounterResponse {
  1: required i64 value,
  2: optional SharestoreException error,
}

# SetCounter
struct SetCounterRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required i64 value,
  4: optional bool need_routing = 1,
}

struct SetCounterTtlRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required i64 value,
  4: required i32 ttl_sec,
  5: optional bool need_routing = 1,
}

struct SetCounterResponse {
  1: optional SharestoreException error,
}

# MultiSetCounter
struct MultiSetCounterRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: required list<i64> values,
  # the follow fields are only used internally
  4: optional list<i32> index,
  5: optional bool need_routing = 1,
}
struct MultiSetCounterTtlRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: required list<i64> values,
  4: required list<i32> ttl_secs,
  # the follow fields are only used internally
  5: optional list<i32> index,
  6: optional bool need_routing = 1,
}

struct MultiSetCounterResponse {
  1: required list<SetCounterResponse> responses,
  2: optional list<i32> sub_request_indices,
}

# MultiGetCounter
struct MultiGetCounterRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
# the follow fields are only used internally
  3: optional list<i32> index,
  4: optional bool need_routing = 1,
}

struct MultiGetCounterResponse {
  1: required list<GetCounterResponse> responses,
  2: optional list<i32> sub_request_indices,
}

# Hash

union HashValue {
  1: string str_val,
  2: i64    int_val,
  3: binary    buf_val,
}

enum HashType {
  STR = 0,
  INT = 1,
  BUF = 2,
}

# HashSet
# key 为 主键
# members 为 子键


struct HashSetRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required list<string> members,
  4: required list<HashValue> values,
  5: optional list<i32> ttl_secs,
  6: optional bool need_routing = 1,
}

struct HashSetResponse {
  1: optional SharestoreException error,
}

struct MultiHashSetRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: required list<list<string>> members,
  4: required list<list<HashValue>> values,
  5: optional list<list<i32>> ttl_secs,
  # the follow fields are only used internally
  6: optional list<i32> index,
  7: optional bool need_routing = 1,
}

struct MultiHashSetResponse {
  1: required list<HashSetResponse> responses,
  2: optional list<i32> sub_request_indices,
}

# HashRem

struct HashRemRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required HashType type = 0, 
  4: optional list<string> members,
  5: optional bool need_routing = 1,
}

struct HashRemResponse {
  1: optional SharestoreException error,
}

struct MultiHashRemRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: optional list<HashType> types = 0, 
  4: optional list<list<string>> members,
  # the follow fields are only used internally
  5: optional list<i32> index,
  6: optional bool need_routing = 1,
}

struct MultiHashRemResponse {
  1: required list<HashRemResponse> responses,
  2: optional list<i32> sub_request_indices,
}

# HashGet

struct HashGetRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required HashType type = 0, 
  4: optional list<string> members,
  5: optional bool need_routing = 1,
  6: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct HashGetResponse {
  1: required list<string> members,
  2: required list<HashValue> values,
  3: optional SharestoreException error,
}

struct MultiHashGetRequest {
  1: optional string segment = "default",
  2: required list<string> keys,
  3: required list<HashType> types = 0, 
  4: optional list<list<string>> members,
  5: optional i32  max_nums = DEFAULT_MAX_NUMS,
  # the follow fields are only used internally
  6: optional list<i32> index,
  7: optional bool need_routing = 1,
}

struct MultiHashGetResponse {
  1: required list<HashGetResponse> responses,
  2: optional list<i32> sub_request_indices,
}

# HashCount
struct HashCountRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required HashType type = 0, 
  4: optional bool need_routing = 1,
  5: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct HashCountResponse {
  1: required i32 count = 0,
  2: optional SharestoreException error,
}

# ZSet

# ZSetAdd
# 采用批量加入的形式
# key 为 主键
# members 为 子健
# 单次操作不超过50个member

struct ZSetAddRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required list<string> members,
  4: required list<i64> scores,
  5: optional list<i32> ttl_secs,
  6: optional bool need_routing = 1,
}

struct ZSetAddResponse {
  1: optional SharestoreException error,
}

# ZSetRem
# 单次操作不超过50个member

struct ZSetRemRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional list<string> members,
  4: optional bool need_routing = 1,
}

struct ZSetRemResponse {
  1: optional SharestoreException error,
}

# ZSetGet
# key 为 主键
# members 为 子健，不设置，返回前max_nums个member

struct ZSetGetRequest {
  1: optional string segment = "default",
  2: required string key,
  3: required bool with_scores = 1,
  4: optional list<string> members,
  5: optional bool need_routing = 1,
  6: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct ZSetGetResponse {
  1: required list<string> members,
  2: optional list<i64> scores,
  3: optional SharestoreException error,
}

# ZSetGetRange
# 根据 index 获取，设置 index_range
# 根据 score 获取，设置 score_range
# index_range 和 score_range 不可同时设置

struct IndexRange {
  1: required i32 start = 0,
  2: required i32 stop = -1,
}

struct ScoreRange {
  1: required i64 min = 0,
  2: required i64 max = 9223372036854775807,
}

struct ZSetGetRangeRequest {
  1: optional string segment = "default",
  3: required string key,
  # 4 and 5 counldn't coexist.
  4: optional IndexRange index_range,
  5: optional ScoreRange score_range,
  6: optional bool with_scores = 1,
  7: optional bool need_routing = 1,
  8: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct ZSetGetRangeResponse {
  1: required list<string> members,
  2: optional list<i64> scores,
  3: optional SharestoreException error,
}

# ZSetRemRange
struct ZSetRemRangeRequest {
  1: optional string segment = "default",
  2: required string key,
  # 3 and 4 counldn't coexist.
  3: optional IndexRange index_range,
  4: optional ScoreRange score_range,
  5: optional bool need_routing = 1,
}

struct ZSetRemRangeResponse {
  1: optional SharestoreException error,
}

# ZSetCount
struct ZSetCountRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional bool need_routing = 1,
  4: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct ZSetCountResponse {
  1: required i32 count = 0,
  2: optional SharestoreException error,
}

# ZSetScan
struct ZSetScanRequest {
  1: optional string segment = "default",
  2: required string key,
  3: optional IOBufPtr cursor,
  4: optional bool with_scores = 1,
  5: optional bool is_reverse = 0,
  6: optional bool need_routing = 1,
  7: optional i32  max_nums = DEFAULT_MAX_NUMS,
}

struct ZSetScanResponse {
  1: required list<string> members,
  2: optional list<i64> scores,
  3: optional IOBufPtr cursor,
  4: optional SharestoreException error,
}

service Sharestore {
  GetResponse getValue(1: GetRequest request)
      throws (1: SharestoreException e)

  TtlResponse ttl(1: TtlRequest request)
      throws (1: SharestoreException e)

  SetResponse setValue(1: SetRequest request)
      throws (1: SharestoreException e)
  
  DelResponse delValue(1: DelRequest request)
      throws (1: SharestoreException e)

  MultiGetResponse multiGetValue(1: MultiGetRequest mrequest)
      throws (1: SharestoreException e)

  MultiSetResponse multiSetValue(1: MultiSetRequest msequest)
      throws (1: SharestoreException e)

  IncrCounterResponse incrCounter(1: IncrCounterRequest request) 
      throws (1: SharestoreException e)

  IncrCounterResponse incrCounterTtl(1: IncrCounterTtlRequest request) 
      throws (1: SharestoreException e)

  MultiIncrCounterResponse multiIncrCounter(1: MultiIncrCounterRequest request)
        throws (1: SharestoreException e)

  MultiIncrCounterResponse multiIncrCounterTtl(1: MultiIncrCounterTtlRequest request)
        throws (1: SharestoreException e)

  SetCounterResponse setCounter(1: SetCounterRequest request) 
      throws (1: SharestoreException e)

  SetCounterResponse setCounterTtl(1: SetCounterTtlRequest request) 
      throws (1: SharestoreException e)

  MultiSetCounterResponse multiSetCounter(1: MultiSetCounterRequest request)
        throws (1: SharestoreException e)

  MultiSetCounterResponse multiSetCounterTtl(1: MultiSetCounterTtlRequest request)
        throws (1: SharestoreException e)

  GetCounterResponse getCounter(1: GetCounterRequest request) 
      throws (1: SharestoreException e)
  
  MultiGetCounterResponse multiGetCounter(1: MultiGetCounterRequest mrequest)
      throws (1: SharestoreException e)
  
  # DedupSet
  DSSetResponse dsSetValue(1: DSSetRequest request)
      throws (1: SharestoreException e)

  DSRemResponse dsRemValue(1: DSRemRequest request)
      throws (1: SharestoreException e)

  DSGetResponse dsGetValue(1: DSGetRequest request)
      throws (1: SharestoreException e)

  DSCountResponse dsCountValue(1: DSCountRequest request)
      throws (1: SharestoreException e)

  DSDelResponse dsDelValue(1: DSDelRequest request)
      throws (1: SharestoreException e)

  DSIsMemberResponse dsIsMember(1: DSIsMemberRequest request)
      throws (1: SharestoreException e)

  # Hash
  HashSetResponse hashSetValue(1: HashSetRequest request)
      throws (1: SharestoreException e)

  MultiHashSetResponse multiHashSetValue(1: MultiHashSetRequest request)
      throws (1: SharestoreException e)

  HashRemResponse hashRemValue(1: HashRemRequest request)
      throws (1: SharestoreException e)

  MultiHashRemResponse multiHashRemValue(1: MultiHashRemRequest request)
      throws (1: SharestoreException e)

  HashGetResponse hashGetValue(1: HashGetRequest request)
      throws (1: SharestoreException e)
  
  MultiHashGetResponse multiHashGetValue(1: MultiHashGetRequest request)
      throws (1: SharestoreException e)
  
  HashCountResponse hashCountValue(1: HashCountRequest request)
      throws (1: SharestoreException e)

  # ZSet
  ZSetAddResponse zsetAddValue(1: ZSetAddRequest request)
      throws (1: SharestoreException e)

  ZSetRemResponse zsetRemValue(1: ZSetRemRequest request)
      throws (1: SharestoreException e)

  ZSetGetResponse zsetGetValue(1: ZSetGetRequest request)
      throws (1: SharestoreException e)

  ZSetGetRangeResponse zsetGetRangeValue(1: ZSetGetRangeRequest request)
      throws (1: SharestoreException e)

  ZSetRemRangeResponse zsetRemRangeValue(1: ZSetRemRangeRequest request)
      throws (1: SharestoreException e)
  
  ZSetCountResponse zsetCountValue(1: ZSetCountRequest request)
      throws (1: SharestoreException e)

  ZSetScanResponse zsetScanValue(1: ZSetScanRequest request)
      throws (1: SharestoreException e)
}
