SHAREdis hopes to encapsulate and expand the key-value interface of TiKV on top of the advantages of TiKV, and use the Redis client protocol to implement Redis data types and interface operations, while shielding the complex cluster management inside the distributed system, Data synchronization operation, thereby providing a distributed SSD cache computing layer service that can replace/supplement Redis.

### Architecture diagram:

![architecture.png](https://user-images.githubusercontent.com/4768595/150509595-896bd7e2-da34-4663-9f0c-b5ceb5e1f436.png "architecture")

### Multiple data structure design:
#### KV type
TiKV key:

| length | 1Byte | segment_size | 4bit | 12bit | raw_key_size |
| ------- | ------- | ------- | ------- | ------- | ------- |
| content | segment_size | segment | type | raw_key_size | raw_key |

TiKV value:

| length | raw_value_len |
| ------- | ------- |
| content | raw_value |

#### SET type
TiKV key:

| length | 1Byte | segment_size | 4bit | 12bit | name_size | all_key_size - before |
| ------- | ------- | ------- | ------- | ------- | ------- | ------- |
| content | segment_size | segment | type | name_size | set_name | member |

TiKV value:

| length |
| ------- |
| content |

#### HASH type
TiKV key:

| length | 1Byte | segment_size | 4bit | 12bit | name_size | all_key_size - before |
| ------- | ------- | ------- | ------- | ------- | ------- | ------- |
| content | segment_size | segment | type | name_size | hash_name | field |

TiKV value:

| length | field_value_len |
| ------- | ------- |
| content | field_value |

#### ZSET type
##### key_score(get by score)
TiKV key:

| length | 1Byte | segment_size | 4bit | 12bit | name_size | 8Byte | all_key_size - before |
| ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- |
| content | segment_size | segment | type | name_size | zset_name | score | member |

TiKV value:

| length |
| ------- |
| content |

##### key_idx(get by key)
TiKV key:

| length | 1Byte | segment_size | 4bit | 12bit | name_size | all_key_size - before |
| ------- | ------- | ------- | ------- | ------- | ------- | ------- |
| content | segment_size | segment | type | name_size | zset_name | member |

TiKV value:

| length | 8Byte |
| ------- | ------- |
| content | score |

### Current list of supported commands:
**Currently, sharedis interacts with clients using the thrift-based rpc protocol, which will be replaced with the Redis protocol in the future**

| Redis Command Groups | Redis Commands |
| ------- | ------- |
| string | set |
| string | get |
| string | mset |
| string | mget |
| string | ttl |
| string | del |
| set | sadd |
| set | srem |
| set | smembers |
| set | scard |
| set | sismember |
| hash | hset |
| hash | hget |
| hash | hkeys |
| hash | hdel |
| hash | hmset |
| hash | hmget |
| zset | zadd |
| zset | zscore |
| zset | zrem |
| zset | zrange |
| zset | zrangebyscore |
| zset | zcard |
| zset | zscan |

