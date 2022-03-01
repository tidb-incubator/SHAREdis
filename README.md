SHAREdis hopes to encapsulate and expand the key-value interface of TiKV on top of the advantages of TiKV, and use the Redis client protocol to implement Redis data types and interface operations, while shielding the complex cluster management inside the distributed system, Data synchronization operation, thereby providing a distributed SSD cache computing layer service that can replace/supplement Redis.

Current list of supported commands:

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
