import socket
import sys
import time
import logging

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol

import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from sharestore import Sharestore
from sharestore import ttypes
from sharestore import constants

from socketpool.pool import ConnectionPool
from socketpool.conn import TcpConnector

class HashType(object):
    STR = 0
    INT = 1
    BUF = 2

    _VALUES_TO_NAMES = {
        0: "STR",
        1: "INT",
        2: "BUF",
    }

    _NAMES_TO_VALUES = {
        "STR": 0,
        "INT": 1,
        "BUF": 2,
    }

class SharestorePool:

    def __init__(self, factory=TcpConnector,
                 retry_max=3, retry_delay=.01,
                 max_lifetime=600., max_size=10,
                 reap_connections=True, reap_delay=1,
                 backend="thread", options=None,
                 timeout_ms=100):
            self.pool = ConnectionPool(
                 factory=factory, retry_max=retry_max, retry_delay=retry_delay,
                 max_lifetime=max_lifetime, max_size=max_size,
                 reap_connections=reap_connections, reap_delay=reap_delay,
                 backend=backend, options=options)
            self.timeout = timeout_ms

    def GetClient(self):
        with self.pool.connection() as conn:
            handle = conn.get_handle()
        
            transport = TSocket.TSocket()
            transport.setHandle(handle)
            transport.setTimeout(self.timeout)
    
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            return Sharestore.Client(protocol)

    def SetTimeout(self, timeout_ms=100):
        self.timeout = timeout_ms     

    def GetSharestoreClient(self, conn):
        handle = conn.get_handle()
    
        transport = TSocket.TSocket()
        transport.setHandle(handle)
        transport.setTimeout(self.timeout)
    
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        return Sharestore.Client(protocol)

    def SetValue(self, segment, key, value, ttl_sec=0, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                set_request = ttypes.SetRequest(segment, key, value, ttl_sec, need_routing)
                result = client.setValue(set_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
            conn.invalidate()
            return False, ttypes.SharestoreException("socket error")
        except Exception as e:
            return False, e

    def GetValue(self, segment, key, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                get_request = ttypes.GetRequest(segment, key, need_routing)
                result = client.getValue(get_request)
                return result.value, result.error
            conn.invalidate()
            return None, ttypes.SharestoreException("socket error")
        except Exception as e:
            return None, e
    
    def ttl(self, segment, key, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                ttl_request = ttypes.TtlRequest(segment, key, need_routing)
                result = client.ttl(ttl_request)
                return result.ttl_sec, result.error
            conn.invalidate()
            return None, ttypes.SharestoreException("socket error")
        except Exception as e:
            return None, e
    
    def DelValue(self, segment, key, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                del_request = ttypes.DelRequest(segment, key, need_routing)
                result = client.delValue(del_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
            conn.invalidate()
            return False, ttypes.SharestoreException("socket error")
        except Exception as e:
            return False, e

    def MultiGetValue(self, segment, keys, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                multiget_request = ttypes.MultiGetRequest(segment, keys, need_routing=need_routing)
                result = client.multiGetValue(multiget_request)
                return result.responses, result.sub_request_indices
            conn.invalidate()
            return None, ttypes.SharestoreException("socket error")
        except Exception as e:
            return None, e
    
    def IncrCounter(self, segment, key, value, need_routing=True):
        try:
            client = self.GetClient()
            
            incr_request = ttypes.IncrCounterRequest(segment, key, value, need_routing)
            result = client.incrCounter(incr_request)

            if result.error == None:
                ret = True
            else:
                ret = False
            return ret, result.error
        except Exception as e:
            return False, e
    
    def GetCounter(self, segment, key, need_routing=True):
        try:
            client = self.GetClient()
            
            get_counter_request = ttypes.GetCounterRequest(segment, key, need_routing)
            result = client.getCounter(get_counter_request)
            return result.value, result.error
        except Exception as e:
            return "", e
    
    def SetCounter(self, segment, key, value, need_routing=True):
        try:
            client = self.GetClient()
            
            incr_request = ttypes.SetCounterRequest(segment, key, value, need_routing)
            result = client.setCounter(incr_request)

            if result.error == None:
                ret = True
            else:
                ret = False
            return ret, result.error
        except Exception as e:
            return False, e
    
    def MultiGetCounter(self, segment, keys, need_routing=True):
        try:
            client = self.GetClient()
            
            mget_counter_request = ttypes.MultiGetCounterRequest(segment, keys, need_routing=need_routing)
            result = client.multiGetCounter(mget_counter_request)
            return result.responses, None
        except Exception as e:
            return "", e

    def DSSetValue(self, segment, key, members, ttl_secs, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                ds_set_request = ttypes.DSSetRequest(segment, key, members, ttl_secs, need_routing=need_routing)
                result = client.dsSetValue(ds_set_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            logging.exception(e)
            return False, e

    def DSRemValue(self, segment, key, members, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                ds_rem_request = ttypes.DSRemRequest(segment, key, members, need_routing)
                result = client.dsRemValue(ds_rem_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e

    def DSGetValue(self, segment, key, with_ttls=False, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                ds_get_request = ttypes.DSGetRequest(segment, key, with_ttls, need_routing, max_nums)
                result = client.dsGetValue(ds_get_request)
                if with_ttls:
                    return result.members, result.ttl_secs, None
                else:
                    return result.members, None, None
        except Exception as e:
            return None, None, e

    def DSCountValue(self, segment, key, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                ds_count_request = ttypes.DSCountRequest(segment, key, need_routing, max_nums)
                result = client.dsCountValue(ds_count_request)
                return result.count, None
        except Exception as e:
            return None, e

    def DSDelValue(self, segment, key, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                ds_del_request = ttypes.DSDelRequest(segment, key, need_routing)
                result = client.dsDelValue(ds_del_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e

    def DSIsMember(self, segment, key, member, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                ds_is_member_request = ttypes.DSIsMemberRequest(segment, key, member, need_routing)
                result = client.dsIsMember(ds_is_member_request)
                return result.yes, None
        except Exception as e:
            return False, e
    
    def HashSetValue(self, segment, key, members, values, ttl_secs=None, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)
                
                hash_values = []
                for value in values:
                    if isinstance(value, str):
                        hash_values.append(ttypes.HashValue(str_val=value))
                    elif isinstance(value, int):
                        hash_values.append(ttypes.HashValue(int_val=value))
                
                hash_set_request = ttypes.HashSetRequest(segment, key, members, hash_values, ttl_secs, need_routing)
                result = client.hashSetValue(hash_set_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e
    
    def MultiHashSetValue(self, segment, key, members, values, ttl_secs=None, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)
                
                hash_values = []
                for value in values:
                    hvalues = []
                    for vl in value:
                        if isinstance(vl, str):
                            hvalues.append(ttypes.HashValue(str_val=vl))
                        elif isinstance(vl, int):
                            hvalues.append(ttypes.HashValue(int_val=vl))
                    hash_values.append(hvalues)
                
                multi_hash_set_request = ttypes.MultiHashSetRequest(segment, key, members, hash_values, ttl_secs, need_routing=need_routing)
                result = client.multiHashSetValue(multi_hash_set_request)

                return True, result.responses
        except Exception as e:
            logging.exception(e)
            return False, e

    def HashRemValue(self, segment, key, members, hash_type=HashType.STR, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                if (hash_type == HashType.STR):
                    hash_ttype = ttypes.HashType.STR
                elif (hash_type == HashType.INT):
                    hash_ttype = ttypes.HashType.INT
                elif (hash_type == HashType.BUF):
                    hash_ttype = ttypes.HashType.BUF
                hash_rem_request = ttypes.HashRemRequest(segment, key, hash_ttype, members, need_routing)
                result = client.hashRemValue(hash_rem_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e

    def MultiHashRemValue(self, segment, keys, members=None, hash_types=None, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)
                multi_hash_rem_request = ttypes.MultiHashRemRequest(segment, keys, hash_types, members, need_routing=need_routing)
                result = client.multiHashRemValue(multi_hash_rem_request)

                return True, result.responses
        except Exception as e:
            return False, e

    def HashCountValue(self, segment, key, type=0, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                hash_count_request = ttypes.HashCountRequest(segment, key, type, need_routing, max_nums)
                result = client.hashCountValue(hash_count_request)
                return result.count, None
        except Exception as e:
            return None, e

    def HashGetValue(self, segment, key, members, hash_type=HashType.STR, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                hash_get_request = ttypes.HashGetRequest(segment, key, hash_type, members, need_routing, max_nums)
                result = client.hashGetValue(hash_get_request)
                if result.error == None:
                    return result.members, result.values, None
                else:
                    return None, None, result.error
        except Exception as e:
            return None, None, e

    def MultiHashGetValue(self, segment, keys, members=None, hash_types=None, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                multi_hash_get_request = ttypes.MultiHashGetRequest(segment, keys, hash_types, members, max_nums, need_routing=need_routing)
                result = client.multiHashGetValue(multi_hash_get_request)
                return True, result.responses
        except Exception as e:
            return False, e

    def HashIncrValue(self, segment, key, members, values, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)
                
                hash_incr_request = ttypes.HashIncrRequest(segment, key, members, values, need_routing)
                result = client.hashIncrValue(hash_incr_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e
    
    def HashExGetValue(self, segment, key, members, hash_type=HashType.STR, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                hashex_get_request = ttypes.HashExGetRequest(segment, key, hash_type, members, need_routing, max_nums)
                result = client.hashExGetValue(hashex_get_request)
                if result.error == None:
                    return result.members, result.values, result.version, None
                else:
                    return None, None, result.error
        except Exception as e:
            return None, None, e

    def HashExRemValue(self, segment, key, version, members, hash_type=HashType.STR, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                if (hash_type == HashType.STR):
                    hash_ttype = ttypes.HashType.STR
                elif (hash_type == HashType.INT):
                    hash_ttype = ttypes.HashType.INT
                elif (hash_type == HashType.BUF):
                    hash_ttype = ttypes.HashType.BUF
                hashex_rem_request = ttypes.HashExRemRequest(segment, key, hash_ttype, version, members, need_routing)
                result = client.hashExRemValue(hashex_rem_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e

    def ZSetAddValue(self, segment, key, members, scores, ttl_secs = None, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                zset_add_request = ttypes.ZSetAddRequest(segment, key, members, scores, ttl_secs, need_routing)
                result = client.zsetAddValue(zset_add_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e
   
    def ZSetRemValue(self, segment, key, members, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                zset_rem_request = ttypes.ZSetRemRequest(segment, key, members, need_routing)
                result = client.zsetRemValue(zset_rem_request)

                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e

    def ZSetCountValue(self, segment, key, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                zset_count_request = ttypes.ZSetCountRequest(segment, key, need_routing, max_nums)
                result = client.zsetCountValue(zset_count_request)
                return result.count, None
        except Exception as e:
            return None, e

    def ZSetGetValue(self, segment, key, with_scores=True, members=[], need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                zset_get_request = ttypes.ZSetGetRequest(segment, key, with_scores, members, need_routing, max_nums)
                result = client.zsetGetValue(zset_get_request)
                if result.error == None:
                    return result.members, result.scores, None
                else:
                    return None, None, result.error
        except Exception as e:
            return None, None, e

    def ZSetGetRangeValue(self, segment, key, index_range=None, score_range=None, with_scores=True, need_routing=True, max_nums=constants.DEFAULT_MAX_NUMS):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)
                
                i_range = None
                s_range = None
                if index_range != None:
                  i_range = ttypes.IndexRange(start = index_range["start"], stop = index_range["stop"])
                if score_range != None:
                  s_range = ttypes.ScoreRange(min = score_range["min"], max = score_range["max"])
                zset_get_range_request = ttypes.ZSetGetRangeRequest(segment, key, i_range, s_range, with_scores, need_routing, max_nums)
                result = client.zsetGetRangeValue(zset_get_range_request)
                if result.error == None:
                    return result.members, result.scores, None
                else:
                    return None, None, result.error
        except Exception as e:
            return None, None, e
    
    def ZSetRemRangeValue(self, segment, key, index_range=None, score_range=None, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                i_range = None
                s_range = None
                if index_range != None:
                  i_range = ttypes.IndexRange(start = index_range["start"], stop = index_range["stop"])
                if score_range != None:
                  s_range = ttypes.ScoreRange(min = score_range["min"], max = score_range["max"])
                zset_rem_range_request = ttypes.ZSetRemRangeRequest(segment, key, i_range, s_range, need_routing)
                result = client.zsetRemRangeValue(zset_rem_range_request)
                if result.error == None:
                    ret = True
                else:
                    ret = False
                return ret, result.error
        except Exception as e:
            return False, e
    
    def ZSetScanValue(self, segment, key, cursor, with_scores=True, is_reverse=False, max_nums=constants.DEFAULT_MAX_NUMS, need_routing=True):
        try:
            with self.pool.connection() as conn:
                client = self.GetSharestoreClient(conn)

                zset_scan_request = ttypes.ZSetScanRequest(segment, key, cursor, with_scores, is_reverse, need_routing, max_nums)
                result = client.zsetScanValue(zset_scan_request)
                if result.error == None:
                    return result.members, result.scores, result.cursor, None
                else:
                    return None, None, None, result.error
        except Exception as e:
            return None, None, None, e
