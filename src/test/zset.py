import socket
import sys
import time
import os
import random
import string
import threading

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from sharestore_lib.SharestorePool import SharestorePool

def TestZSet(pool):
    # zsetadd 
    ret, error = pool.ZSetAddValue("tjk_test", "aaaa", ['mb1_v5_1615458709', 'mpp1_v3_1615458709', 'mfp2_v4_1615458709', 'lr1_1615458709', 'mnb1_1615458709', 'csnb1_1615458709', 'tdsp_1615458709', 'tpp1_v5_1615458709', 'tpb1_v2_1615458709', 'tdrp_1615458709', 'default_1615458709'], [25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25])
    if ret:
        print("zsetadd succ")
    else:
        print("zsetadd error:", error)
        sys.exit(1)

def TestZSetGtimes(pool):
    # zsetget all
    members, scores, error = pool.ZSetGetValue("scas_likeit_tag_items_ranking_prod", "h_fkcX")
    if not error:
        print("zsetget succ", members, scores)
    else:
        print("zsetget error:", error)
        sys.exit(1)

def TestZSetMtimes():
    #options = {'host': '172.26.94.88', 'port': 9090}
    #options = {'host': '10.20.3.63', 'port': 9090}
    options = {'host': 'prod.readonly-common.cbs.sg2.sharestore', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=100)
    for i in range(1):
        #TestZSet(pool)
        TestZSetGtimes(pool)

if __name__ == "__main__":
    options = {'host': '10.20.0.130', 'port': 9090}
    #options = {'host': 'prod.ads-algo-ro.cbs.sg1.sharestore', 'port': 9090}
    #options = {'host': '10.21.23.6', 'port': 9090}
    #options = {'host': 'prod.main.cbs.sg2.sharestore', 'port': 9090}
    #options = {'host': 'prod.readonly-common.cbs.sg2.sharestore', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=100)
    segment = 'tjk_test'
    test_k = 'zset02testkey09'    
    # zsetadd 
    ret, error = pool.ZSetAddValue(segment, test_k, ['mb1_v5_161', 'mpp1_v3_161', 'mfp2_v4_169'], [25, 15, 40], [60, 60, 60])
    if ret:
        print("zsetadd succ")
    else:
        print("zsetadd error:", error)
        sys.exit(1)
    # zsetadd 
    ret, error = pool.ZSetAddValue(segment, test_k, ['mfp2_v4_169', 'lr1_161', 'mnb1_161', 'csnb1_161'], [35, 45, 25, 55], [60, 60, 60, 40])
    if ret:
        print("zsetadd succ")
    else:
        print("zsetadd error:", error)
        sys.exit(1)
    # zsetadd 
    ret, error = pool.ZSetAddValue(segment, test_k, ['csnb1_161', 'tdsp_161', 'tpp1_v5_161', 'tpb1_v2_161', 'tdrp_161', 'default_161'], [55, 75, 65, 95, 75, 85], [40, 50, 50, 50, 70, 0])
    if ret:
        print("zsetadd succ")
    else:
        print("zsetadd error:", error)
        sys.exit(1)
    # zsetget all
    members, scores, error = pool.ZSetGetValue(segment, test_k)
    if not error:
        print("zsetget succ", members, scores)
    else:
        print("zsetget error:", error)
        sys.exit(1)
    # zsetrem
    ret, error = pool.ZSetRemValue(segment, test_k, ['mb1_v5_161', 'csnb1_161', 'mnb1_161'])
    if ret:
        print("zsetrem succ")
    else:
        print("zsetrem error:", error)
        sys.exit(1)
    # zsetget all
    members, scores, error = pool.ZSetGetValue(segment, test_k)
    if not error:
        print("zsetget succ", members, scores)
    else:
        print("zsetget error:", error)
        sys.exit(1)
    # zsetcount
    count, error = pool.ZSetCountValue(segment, test_k)
    if not error:
        print("zsetcount succ", count)
    else:
        print("zsetcount error:", error)
        sys.exit(1)
    # zsetgetrange
    members, scores, error = pool.ZSetGetRangeValue(segment, test_k, None, {"min":55, "max":85})
    if not error:
        print("zsetgetrange succ", members, scores)
    else:
        print("zsetgetrange error:", error)
        sys.exit(1)
    # zsetremrange
    ret, error = pool.ZSetRemRangeValue(segment, test_k, None, {"min":35, "max":75})
    if ret:
        print("zsetremrange succ")
    else:
        print("zsetremrange error:", error)
        sys.exit(1)
    # zsetgetrange
    members, scores, error = pool.ZSetGetRangeValue(segment, test_k, None, {"min":0, "max":100})
    if not error:
        print("zsetgetrange succ", members, scores)
    else:
        print("zsetgetrange error:", error)
        sys.exit(1)
    # zsetscan
    cursor = ""
    while True:
        members, scores, cursor, error = pool.ZSetScanValue(segment, test_k, cursor, True, False, 1)
        if not error:
            print("zsetscan succ", members, scores)
        else:
            print("zsetscan error:", error)
            sys.exit(1)
        if None == cursor or 0 == len(cursor):
            print("zsetscan end")
            break
    # zsetscan
    cursor = ""
    while True:
        members, scores, cursor, error = pool.ZSetScanValue(segment, test_k, cursor, True, True, 1)
        if not error:
            print("zsetscan succ", members, scores)
        else:
            print("zsetscan error:", error)
            sys.exit(1)
        if None == cursor or 0 == len(cursor):
            print("zsetscan end")
            break