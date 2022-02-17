import socket
import sys
import time
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from sharestore_lib import SharestorePool
if __name__ == "__main__":
    #options = {'host': 'prod.shareit.cbs.sg2.sharestore', 'port': 9090}
    options = {'host': 'prod.sprs.cbs.sg2.sharestore', 'port': 9090}
    #options = {'host': '10.21.23.6', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=100)
    segment = 'sprs_all_counter_prod'
    #segment = 'tjk_test'
    test_k = '0903testkey06'
    # set value
    #ret, error = pool.IncrCounter(segment, 'tttttest', 2)
    ##ret, error = pool.SetValue("new_stress_test", "abcde", "asd09111", 1200)
    #if ret:
    #    print("set succ")
    #else:
    #    print("set error:", error)
    #    sys.exit(1)
    # set value
    #ret, error = pool.SetValue("test", "zxc", "ytr678", 1200)
    #if ret:
    #    print("set succ")
    #else:
    #    print("set error:", error)
    #    sys.exit(1)
    # get value
    value, error = pool.GetCounter(segment, 'shareit_all_show_v5hRL6')
    #value, error = pool.GetCounter(segment, 'tttttest')
    if not error:
        print("get succ:", value)
    else:
        print("get error:", error)
    # del value
    #ret, error = pool.DelValue("test", "qwe")
    #if ret:
    #    print("del succ")
    #else:
    #    print("del error:", error)
    # multiget value
    #responses, error = pool.MultiGetValue("test", ["qwe", "zxc"])
    #if not error:
    #    print("get succ:", responses)
    #else:
    #    print("get error:", error)
