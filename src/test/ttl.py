import socket
import sys
import time

import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from sharestore_lib import SharestorePool

if __name__ == "__main__":

    options = {'host': 'prod.uo-trans.cbs.sg2.sharestore', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=100)

    # set value
    #ret, error = pool.SetValue("test", "qwe", b"asd09111", 1200)
    #if ret:
    #    print("set succ")
    #else:
    #    print("set error:", error)
    #    sys.exit(1)

    # set value
    #ret, error = pool.SetValue("test", "zxc", b"ytr678", 1200)
    #if ret:
    #    print("set succ")
    #else:
    #    print("set error:", error)
    #    sys.exit(1)

    # get value
    #value, error = pool.GetValue("test", "qwe")
    #if not error:
    #    print("get succ:", value)
    #else:
    #    print("get error:", error)
    
    # ttl value
    value, error = pool.ttl("uo_trans_weekly_prod", "9d77e1303e9445fda934c1dae8b2ce69")
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
    
