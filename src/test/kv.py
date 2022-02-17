import socket
import sys
import time
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from sharestore_lib import SharestorePool
if __name__ == "__main__":
    #options = {'host': 'NLB-CBS-sharestore-server-test-9316364d543dd2d6.elb.ap-southeast-1.amazonaws.com', 'port': 9090}
    options = {'host': '10.20.0.130', 'port': 9090}
    #options = {'host': 'prod.ads-algo-ro.cbs.sg1.sharestore', 'port': 9090}
    #options = {'host': '10.21.23.6', 'port': 9090}
    #options = {'host': '172.26.92.56', 'port': 9090}
    #options = {'host': 'prod.main.cbs.sg2.sharestore', 'port': 9090}
    #options = {'host': 'prod.readonly-common.cbs.sg2.sharestore', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=100)
    segment = 'rwtabletest'
    test_k = '090302testkey09'
    #for num in range(1,15):
    #    test_k = test_k + test_k
    # set value
    #ret, error = pool.SetValue(segment, test_k, "asd09111", 1200)
    #ret, error = pool.SetValue("tjk_test", "abcde", "asd09111", 1200)
    #if ret:
    #    print("set succ")
    #else:
    #    print("set error:", error)
    #    sys.exit(1)
    # set value
    ret, error = pool.SetValue("tjk_test", test_k, "ytr567890", 60)
    if ret:
        print("set succ", len(test_k))
    else:
        print("set error:", error)
        sys.exit(1)
    # get value
    #value, error = pool.GetValue(segment, test_k)
    #value, error = pool.MultiGetValue("tjk_test", [test_k, "K6qE"])
    value, error = pool.GetValue("tjk_test", test_k)
    #value, error = pool.GetValue("ads_anti_wm_feature_prod", "other_1212680")
    if not error:
        print("get succ:", value)
    else:
        print("get error:", error)
    # del value
    #ret, error = pool.DelValue("hermes_user_token", "da01a537fbd54474a6cd4b557f15ba34")
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
