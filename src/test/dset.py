import socket
import sys
import time
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from sharestore_lib import SharestorePool
if __name__ == "__main__":
    #options = {'host': 'NLB-CBS-sharestore-server-test-9316364d543dd2d6.elb.ap-southeast-1.amazonaws.com', 'port': 9090}
    options = {'host': '10.20.0.130', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=100)
    segment = 'rwtabletest'
    test_k = '090d302testkey09'
    # dsSet value
    ret, error = pool.DSSetValue("tjk_test", test_k, ["ytr590", "ytr591", "ytr592", "ytr593"], [30, 20, 40, 0])
    if ret:
        print("dsset succ", len(test_k))
    else:
        print("dsset error:", error)
        sys.exit(1)
    # dsGet value
    values, ttls, error = pool.DSGetValue("tjk_test", test_k, True)
    if not error:
        print("dsget succ:", values, ttls)
    else:
        print("dsget error:", error)
        sys.exit(1)
    # dsCount 
    cnt, error = pool.DSCountValue("tjk_test", test_k)
    if not error:
        print("count succ:", cnt)
    else:
        print("count error:", error)
        sys.exit(1)
    # dsIsMember 
    yes, error = pool.DSIsMember("tjk_test", test_k, "ytr591")
    if not error:
        print("is_member succ:", yes)
    else:
        print("is_member error:", error)
        sys.exit(1)
    yes, error = pool.DSIsMember("tjk_test", test_k, "ytr691")
    if not error:
        print("is_member succ:", yes)
    else:
        print("is_member error:", error)
        sys.exit(1)
    # dsRem value
    ret, error = pool.DSRemValue("tjk_test", test_k, ["ytr591", "ytr594"])
    if ret:
        print("rem succ")
    else:
        print("rem error:", error)
        sys.exit(1)
    values, ttls, error = pool.DSGetValue("tjk_test", test_k, True)
    # dsGet value
    if not error:
        print("dsget succ:", values, ttls)
    else:
        print("dsget error:", error)
        sys.exit(1)
    # dsDel value
    ret, error = pool.DSDelValue("tjk_test", test_k)
    if ret:
        print("del succ")
    else:
        print("del error:", error)
        sys.exit(1)
    # dsGet value
    values, ttls, error = pool.DSGetValue("tjk_test", test_k, True)
    if not error:
        print("dsget succ:", values, ttls)
    else:
        print("dsget error:", error)
        sys.exit(1)
