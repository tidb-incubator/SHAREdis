import socket
import sys
import time
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from sharestore_lib.SharestorePool import SharestorePool
from sharestore_lib.SharestorePool import HashType
if __name__ == "__main__":
    #options = {'host': '172.26.83.192', 'port': 9090}
    #options = {'host': '172.26.85.206', 'port': 9090}
    #options = {'host': 'prod.ads-algo-ro.cbs.sg1.sharestore', 'port': 9090}
    options = {'host': '10.20.0.130', 'port': 9090}
    #options = {'host': 'prod.ads-od.cbs.sg1.sharestore', 'port': 9090}
    #options = {'host': 'prod.extract-ads.cbs.sg1.sharestore', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=1000)
    segment = "tjk_test"
    # hashset
    #ret, error = pool.HashSetValue(segment, "test11233", ["member001", "member002", "testmem003", "testmem004"], ["value001", "value002", "value003", "value004"], [0, 30, 50, 40])
    ret, error = pool.HashSetValue(segment, "test11233", ["member001", "member002", "testmem003", "testmem004"], ["value001", "value002", "value003", "value004"])
    if ret:
        print("hashset succ")
    else:
        print("hashset error:", error)
        sys.exit(1)

    #time.sleep(2)
    # hashget
    #members, values, error = pool.HashGetValue("ads_rcmd_user_profile_pb_prod", "OG_user_base_e7705548beeb43208ce304b4f78efcbe", [], HashType.BUF)
    members, values, error = pool.HashGetValue(segment, "test11233", ["member002", "testmem003"], HashType.STR, True)
    if not error:
        #print("hashget succ", members, values, len(values[0].buf_val))
        print("hashget succ", members, values)
    else:
        print("hashget error:", error)
        sys.exit(1)

    # hashrem
    ret, error = pool.HashRemValue(segment, "test11233", ["testmem003", "testmem004"], HashType.STR)
    if ret:
        print("hashrem succ")
    else:
        print("hashrem error:", error)
        sys.exit(1)

    # hashget all
    members, values, error = pool.HashGetValue(segment, "test11233", [], HashType.STR, True)
    if not error:
        #print("hashget succ", members, values, len(values[0].buf_val))
        print("hashget all succ", members, values)
    else:
        print("hashget all error:", error)
        sys.exit(1)

    # multihashset
    ret, error = pool.MultiHashSetValue(segment, ["test21233", "test31233"], [["member101", "member102", "testmem103", "testmem104"], ["member201", "member202", "testmem203"]], [["value101", "value102", "value103", "value104"], ["value201", "value202", "valu203"]], [[0, 30, 50, 40], [30, 50, 0]])
    if ret:
        print("multihashset succ")
    else:
        print("multihashset error:", error)
        sys.exit(1)

    #time.sleep(2)
    # hashget
    #members, values, error = pool.HashGetValue("ads_rcmd_user_profile_pb_prod", "OG_user_base_e7705548beeb43208ce304b4f78efcbe", [], HashType.BUF)
    ret, response = pool.MultiHashGetValue(segment, ["test21233", "test31233"], [["member101", "testmem104"], []], [HashType.STR, HashType.STR], True)
    if ret:
        #print("hashget succ", members, values, len(values[0].buf_val))
        print("multihashget succ", response)
    else:
        print("multihashget error:", response)
        sys.exit(1)

    # hashrem
    ret, error = pool.MultiHashRemValue(segment, ["test21233", "test31233"], [["testmem103", "testmem104"], ["member201"]], [HashType.STR, HashType.STR])
    if ret:
        print("multihashrem succ")
    else:
        print("multihashrem error:", error)
        sys.exit(1)

    # hashget all
    ret, response = pool.MultiHashGetValue(segment, ["test21233", "test31233"], [[], []], [HashType.STR, HashType.STR], True)
    if ret:
        #print("hashget succ", members, values, len(values[0].buf_val))
        print("multihashget all succ", response)
    else:
        print("multihashget all error:", response)
        sys.exit(1)

    #ret, error = pool.HashSetValue("test", "hashtest1", ["member001", "member005"], ["value001", "value005"], [100, 1000])
    #if ret:
    #    print("hashset succ")
    #else:
    #    print("hashset error:", error)
    #    sys.exit(1)
    # hashget partial
    #members, values, error = pool.HashGetValue("test", "hashtest1", ["testmem004", "testmem04"])
    #if not error:
    #    print("hashget succ", members, values)
    #else:
    #    print("hashget error:", error)
    #    sys.exit(1)
    # hashrem
    #ret, error = pool.HashRemValue("test", "hashtest1", ["member002", "member02"])
    #if ret:
    #    print("hashrem succ")
    #else:
    #    print("hashrem error:", error)
    # hashget all
    #members, values, error = pool.HashGetValue("test", "hashtest1", [])
    #if not error:
    #    print("hashget succ", members, values)
    #else:
    #    print("hashget error:", error)
    #    sys.exit(1)
    # hashset count
    #ret, error = pool.HashSetValue("test", "hashtest1", ["member001", "member002", "testmem003", "testmem004"], [1, 10002, 10003, 10004])
    #if ret:
    #    print("\nhashset counter succ")
    #else:
    #    print("\nhashset counter error:", error)
    #    sys.exit(1)
    # hashcount value
    #count, error = pool.HashCountValue("test", "hashtest1", HashType.INT)
    #if not error:
    #    print("\nhashcount get succ", count)
    #else:
    #    print("\nhashcount get error:", error)
    #    sys.exit(1)
    # hashget all
    #members, values, error = pool.HashGetValue("test", "hashtest1", [], HashType.INT)
    #if not error:
    #    print("hashget counter succ", members, values)
    #else:
    #    print("hashget counter error:", error)
    #    sys.exit(1)
    #ret, error = pool.HashIncrValue("test", "hashtest1", ["member001", "member002", "testmem003", "testmem004"], [1, 1, 1, 1])
    #if ret:
    #    print("hashset counter succ")
    #else:
    #    print("hashset counter error:", error)
    #    sys.exit(1)
    # hashget partial
    #members, values, error = pool.HashGetValue("test", "hashtest1", ["testmem004", "testmem04"], HashType.INT)
    #if not error:
    #    print("hashget counter succ", members, values)
    #else:
    #    print("hashget counter error:", error)
    #    sys.exit(1)
    # hashget partial
    #members, values, error = pool.HashGetValue("test", "hashtest1", [], HashType.INT)
    #if not error:
    #    print("hashget counter succ", members, values)
    #else:
    #    print("hashget counter error:", error)
    #    sys.exit(1)
    # hashrem
    #ret, error = pool.HashRemValue("test", "hashtest1", ["member001", "member002", "testmem003", "testmem004"], HashType.INT)
    #if ret:
    #    print("hashrem counter succ")
    #else:
    #    print("hashrem counter error:", error)
    # hashget all
    #members, values, error = pool.HashGetValue("test", "hashtest1", [], HashType.INT)
    #if not error:
    #    print("hashget counter succ", members, values)
    #else:
    #    print("hashget counter error:", error)
    #    sys.exit(1)
