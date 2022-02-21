import socket
import sys
import time
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from sharestore_lib.SharestorePool import SharestorePool
from sharestore_lib.SharestorePool import HashType
if __name__ == "__main__":
    #options = {'host': '172.26.83.192', 'port': 9090}
    options = {'host': 'prod.sprs.cbs.sg2.sharestore', 'port': 9090}
    #options = {'host': '172.26.95.190', 'port': 9090}
    #options = {'host': 'prod.ads-od.cbs.sg1.sharestore', 'port': 9090}
    #options = {'host': 'prod.ads-extract.cbs.sg1.sharestore', 'port': 9090}
    pool = SharestorePool(max_size=10, options=options)
    pool.SetTimeout(timeout_ms=1000)
    # hashset
    #ret, error = pool.HashSetValue("cbs_usp_user_setting_prod", "7EkqA_testtjk", ["member001", "member002", "testmem003", "testmem004"], ["value001", "value002", "value003", "value004"], [0, 0, 0, 0])
    #if ret:
    #    print("hashset succ")
    #else:
    #    print("hashset error:", error)
    #    sys.exit(1)
    #time.sleep(2)
    # hashget all
    #members, values, error = pool.HashGetValue("ads_rcmd_user_profile_pb_prod", "OG_user_base_e7705548beeb43208ce304b4f78efcbe", [], HashType.BUF)
    members, values, version, error = pool.HashExGetValue("sprs_userinfo_profile_prod", "realauthor_{3lq14yl}", [], HashType.STR)
    if not error:
        #print("hashget succ", members, values, len(values[0].buf_val))
        print("hashget succ", members, values, version)
    else:
        print("hashget error:", error)
        sys.exit(1)
    # hashrem
    ret, error = pool.HashExRemValue("sprs_userinfo_profile_prod", "realauthor_{3lq14y}", version, [], HashType.STR)
    if ret:
        print("hashrem counter succ")
    else:
        print("hashrem counter error:", error)
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
