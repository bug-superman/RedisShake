import pybbt as p

import helpers as h

import os

def get_aof_file_relative_path():
    if h.REDIS_SERVER_VERSION  == 7.0:
        aof_file = "/appendonlydir/appendonly.aof.manifest"
    else:
        aof_file = "/appendonly.aof"
    return aof_file
    
def test(src, dst):

    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))
   
    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())

def test_error(src, dst):
    #set aof 
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))
    #destroy file
    file_path = src.dir + get_aof_file_relative_path()
    with open(file_path, "r+") as file:
        destroy_data = "xxxxs"
        file.seek(0, 0)
        file.write(destroy_data)


    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}/appendonlydir/appendonly.aof.manifest", dst)
    p.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    #cant restore
    p.ASSERT_EQ( dst.dbsize(), 0)



def test_rm_file(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))
    #rm file
    file_path = src.dir + "/appendonlydir/appendonly.aof.1.base.rdb"
    os.remove(file_path)
    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    #cant restore
    p.ASSERT_EQ(dst.dbsize(), 0)

def test_timestamp(dst):
    current_directory = os.getcwd()
    opts = h.ShakeOpts.create_aof_opts(f"{current_directory}{get_aof_file_relative_path()}", dst, 1697476302)
    h.Shake.run_once(opts)
    #checkout data 
    pip = dst.pipeline()
    prefix = "string"
    i = 0
    pip.get(f"{prefix}_{i}_str")
    pip.get(f"{prefix}_{i}_int")
    pip.get(f"{prefix}_{i}_int0")
    pip.get(f"{prefix}_{i}_int1")
    ret = pip.execute()
    p.ASSERT_EQ(ret, [b"string", b"0", b"-1", b"123456789"]) 
    p.ASSERT_EQ(dst.dbsize(), 4)

@p.subcase()
def aof_to_standalone():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof 
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")

    ret = src.do("CONFIG SET", "aof-timestamp-enabled", "yes")
    p.log(f"aof_ret: {ret}")
    dst = h.Redis()
    test(src, dst)
  

@p.subcase()
def aof_to_standalone_rm_file():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof 
    ret = src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Redis()
    test_rm_file(src, dst)

@p.subcase()
def aof_to_standalone_error():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof 
    ret = src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Redis()
    test_error(src, dst)

@p.subcase()
def aof_to_cluster():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof 
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")
    dst = h.Cluster()
    test(src, dst)

@p.subcase()
def aof_to_standalone_single():
    if h.REDIS_SERVER_VERSION >= 7.0:
        return
    src = h.Redis()
    #set preamble no
    ret = src.do("CONFIG SET", "aof-use-rdb-preamble", "no")
    p.log(f"aof_ret: {ret}")
    #set aof 
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")
    dst = h.Redis()
    test(src, dst)

@p.subcase()
def aof_to_standalone_timestamp():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    dst = h.Redis()
    test_timestamp(dst)
    
@p.case(tags=["sync"])
def main():
    aof_to_standalone()
    aof_to_standalone_single()
    aof_to_standalone_error()
    aof_to_standalone_rm_file()
    aof_to_cluster()
    aof_to_standalone_timestamp()
if __name__ == '__main__':
    main()
