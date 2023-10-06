import pybbt as p

import helpers as h


def test(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))

    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}/appendonlydir/appendonly.aof.manifest", dst)
    p.log(f"opts: {opts}")
    p.log("aof test start")
    h.Shake.run_once(opts)

    p.log("aof test check_data")
    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())


@p.subcase()
def aof_to_standalone():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof 
    src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Redis()
    test(src, dst)


@p.subcase()
def aof_to_cluster():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    
    src = h.Redis()
    #set aof
    src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Cluster()
    test(src, dst)


@p.case(tags=["sync"])
def main():
    aof_to_standalone()
    aof_to_cluster()


if __name__ == '__main__':
    main()
