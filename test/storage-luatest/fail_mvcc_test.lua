local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {memtx_use_mvcc_engine = true})
end

local test_group = t.group('storage', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
            },
        },
    },
    bucket_count = 20
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_wait_vclock_all(g)
    vtest.cluster_rebalancer_disable(g)
    g.replica_1_a:exec(function()
        _G.bucket_recovery_pause()
    end)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

test_group.test_fail_mvcc = function(g)
    g.replica_1_a:exec(function()
        local _bucket = box.space._bucket
        local count = ivconst.DEFAULT_BUCKET_COUNT
        local bid1, bid2 = count + 1, count + 2
        _bucket:insert({bid1, ivconst.BUCKET.RECEIVING})
        _bucket:insert({bid2, ivconst.BUCKET.RECEIVING})
        _bucket:update(bid1, {{'=', 2, ivconst.BUCKET.GARBAGE}})
        _bucket:update(bid2, {{'=', 2, ivconst.BUCKET.ACTIVE}})
        _bucket:replace({bid1, ivconst.BUCKET.ACTIVE})
    end)
end
