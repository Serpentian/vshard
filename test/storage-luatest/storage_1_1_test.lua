local t = require('luatest')
local vconst = require('vshard.consts')
local vtest = require('test.luatest_helpers.vtest')
local vutil = require('vshard.util')

local group_config = {{engine = 'memtx'}, {engine = 'vinyl'}}

if vutil.feature.memtx_mvcc then
    table.insert(group_config, {
        engine = 'memtx', memtx_use_mvcc_engine = true
    })
    table.insert(group_config, {
        engine = 'vinyl', memtx_use_mvcc_engine = true
    })
end

local test_group = t.group('storage', group_config)

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 10
}
local global_cfg

test_group.before_all(function(g)
    cfg_template.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine
    global_cfg = vtest.config_new(cfg_template)

    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    vtest.cluster_rebalancer_disable(g)
end)

test_group.after_all(function(g)
    g.cluster:drop()
end)

--
-- Test how bucket_send preserves tuple field types over the network (gh-327).
--
test_group.test_bucket_send_field_types = function(g)
    -- Make a space with all the field types whose msgpack representation is
    -- nontrivial.
    local _, err = vtest.cluster_exec_each_master(g, function(engine)
        local format = {
            {'id', 'unsigned'},
            {'bid', 'unsigned'},
        }
        local i = 1
        local tuple = {i, box.NULL}
        -- Decimal appeared earlier, but this is when it can be used as a field
        -- type. Same logic with other types.
        if ivutil.version_is_at_least(2, 3, 0, nil, 0, 0) then
            ivutil.table_extend(format, {
                {'fdecimal', 'decimal'},
            })
            i = i + 1
            table.insert(tuple, require('decimal').new(i))
        end
        if ivutil.version_is_at_least(2, 4, 0, nil, 0, 0) then
            ivutil.table_extend(format, {
                {'fuuid', 'uuid'},
            })
            i = i + 1
            table.insert(tuple, ivtest.uuid_from_int(i))
        end
        if ivutil.version_is_at_least(2, 10, 0, nil, 0, 0) then
            ivutil.table_extend(format, {
                {'fdatetime', 'datetime'},
                {'finterval', 'interval'},
                {'fdouble', 'double'},
                {'fbinary', 'varbinary'},
            })
            local dt = require('datetime')
            i = i + 1
            table.insert(tuple, dt.new({year = i}))
            i = i + 1
            table.insert(tuple, dt.interval.new({year = i}))
            i = i + 1
            table.insert(tuple, require('ffi').cast('double', i))
            -- That is the simplest way to get an MP_BIN in Lua without FFI.
            i = i + 1
            local value = box.execute([[SELECT CAST(? AS VARBINARY)]],
                                      {tostring(i)}).rows[1]
            value = imsgpack.object(value):iterator()
            -- Skip tuple's MP_ARRAY header.
            value:decode_array_header()
            -- Get first and only field - MP_BIN.
            table.insert(tuple, value:take())
        end
        local s = box.schema.create_space('test', {
            engine = engine,
            format = format,
        })
        s:create_index('pk')
        s:create_index('bucket_id', {unique = false, parts = {2}})
        rawset(_G, 'test_tuple', tuple)
    end, {g.params.engine})
    t.assert_equals(err, nil, 'space creation no error')

    -- Send the bucket with a complicated tuple.
    local bid = g.replica_1_a:exec(function(dst)
        local bid = _G.get_first_bucket()
        local tuple = _G.test_tuple
        tuple[2] = bid
        box.space.test:replace(tuple)
        local ok, err = ivshard.storage.bucket_send(bid, dst,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil, 'bucket_send no error')
        ilt.assert(ok, 'bucket_send ok')
        return bid
    end, {g.replica_2_a:replicaset_uuid()})

    -- Ensure the tuple is delivered as is and fits into the space's format.
    g.replica_2_a:exec(function(bid)
        local src_tuple = _G.test_tuple
        src_tuple[2] = bid
        local dst_tuple = box.space.test:get{src_tuple[1]}
        -- Comparison unfortunately can only be done in Lua. Msgpack objects are
        -- incomparable which means the original MP_BIN wouldn't be equal to
        -- anything. But that should be safe anyway if the tuple managed to fit
        -- into the space.
        dst_tuple = dst_tuple:totable()
        src_tuple = box.tuple.new(src_tuple):totable()
        ilt.assert_equals(dst_tuple, src_tuple, 'tuple is delivered as is')
    end, {bid})

    -- Cleanup.
    g.replica_1_a:exec(function()
        _G.bucket_gc_wait()
    end)

    g.replica_2_a:exec(function(bid, dst)
        box.space.test:truncate()
        local ok, err = ivshard.storage.bucket_send(bid, dst,
                                                    {timeout = iwait_timeout})
        ilt.assert_equals(err, nil, 'bucket_send no error')
        ilt.assert(ok, 'bucket_send ok')
        _G.bucket_gc_wait()
    end, {bid, g.replica_1_a:replicaset_uuid()})

    vtest.cluster_exec_each_master(g, function()
        box.space.test:drop()
    end)
end

--
-- Custom triggers on bucket changes.
--
test_group.test_on_bucket_event = function(g)
    vtest.cluster_exec_each_master(g, function(engine)
        local event_space = box.schema.space.create('events', {
            engine = engine,
        })
        event_space:create_index('primary')

        local function data_space_new(name)
            local s = box.schema.space.create(name, {
                engine = engine,
                format = {
                    {'id', 'unsigned'},
                    {'bucket_id', 'unsigned'},
                },
            })
            s:create_index('id', {parts = {'id'}})
            s:create_index('bucket_id', {
                parts = {'bucket_id'}, unique = false
            })
        end
        data_space_new('data1')
        data_space_new('data2')

        local test_txn_id = 0
        local test_events = {}
        local test_commits = {}
        rawset(_G, 'test_trigger', function(event_type, bucket_id, data)
            test_txn_id = test_txn_id + 1
            local tid = test_txn_id
            local event_data = {event_type, bucket_id, data}
            ilt.assert(box.is_in_txn())
            ilt.assert(not test_events[tid])
            event_space:insert{tid, event_data}
            test_events[tid] = event_data

            box.on_commit(function(iterator)
                for _, _, _, space_id in iterator() do
                    if space_id == event_space.id then
                        test_commits[tid] = true
                        break
                    end
                end
            end)
        end)
        ivshard.storage.on_bucket_event(_G.test_trigger)

        rawset(_G, 'process_events', function(expected_events)
            -- Compare only the items, not looking at the order. Because the
            -- spaces can be sent in any order, can't rely on one.
            ilt.assert_items_equals(test_events, expected_events)
            -- Ensure that each saved event is also persisted.
            for tid, event_data in pairs(test_events) do
                ilt.assert_equals(box.space.events:get{tid}, {tid, event_data})
                ilt.assert_not_equals(test_commits[tid], nil)
                test_commits[tid] = nil
                box.space.events:delete{tid}
            end
            test_events = {}
            -- No unexpected events anywhere.
            ilt.assert(not next(test_commits))
            ilt.assert_equals(box.space.events.index[0]:min(), nil)
        end)
    end, {g.params.engine})

    local rs1_uuid = g.replica_1_a:exec(function()
        return box.info.cluster.uuid
    end)
    local bid = g.replica_2_a:exec(function(rs1_uuid)
        local bid = _G.get_first_bucket()
        box.space.data1:insert({10, bid})
        box.space.data2:insert({20, bid})
        local ok, err = ivshard.storage.bucket_send(
            bid, rs1_uuid, {timeout = _G.iwait_timeout})
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
        _G.bucket_gc_wait()
        return bid
    end, {rs1_uuid})

    g.replica_2_a:exec(function(events)
        _G.process_events(events)
    end, {{
        [1] = {vconst.BUCKET_EVENT.GC, bid, {spaces = {'data1'}}},
        [2] = {vconst.BUCKET_EVENT.GC, bid, {spaces = {'data2'}}},
    }})

    g.replica_1_a:exec(function(events)
        _G.process_events(events)
    end, {{
        [1] = {vconst.BUCKET_EVENT.RECV, bid, {spaces = {'data1'}}},
        [2] = {vconst.BUCKET_EVENT.RECV, bid, {spaces = {'data2'}}},
    }})

    -- Cleanup
    vtest.cluster_exec_each_master(g, function()
        ivshard.storage.on_bucket_event(nil, _G.test_trigger)
        box.space.events:drop()
        box.space.data1:drop()
        box.space.data2:drop()
    end)
end

--
-- Test that services for all background fibers are created
-- and work properly (gh-107).
--
test_group.test_basic_storage_service_info = function(g)
    local uuid = g.replica_1_a:exec(function()
        -- Test that all services save states
        local info = ivshard.storage.info({with_services = true})
        ilt.assert_not_equals(info.services, nil)
        ilt.assert_not_equals(info.services.gc, nil)
        ilt.assert_not_equals(info.services.recovery, nil)
        ilt.assert_not_equals(info.services.rebalancer, nil)
        -- Routes applier service is created as soon as it's needed
        ilt.assert_equals(info.services.routes_applier, nil)

        -- Forbid routes_apply service to die
        local internal = ivshard.storage.internal
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = true
        -- Break timeout in order to get error
        rawset(_G, 'chunk_timeout', ivconst.REBALANCER_CHUNK_TIMEOUT)
        ivconst.REBALANCER_CHUNK_TIMEOUT = 1e-6
        return box.info.cluster.uuid
    end)

    g.replica_2_a:exec(function(uuid)
        -- Send bucket to create disbalance in
        -- order to test routes applier service
        local bid = _G.get_first_bucket()
        local ok, err = ivshard.storage.bucket_send(bid, uuid)
        ilt.assert_equals(err, nil)
        ilt.assert(ok)
    end, {uuid})

    vtest.cluster_rebalancer_enable(g)

    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        local applier_name = 'routes_applier_service'
        ivtest.wait_for_not_nil(internal, applier_name)
        local service = internal[applier_name]
        ivtest.service_wait_for_error(service, 'Timed?[Oo]ut')

        -- Restore everything
        ivconst.REBALANCER_CHUNK_TIMEOUT = _G.chunk_timeout
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = false
        ivtest.wait_for_nil(internal, applier_name)
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = true

        -- All buckets must be recovered to the ACTIVE state,
        -- otherwise rebalancer won't work.
        ivshard.storage.recovery_wakeup()
        service = ivshard.storage.internal.recovery_service
        ivtest.service_wait_for_new_ok(service)
    end)

    g.replica_2_a:exec(function()
        ivshard.storage.recovery_wakeup()
        local service = ivshard.storage.internal.recovery_service
        ivtest.service_wait_for_new_ok(service)
    end)

    g.replica_1_a:exec(function()
        local internal = ivshard.storage.internal
        local applier_name = 'routes_applier_service'
        ivtest.wait_for_not_nil(internal, applier_name,
            {on_yield = ivshard.storage.rebalancer_wakeup})

        -- Everything is all right now
        ivtest.service_wait_for_ok(internal[applier_name])
        internal.errinj.ERRINJ_APPLY_ROUTES_STOP_DELAY = false

        ivtest.service_wait_for_ok(internal.rebalancer_service,
            {on_yield = ivshard.storage.rebalancer_wakeup})
    end)

    -- Cleanup
    vtest.cluster_rebalancer_disable(g)
end
