--
-- Usage:
--
-- local router = require('router')
-- local clock = require('clock')
--
-- local remote = router.remote_new({
--     uri = 'localhost:3301',
--     bucket_count = 3000,
-- })
-- remote:warmup(10)
--
-- local start_time = {
--     time = clock.time(),
--     proc = clock.proc(),
-- }
--
-- remote:workload(10, 'get', 1000)
--
-- real_time = clock.time() - start_time.time
-- cpu_time = clock.proc() - start_time.proc
--

local clock = require('clock')
local net_box = require('net.box')
local fiber = require('fiber')
local log = require('log')

--------------------------------------------------------------------------------
-- Operations
--------------------------------------------------------------------------------

local operations = {
    ['replace'] = {
        request_type = 'vshard.router.callrw',
        func_name = 'box.space.customer:replace',
        tuple = function(bid) return {bid, bid, 'name'} end,
        options = {},
    },
    ['get'] = {
        request_type = 'vshard.router.callro',
        func_name = 'box.space.customer:get',
        tuple = function(bid) return {bid} end,
        options = {},
    },
    ['select'] = {
        request_type = 'vshard.router.callro',
        func_name = 'box.space.customer:select',
        tuple = function(bid) return {bid} end,
        options = {iterator = 'GE', limit = 50},
    },
}

--------------------------------------------------------------------------------
-- Utils
--------------------------------------------------------------------------------

local function fiber_join(f)
    local ok, err = f:join()
    if not ok then
        log.warn(('Failed to join fiber: %s'):format(err))
    end
end

local function fiber_pool_do(func, fibers_num, args)
    local fibers = {}
    for i = 1, fibers_num do
        local f = fiber.create(func, i, args)
        f:set_joinable(true)
        fibers[i] = f
    end
    for _, f in ipairs(fibers) do
        fiber_join(f)
    end
end

local function uri_connect(uri)
    local conn_opts = {reconnect_after = 0.5, wait_connected = 10}
    local c = net_box.connect(uri, conn_opts)
    local is_connected = c:wait_connected(30)
    if not is_connected then
        error('Could not connect to the instance %s', uri)
    end
    return c
end

--------------------------------------------------------------------------------
-- Remote router
--------------------------------------------------------------------------------

local function router_remote_get_call_func(_, op_name, conn)
    local operation = operations[op_name]
    if operation == nil then
        error('Unknown operation')
    end

    return function(bucket_id)
        return conn:call(operation.request_type, {bucket_id,
            operation.func_name,
            {operation.tuple(bucket_id), operation.options}
        })
    end
end

local function warmup_f(worker_num, args)
    local start = (worker_num - 1) * args.buckets_per_fiber + 1
    for bid = start, start + args.buckets_per_fiber - 1 do
        args.warmup_func(bid)
    end
end

local function router_remote_warmup(router, fibers_num)
    local conn = uri_connect(router.uri)
    fiber_pool_do(warmup_f, fibers_num, {
        buckets_per_fiber = router.bucket_count / fibers_num,
        warmup_func = router:get_call_func('replace', conn),
    })
    conn:close()
end

local function load_remote_router_f(worker_num, args)
    local router = args.router
    local c = uri_connect(router.uri)
    local op = router:get_call_func(args.op_name, c)
    local bid = (worker_num - 1) * args.buckets_per_fiber
    for _ = 1, args.ops_per_fiber do
        if bid == 0 then bid = 1 end
        local start_ts = clock.time()
        local _, err = op(bid )
        local latency = clock.time() - start_ts
        bid = (bid + 1) % router.bucket_count
        -- May be box.NULL.
        if err ~= nil then
            log.warn(err)
            router.stats.error_num = router.stats.error_num + 1
        else
            router.stats.latency_sum = router.stats.latency_sum + latency
        end
    end
    c:close()
end

local function router_remote_workload(router, fibers_num, op_name, ops_num)
    fiber_pool_do(load_remote_router_f, fibers_num, {
        buckets_per_fiber = router.bucket_count / fibers_num,
        ops_per_fiber = ops_num / fibers_num,
        op_name = op_name,
        router = router,
    })
end

local router_remote_mt = {
    __index = {
        warmup = router_remote_warmup,
        get_call_func = router_remote_get_call_func,
        workload = router_remote_workload,
    }
}

local function router_remote_new(opts)
    assert(opts.bucket_count)
    assert(opts.uri)
    local router = {
        bucket_count = opts.bucket_count,
        uri = opts.uri,
        stats = {
            latency_sum = 0,
            error_num = 0,
        },
    }
    return setmetatable(router, router_remote_mt)
end

--------------------------------------------------------------------------------
-- Module definition
--------------------------------------------------------------------------------

return {
    remote_new = router_remote_new,
}
