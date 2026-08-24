-- Router-side benchmark of storage.call vs storage_c.call.
-- VSHARD_C_CALL in the environment selects which storage-side
-- function vshard.router.call invokes. The storage must already
-- run (see run.sh).
local clock = require('clock')
local fiber = require('fiber')

local RS_UUID = 'cbf06940-0790-498b-948d-042b62cf3d29'
local INST_UUID = '8a274925-a26d-47fc-9e1b-af88ce939412'
local BUCKET_COUNT = 3000
local BENCH_SPACE_ID = 777

local FIBERS = tonumber(os.getenv('BENCH_FIBERS')) or 10
local DURATION = tonumber(os.getenv('BENCH_DURATION')) or 10
local WARMUP = tonumber(os.getenv('BENCH_WARMUP')) or 2
-- Sample every Nth request latency to keep the overhead low.
local SAMPLE = 16

local cfg = {
    bucket_count = BUCKET_COUNT,
    discovery_mode = 'on',
    sharding = {
        [RS_UUID] = {
            replicas = {
                [INST_UUID] = {
                    uri = 'storage:storage@127.0.0.1:3311',
                    name = 'storage_1_a',
                    master = true,
                },
            },
        },
    },
}

local vshard = require('vshard')
vshard.router.cfg(cfg)

-- Wait for the storage and discovery.
local deadline = clock.monotonic() + 60
while true do
    local ok = vshard.router.callrw(1, 'echo_lua', {1}, {timeout = 5})
    if ok ~= nil then
        break
    end
    if clock.monotonic() > deadline then
        io.stderr:write('storage is not available\n')
        os.exit(1)
    end
    fiber.sleep(0.1)
end

local payload = string.rep('x', 32)

local workloads = {
    {
        name = 'replace C',
        mode = 'write',
        call = function(i, bid)
            return vshard.router.callrw(bid,
                'bench_c.replace',
                {BENCH_SPACE_ID, {i % 100000, bid, payload}},
                {timeout = 10})
        end,
    },
    {
        name = 'replace Lua',
        mode = 'write',
        call = function(i, bid)
            return vshard.router.callrw(bid, 'bench_replace_lua',
                {{i % 100000, bid, payload}}, {timeout = 10})
        end,
    },
    {
        name = 'get C',
        mode = 'read',
        call = function(i, bid)
            return vshard.router.callro(bid, 'bench_c.get',
                {BENCH_SPACE_ID, {i % 100000}}, {timeout = 10})
        end,
    },
    {
        name = 'get Lua',
        mode = 'read',
        call = function(i, bid)
            return vshard.router.callro(bid, 'bench_get_lua',
                {i % 100000}, {timeout = 10})
        end,
    },
}

local function percentile(sorted, p)
    if #sorted == 0 then
        return 0
    end
    local idx = math.max(1, math.ceil(#sorted * p))
    return sorted[idx]
end

local function run_workload(w)
    local stop = false
    local measuring = false
    local counts = {}
    local errors = 0
    local lats = {}
    local fibers = {}
    for f = 1, FIBERS do
        counts[f] = 0
        fibers[f] = fiber.new(function()
            local i = f * 1000003
            while not stop do
                i = i + 1
                local bid = i % BUCKET_COUNT + 1
                local sample = measuring and i % SAMPLE == 0
                local t0
                if sample then
                    t0 = clock.monotonic64()
                end
                local ok, err = w.call(i, bid)
                if ok == nil and err ~= nil then
                    errors = errors + 1
                elseif sample then
                    table.insert(lats,
                                 tonumber(clock.monotonic64() - t0) / 1e3)
                end
                counts[f] = counts[f] + 1
            end
        end)
        fibers[f]:set_joinable(true)
    end
    local function total()
        local s = 0
        for f = 1, FIBERS do
            s = s + counts[f]
        end
        return s
    end
    fiber.sleep(WARMUP)
    local c0 = total()
    lats = {}
    measuring = true
    local t0 = clock.monotonic()
    fiber.sleep(DURATION)
    local c1 = total()
    local elapsed = clock.monotonic() - t0
    stop = true
    for f = 1, FIBERS do
        fibers[f]:join()
    end
    table.sort(lats)
    return {
        rps = (c1 - c0) / elapsed,
        p50 = percentile(lats, 0.50),
        p95 = percentile(lats, 0.95),
        p99 = percentile(lats, 0.99),
        errors = errors,
    }
end

local is_c = os.getenv('VSHARD_C_CALL')
is_c = is_c ~= nil and is_c ~= '' and is_c ~= '0'
local mode = is_c and 'storage_c.call (C)' or 'storage.call (Lua)'
print(('# wrapper: %s, fibers: %d, duration: %ds'):format(
      mode, FIBERS, DURATION))
print(('%-22s %10s %9s %9s %9s %7s'):format(
      'workload', 'RPS', 'p50(us)', 'p95(us)', 'p99(us)', 'errors'))
for _, w in ipairs(workloads) do
    local r = run_workload(w)
    print(('%-22s %10.0f %9.1f %9.1f %9.1f %7d'):format(
          'router '..w.name, r.rps, r.p50, r.p95, r.p99, r.errors))
end

-- Secondary series: direct net.box calls to the storage, no
-- router overhead - isolates the storage-side cost.
local netbox = require('net.box')
local conn = netbox.connect('storage:storage@127.0.0.1:3311')
conn:wait_connected(5)
local call_name = is_c and 'vshard.storage_c.call' or 'vshard.storage.call'
local direct = {
    {
        name = 'replace C',
        call = function(i, bid)
            return conn:call(call_name, {bid, 'write',
                'bench_c.replace',
                {BENCH_SPACE_ID, {i % 100000, bid, payload}}})
        end,
    },
    {
        name = 'replace Lua',
        call = function(i, bid)
            return conn:call(call_name, {bid, 'write', 'bench_replace_lua',
                {{i % 100000, bid, payload}}})
        end,
    },
    {
        name = 'get C',
        call = function(i, bid)
            return conn:call(call_name, {bid, 'read',
                'bench_c.get',
                {BENCH_SPACE_ID, {i % 100000}}})
        end,
    },
    {
        name = 'get Lua',
        call = function(i, bid)
            return conn:call(call_name, {bid, 'read', 'bench_get_lua',
                {i % 100000}})
        end,
    },
}
for _, w in ipairs(direct) do
    local r = run_workload(w)
    print(('%-22s %10.0f %9.1f %9.1f %9.1f %7d'):format(
          'direct '..w.name, r.rps, r.p50, r.p95, r.p99, r.errors))
end
os.exit(0)
