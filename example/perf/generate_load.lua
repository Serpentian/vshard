local clock = require('clock')
local log = require('log')
local router = require('router')
local benchmark = require('benchmark')

local USAGELINE = [[

 Usage: tarantool generate_load.lua [options]

]]

local HELP = [[
   bucket_count <number, 3000>   - number of buckets in cluster
   fibers <number, 50>           - number of load fibers to run per router
   help (same as -h)             - print this message
   ops <number, 1000000>         - total amount of operations to be performed
   op_type <string, replace>     - which operation is used
   uri <string, localhost:3305>  - uri(s) of the router(s), comma separated
   warmup <boolean, false>       - whether warmup of the refs is needed

]]

local parsed_params = {
    {'bucket_count', 'number'},
    {'fibers', 'number'},
    {'ops', 'number'},
    {'op_type', 'string'},
    {'uri', 'string'},
    {'warmup', 'boolean'},
    {'h', 'boolean'},
    {'help', 'boolean'},
}

--------------------------------------------------------------------------------
-- Parse command line arguments
--------------------------------------------------------------------------------

local params = benchmark.argparse(arg, parsed_params, HELP)
local bench = benchmark.new(params)

-- Default values.
local bucket_count = params.bucket_count or 3000
local fibers_num = params.fibers or 50
local ops_num = params.ops or 1000000
local op_type = params.op_type or 'replace'
local uri = params.uri or 'localhost:3305'
local warmup = params.warmup or false

--------------------------------------------------------------------------------
-- Test performance
--------------------------------------------------------------------------------

local instances = {}
for u in string.gmatch(uri, '[^%,]+') do
    local instance = router.remote_new({uri = u, bucket_count = bucket_count})
    table.insert(instances, instance)
end

if warmup == true then
    local num = bucket_count / fibers_num > 100 and fibers_num or 1
    log.info("Warming up with %d fibers", num)
    instances[1]:warmup(num)
end

-- Start timer.
local timer_begin = {
    clock.time(),
    clock.proc()
}

local ops_per_router = math.floor(ops_num / #instances)
log.info("Performing %d operations", ops_num)
router.fiber_pool_do(function(i, routers)
    routers[i]:workload(fibers_num, op_type, ops_per_router)
end, #instances, instances)

--------------------------------------------------------------------------------
-- Results.
--------------------------------------------------------------------------------

local real_time = clock.time() - timer_begin[1]
local cpu_time = clock.proc() - timer_begin[2]
local ops_done = ops_num
local latency_sum = 0
for _, instance in ipairs(instances) do
    ops_done = ops_done - instance.stats.error_num
    latency_sum = latency_sum + (instance.stats.latency_sum / ops_per_router)
end

local res = bench:add_result('generate_load', {
    real_time = real_time,
    cpu_time = cpu_time,
    items = ops_done,
})

log.info('# cluster done %d ops in time: %f, cpu: %f',
         ops_done, res.real_time, res.cpu_time)
log.info('# cluster average speed: %f', res.items_per_second)
log.info('# average latency: %f', latency_sum / #instances)
bench:dump_results()

require('os').exit(0)
