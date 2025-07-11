local clock = require('clock')
local log = require('log')
local router = require('router')

local USAGELINE = [[

 Usage: tarantool generate_load.lua [options]

]]

local HELP = [[
   bucket_count <number, 3000>   - number of buckets in cluster
   fibers <number, 50>           - number of fibers to run simultaneously
   help (same as -h)             - print this message
   ops <number, 1000000>         - total amount of operations to be performed
   op_type <string, replace>     - which operation is used
   uri <string, localhost:3305>  - uri of the router
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

local params = require('internal.argparse').parse(arg, parsed_params)
if params.h or params.help then
    print(USAGELINE .. HELP)
    os.exit(0)
end

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

local instance = router.remote_new({uri = uri, bucket_count = bucket_count})
if warmup == true then
    local num = bucket_count / fibers_num > 100 and fibers_num or 1
    log.info("Warming up with %d fibers", num)
    instance:warmup(num)
end

-- Start timer.
local timer_begin = {
    clock.time(),
    clock.proc()
}

log.info("Performing %d operations", ops_num)
instance:workload(fibers_num, op_type, ops_num)

--------------------------------------------------------------------------------
-- Results.
--------------------------------------------------------------------------------

local real_time = clock.time() - timer_begin[1]
local cpu_time = clock.proc() - timer_begin[2]
local ops_done = ops_num - instance.stats.error_num

log.info('# cluster done %d ops in time: %f, cpu: %f',
         ops_done, real_time, cpu_time)
log.info('# cluster average speed: %f', ops_done / real_time)
log.info('# average latency: %f', instance.stats.latency_sum / ops_done)

require('os').exit(0)
