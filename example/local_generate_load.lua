local router = require('perf.router')
local clock = require('clock')
local log = require('log')

local BUCKET_COUNT = 3000
local OPS_NUM = 1000000
local OP_TYPE = 'get'
local FIBERS_NUM = 50
local WARMUP = true

local instance = router.local_new({bucket_count = BUCKET_COUNT})
if WARMUP == true then
    local num = BUCKET_COUNT / FIBERS_NUM > 100 and FIBERS_NUM or 1
    log.info("Warming up with %d fibers", num)
    instance:warmup(num)
end

-- Start timer.
local timer_begin = {
    clock.time(),
    clock.proc()
}

log.info("Performing %d operations", OPS_NUM)
instance:workload(FIBERS_NUM, OP_TYPE, OPS_NUM)

--------------------------------------------------------------------------------
-- Results.
--------------------------------------------------------------------------------

local real_time = clock.time() - timer_begin[1]
local cpu_time = clock.proc() - timer_begin[2]
local ops_done = OPS_NUM - instance.stats.error_num

log.info('# cluster done %d ops in time: %f, cpu: %f',
                    ops_done, real_time, cpu_time)
log.info('# cluster average speed: %f', ops_done / real_time)
log.info('# average latency: %f',
                    instance.stats.latency_sum / ops_done)
