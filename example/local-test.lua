local clock = require('clock')
local log = require('log')
local fiber = require('fiber')
local net_box = require('net.box')

local BID = 15001
local ITERS = 1000000
local TUPLE = {BID}
local OPTIONS = {{iterator = 'GE', limit = 7}}

local mean_storage_call = 0
local mean_no_storage_call = 0

fiber.set_max_slice(10)
local start_t, tmp_mean
for i = 1,10 do
    local start_t = clock.time();
    for i = 1,ITERS do
        vshard.storage.call(BID, 'read', 'box.space.customer:select', {TUPLE, OPTIONS})
    end
    mean_storage_call = mean_storage_call + (clock.time() - start_t) / ITERS
    fiber.yield()

    start_t = clock.time();
    for i = 1,ITERS do
        box.space.customer:select(TUPLE, OPTIONS)
    end
    mean_no_storage_call = mean_no_storage_call + (clock.time() - start_t) / ITERS
    fiber.yield()
end

mean_storage_call = mean_storage_call / 10
mean_no_storage_call = mean_no_storage_call / 10
log.warn('STORAGE_CALL: %.8f', mean_storage_call)
log.warn('NO_STORAGE_CALL: %.8f', mean_no_storage_call)
log.warn('PERCENTAGE: %.5f', mean_storage_call / mean_no_storage_call)
