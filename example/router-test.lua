
local fibers_storage = {}
for i = 1, fibers_num do
    local f = fiber.create(fiber_load, instances.router_1,
                           (i - 1) * buckets_per_fiber)
    f:set_joinable(true)
    fibers_storage[i] = f
end
