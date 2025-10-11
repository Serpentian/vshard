--
-- The module is intended for limiting the number of messages, thrown to
-- logs, so that there's no more than 1 per consts.LOG_RATELIMIT_INTERVAL
-- is logged.
--
local consts = require('vshard.consts')
local lheap = require('vshard.heap')
local fiber = require('fiber')
local log = require('log')

--
-- Returns true, if message can be printed in logs, false otherwise. Arguments:
--     * new_entry - used for deduplication of the log entries. Structure: {
--         type = <string>,
--         code = <int>,
--         name = <string>,
--     }
--
local function ratelimit_can_log_impl(limiter, new_entry)
    if (consts.LOG_RATELIMIT_INTERVAL<= 0 or not new_entry) then
        return true
    end
    local entry_type, code
    if (type(limiter.custom_get_entry_signature) == 'function') then
        entry_type, code = limiter.custom_get_entry_signature(new_entry)
        if type(entry_type) ~= 'string' or type(code) ~= 'number' then
            return false
        end
    else
        entry_type, code = new_entry.type, new_entry.code
    end
    assert(entry_type and code)
    local map = limiter.map
    local existing = map[entry_type] and map[entry_type][code]
    if existing then
        existing.suppressed = existing.suppressed + 1
        return false
    end
    local new_map_entry = {
        entry = new_entry,
        suppressed = 0,
    }
    local map_type = map[entry_type] or {}
    map_type[code] = new_map_entry
    map[entry_type] = map_type
    local new_heap_entry = {
        deadline = fiber.clock() + consts.LOG_RATELIMIT_INTERVAL,
        type = entry_type,
        code = code,
    }
    limiter.heap:push(new_heap_entry)
    return true
end

local function ratelimit_flush(limiter)
    local heap = limiter.heap
    if heap:count() == 0 then
        return
    end
    local map = limiter.map
    local current_ts = fiber.clock()
    while heap:top() and heap:top().deadline <= current_ts do
        local top = heap:pop()
        assert(map[top.type] and map[top.type][top.code])
        local map_entry = map[top.type][top.code]
        map[top.type][top.code] = nil
        if map[top.type] == {} then
            map[top.type] = nil
        end
        if map_entry.suppressed > 0 then
            local e = map_entry.entry
            -- Some errors can have no names (e.g. `SocketError`).
            log.info("Suppressed %d '%s' messages from '%s'",
                     map_entry.suppressed,
                     e.name or string.format('%s.%s', e.type, e.code),
                     limiter.name)
        end
    end
end

local function ratelimit_can_log(limiter, new_entry)
    ratelimit_flush(limiter)
    return ratelimit_can_log_impl(limiter, new_entry)
end

local ratelimit_mt = {
    __index = {
        flush = ratelimit_flush,
        can_log = ratelimit_can_log,
    }
}

local function heap_min_deadline_cmp(entry1, entry2)
    return entry1.deadline < entry2.deadline
end

local function ratelimit_new(cfg)
    cfg = cfg or {}
    local ratelimit = {
        name = cfg.name or 'default',
        --
        -- Map has the following structure: {
        --     <type1, string> = {
        --         <code1, int> = entry,
        --         <code2, int> = entry,
        --         <...>
        --     },
        --     <...>
        -- }
        --
        map = {},
        --
        -- Heap is used for sorting the entries by their deadline and flushing
        -- the map and heap, when new entry appears.
        --
        heap = lheap.new(heap_min_deadline_cmp),
        --
        -- By default the ratelimit filters the entries according to their
        -- type and code, however, this is not enough in some cases, for which
        -- this functions exists. It accepts as argument the entry and
        -- returns either the type and code or nil. In case of nil the entry
        -- should not be printed.
        --
        custom_get_entry_signature = cfg.custom_get_entry_signature,
    }
    setmetatable(ratelimit, ratelimit_mt)
    return ratelimit
end

return {
    new = ratelimit_new,
}
