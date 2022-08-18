-- vshard.log_fiber
--
-- The wrapper around the log module, the purpose of which is to save last
-- log messages and states in the memory according to the fiber's names, by
-- which the logging was requested. The number of messages can be configured
-- via 'log_vshard_background' option in vshard.*.cfg(). This option can also
-- be a boolean (false - logs are only forwarded to the log module, states are
-- not saved; true - default value of saved log messages is used).
--
-- <log_fiber> = {
--     logs = {
--         [fiber_name] = {
--             state = <string, which shows in which state the fiber is
--                      right now (e.g. error, sleeping ...),
--             messages = {
--                 <log_message> = {
--                     time = number,
--                     level = string,
--                     data = string,
--                 },
--             },
--         },
--     },
--     is_enabled = <boolean, which indicates whether states and log messages
--                   are needed to be saved>
--     log_length = <the number of messages saved in 'logs' heap>
-- }
--

local log = require('log')
local fiber = require('fiber')
local heap = require('vshard.heap')
local util = require('vshard.util')
local consts = require('vshard.consts')
local fiber_clock = fiber.clock
local gsc = util.generate_self_checker

local json = require("json").new()
json.cfg{
    encode_invalid_numbers = true,
    encode_load_metatables = true,
    encode_use_tostring    = true,
    encode_invalid_as_nil  = true,
}

local LOG_FIBER_TEMPLATE = {
    -- Just forward if not enabled. Disabled by default.
    is_enabled = nil,
    -- Max number of messages saved for every fiber.
    log_length = nil,
    -- All known fibers with their logs and states.
    logs = {}
}

local lvl = {
    ERROR   = 2,
    WARN    = 4,
    INFO    = 5,
    VERBOSE = 6,
    DEBUG   = 7,
}

local DEFAULT_LOG_LEVEL = lvl.INFO

local lvl2func = {
    [lvl.ERROR]   = log.error,
    [lvl.WARN]    = log.warn,
    [lvl.INFO]    = log.info,
    [lvl.VERBOSE] = log.verbose,
    [lvl.DEBUG]   = log.debug,
}

local lvl2char = {
    [lvl.ERROR]   = 'E',
    [lvl.WARN]    = 'W',
    [lvl.INFO]    = 'I',
    [lvl.VERBOSE] = 'V',
    [lvl.DEBUG]   = 'D',
}

--
-- Helper function to get the number of seconds, minutes or hours
-- that past since the time.
--
local function time_past_since(time)
    local time_past = math.floor(fiber_clock() - time)
    local units = {'s', 'm', 'h'}
    for i, unit in ipairs(units) do
        if time_past < 60 or not next(units, i) then
            return time_past, unit
        end
        time_past = math.floor(time_past / 60)
    end
end

-- In some versions of tarantool (e.g. in 1.10) it's impossibe to get the
-- current configuration of the log module. The only way to get log_level is
-- box.cfg. Moreover, in these versions the option is not shared among box.cfg
-- and log.level(): e.g. if log.level is set to 7, in box.cfg it will still
-- have a default value (5).
local function get_log_level()
    -- In most cases log has its own cfg
    if log.cfg then
        return log.cfg.level
    end
    -- We can also extract log_level from box.cfg
    if type(box.cfg) ~= 'function' then
        return box.cfg.log_level
    end
    -- Overwise use default value
    return DEFAULT_LOG_LEVEL
end

--
-- Binary heap sort. The most old messages should be on top.
--
local function heap_min_time_cmp(msg1, msg2)
    return msg1.time < msg2.time
end

--
-- Creates the default storage for the messages and states if
-- it doesn't exist. Overwise, just return it.
--
local function logs_get_or_create(logs, fiber_name)
    local log = logs[fiber_name]
    if not log then
        logs[fiber_name] = {
            state = 'unknown',
            messages = heap.new(heap_min_time_cmp)
        }
        log = logs[fiber_name]
    end
    return log
end

--------------------------------------------------------------------------------
-- Log fiber methods
--------------------------------------------------------------------------------

local function log_fiber_say(log_fiber, lvl, fmt, ...)
    -- Forward to the log module. It's better to do it before saving as if
    -- there's an error (e.g. in format) it will be thrown now.
    lvl2func[lvl](fmt, ...)
    -- Write the message to the log_fiber
    if log_fiber.is_enabled and log_fiber.log_length ~= 0 and
            lvl <= get_log_level() then
        local messages = logs_get_or_create(log_fiber.logs,
                                            fiber.self().name()).messages
        -- Drop the oldest message
        if messages:count() >= log_fiber.log_length then
            messages:pop()
        end

        -- Format the data
        local type_fmt = type(fmt)
        if select('#', ...) ~= 0 then
            fmt = string.format(fmt, ...)
        elseif type_fmt == 'table' then
            -- Log fiber doesn't ignore any special fields (e.g. file, pid)
            -- if the log message is in json format
            fmt = json.encode(fmt)
        elseif type_fmt ~= 'string' then
            fmt = tostring(fmt)
        end

        -- Push a new message
        messages:push({
            time = fiber_clock(),
            level = lvl,
            data = fmt,
            -- Used by the heap.
            index = -1,
        })
    end
end

-- Just a syntactic sugar over log_fiber_say routine.
local function log_fiber_say_closure(lvl)
    return function (log_fiber, fmt, ...)
        log_fiber_say(log_fiber, lvl, fmt, ...)
    end
end

local function log_fiber_set_state(log_fiber, new_state)
    local log = logs_get_or_create(log_fiber.logs, fiber.self().name())
    log.state = new_state
end

-- Get info about all known fibers
-- @retval table, consists of the fiber names with the corresponding
--         states and formatted log messages
local function log_fiber_get_info(log_fiber)
    local info = {}
    for name, log in pairs(log_fiber.logs) do
        info[name] = {
            state = log.state,
            last_messages = {},
        }
        local count = log.messages:count()
        if count > 0 then
            -- Old messages are on top, reverse order.
            for i = count, 1, -1 do
                local msg = log.messages.data[i]
                local time, unit = time_past_since(msg.time)
                info[name].last_messages[count - i + 1] = tostring(time) ..
                    unit .. ' ' .. lvl2char[msg.level] .. '> ' .. msg.data
            end
        end
    end
    return info
end

--
-- Drop logs for specified fibers.
-- @param fiber_name the name if the fiber for which the logs will be dropped
-- Note: if no fiber_name is passed, all logs will be dropped
--
local function log_fiber_drop(log_fiber, fiber_name)
    if fiber_name then
        log_fiber.logs[fiber_name] = nil
        return
    end
    log_fiber.logs = {}
end

-- Do nothing if the instance is disabled
local function log_fiber_make_enable_api(func)
    return function(log_fiber, arg1)
        if not log_fiber.is_enabled then
            return nil
        end
        return func(log_fiber, arg1)
    end
end

--
-- Configure logger according to the 'log_vshard_background' option from
-- vshard's configuration table:
--
--   * If the option is not set or it's value is false, then messages are
--     just forwarded to the log module.
--   * If the type of the option is a number, it's used as 'log_length'.
--   * If it's true, then default number of messages from consts is used.
--
-- Note: if 'log_vshard_background' is == 0, only fiber states will be saved.
--
local function log_fiber_cfg(log_fiber, cfg)
    local cfg_opt = cfg and cfg.log_vshard_background
    if not cfg_opt then
        log_fiber.is_enabled = false
        -- Drop everything
        log_fiber.logs = {}
        return
    end
    log_fiber.is_enabled = true
    log_fiber.log_length = cfg_opt ~= true and cfg_opt or
        consts.DEFAULT_FIBER_LOG_LENGTH
end

--------------------------------------------------------------------------------
-- Managing logger instance
--------------------------------------------------------------------------------

local log_fiber_mt = {
    __index = {
        cfg = log_fiber_cfg,
        drop = log_fiber_make_enable_api(log_fiber_drop),
        get_info = log_fiber_make_enable_api(log_fiber_get_info),
        set_state = log_fiber_make_enable_api(log_fiber_set_state),
        -- Wrappers around the log module
        info = log_fiber_say_closure(lvl.INFO),
        warn = log_fiber_say_closure(lvl.WARN),
        debug = log_fiber_say_closure(lvl.DEBUG),
        error = log_fiber_say_closure(lvl.ERROR),
        verbose = log_fiber_say_closure(lvl.VERBOSE),
    }
}

--
-- Wrap self methods with a sanity checker.
--
local index = {}
for name, func in pairs(log_fiber_mt.__index) do
    index[name] = gsc("log_fiber", name, log_fiber_mt, func)
end
log_fiber_mt.__index = index

--
-- Create a new instance of the log_fiber.
-- @param cfg Configuration table for vshard
-- @retval log_fiber instance.
--
local function log_fiber_new(cfg)
    local log_fiber = table.deepcopy(LOG_FIBER_TEMPLATE)
    setmetatable(log_fiber, log_fiber_mt)
    log_fiber_cfg(log_fiber, cfg)
    return log_fiber
end

return {
    new = log_fiber_new,
}
