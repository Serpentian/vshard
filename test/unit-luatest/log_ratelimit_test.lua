local t = require('luatest')
local server = require('test.luatest_helpers.server')
local verror = require('vshard.error')
local vratelimit = require('vshard.log_ratelimit')
local vutil = require('vshard.util')

local test_group = t.group('log_ratelimit')

test_group.before_all(function(g)
    g.server = server:new({alias = 'node'})
    g.server:start()
    g.server:exec(function()
        rawset(_G, 'ivratelimit', require('vshard.log_ratelimit'))
        rawset(_G, 'iverror', require('vshard.error'))
    end)
end)

test_group.after_all(function(g)
    g.server:drop()
end)

test_group.test_get_entry_signature = function()
    -- The custom entry signature is not set.
    local limiter = vratelimit.new()
    t.assert(limiter:can_log(nil))
    local err = verror.box(box.error.new(box.error.NO_CONNECTION))
    t.assert(limiter:can_log(err))
    t.assert(limiter.map[err.type][err.code])
    err = verror.vshard(verror.code.NO_SUCH_REPLICASET, 'rs1')
    t.assert(limiter:can_log(err))
    t.assert(limiter.map[err.type][err.code])
    err = verror.make('Some error')
    t.assert(limiter:can_log(err))
    t.assert(limiter.map[err.type][err.code])

    -- Custom parsing of the signature.
    local custom_type = 'CustomError'
    local function custom(entry)
        if entry.type == 'ClientError' and entry.code == box.error.PROC_LUA then
            if string.find(entry.message, 'do_write') then
                return custom_type, 1
            elseif string.find(entry.message, 'not_write') then
                -- Not logged.
                return nil
            end
            return entry.type, entry.code
        end
    end
    limiter = vratelimit.new({custom_get_entry_signature = custom})
    err = verror.make('do_write')
    t.assert(limiter:can_log(err))
    t.assert(limiter.map[custom_type][1])
    err = verror.make('not_write')
    t.assert_not(limiter:can_log(err))
    t.assert_equals(limiter.heap:count(), 1)
end

test_group.test_log = function(g)
    g.server:exec(function()
        local function custom(entry)
            if string.find(entry.message, 'no_write') then
                return false
            end
            return entry.type, entry.code
        end
        local consts = require('vshard.consts')
        local old_interval = consts.LOG_RATELIMIT_INTERVAL
        consts.LOG_RATELIMIT_INTERVAL = 0.01
        local limiter = _G.ivratelimit.new{custom_get_entry_signature = custom}
        -- Test, that forbidden entry is not added to the heap and map.
        local err = iverror.make('no_write')
        ilt.assert_not(limiter:can_log(err))
        ilt.assert_equals(limiter.heap:count(), 0)
        ilt.assert_not(limiter.map[err.type])

        -- Test, that the same entry is not printed several times.
        err = iverror.make('Some error')
        ilt.assert(limiter:can_log(err))
        ilt.assert_not(limiter:can_log(err))
        local entry = limiter.map[err.type][err.code]
        ilt.assert(entry)
        ilt.assert_equals(entry.suppressed, 1)
        ilt.assert_equals(limiter.heap:count(), 1)

        -- Test, that suppressed message is printed to logs.
        require('fiber').sleep(consts.LOG_RATELIMIT_INTERVAL)
        limiter:flush()
        consts.LOG_RATELIMIT_INTERVAL = old_interval
    end)
    t.assert(g.server:grep_log("Suppressed 1 .* messages"))
end

--
-- Default log ratelimiter is used for services, it forbids to log some
-- type of errors (e.g. VHANDSHAKE_NOT_COMPLETE).
--
test_group.test_service_limiter = function()
    -- On 2.11 box.error fails with "error: invalid option '%.*' to 'format'".
    t.run_only_if(vutil.version_is_at_least(3, 0, 0, nil, 0, 0))
    local limiter = vutil.new_ratelimit_for_service()
    local forbidden_errors = {
        ['VHANDSHAKE_NOT_COMPLETE'] =
            verror.vshard(verror.code.VHANDSHAKE_NOT_COMPLETE, 'replica'),
        ['STORAGE_IS_DISABLED'] =
            verror.vshard(verror.code.STORAGE_IS_DISABLED, 'reason'),
        ['OBJECT_IS_OUTDATED'] = verror.vshard(verror.code.OBJECT_IS_OUTDATED),
        ['ACCESS_DENIED'] =
            box.error.new({code = box.error.ACCESS_DENIED,
                           type = 'AccessDeniedError',
                           message = "Execute access to function 'vshard.c"}),
        ['NO_SUCH_PROC'] = box.error.new(box.error.NO_SUCH_PROC, 'vshard.call'),
    }
    for name, err in pairs(forbidden_errors) do
        t.assert_not(limiter:can_log(err), name)
        t.assert_not(limiter.map[err.type])
    end
    t.assert_equals(limiter.heap:count(), 0)
end
