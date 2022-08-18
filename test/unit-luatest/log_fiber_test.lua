local t = require('luatest')
local log = require('log')
local fiber = require('fiber')
local vlogf = require('vshard.log_fiber')
local g = t.group('log_fiber')

local CURRENT_FIBER_NAME = 'luatest'
local DEFAULT_FIBER_NAME = 'lua'

g.before_all(function(g)
    -- In 1.10 the log module doesn't have cfg in it. So the only way to
    -- get log_level for log_fiber is box.cfg
    box.cfg{log_level = 7}
    g.logf = vlogf.new({
        log_vshard_background = true,
    })
end)

g.after_each(function(g)
    -- Cleaning the logger
    g.logf:drop()
end)

g.test_basic = function(g)
    t.assert_equals(g.logf:get_info(), {})
    g.logf:info('info')

    -- Saving the message for the 'luatest' fiber
    local info = g.logf:get_info()[CURRENT_FIBER_NAME]
    t.assert_equals(info.state, 'unknown')
    t.assert_equals(#info.last_messages, 1)
    t.assert_str_contains(info.last_messages[1], 'info')

    -- Setting the state of the fiber
    g.logf:set_state('some state')
    info = g.logf:get_info()[CURRENT_FIBER_NAME]
    t.assert_equals(info.state, 'some state')
end

g.test_log_level = function(g)
    g.logf:info('info')
    g.logf:warn('warn')
    g.logf:debug('debug')
    g.logf:error('error')
    g.logf:verbose('verbose')

    local info = g.logf:get_info()[CURRENT_FIBER_NAME]
    t.assert_equals(#info.last_messages, 5)
    -- The messages are sorted in the ascending order by the time of logging
    t.assert_str_contains(info.last_messages[1], 'V> verbose')
    t.assert_str_contains(info.last_messages[2], 'E> error')
    t.assert_str_contains(info.last_messages[3], 'D> debug')
    t.assert_str_contains(info.last_messages[4], 'W> warn')
    t.assert_str_contains(info.last_messages[5], 'I> info')
end

g.test_drop = function(g)
    g.logf:info('luatest info')
    fiber.create(function() g.logf:info('lua info') end)

    local info = g.logf:get_info()
    t.assert_not_equals(info[CURRENT_FIBER_NAME], nil)
    t.assert_not_equals(info[DEFAULT_FIBER_NAME], nil)

    g.logf:drop(DEFAULT_FIBER_NAME)

    info = g.logf:get_info()
    t.assert_not_equals(info[CURRENT_FIBER_NAME], nil)
    t.assert_equals(info[DEFAULT_FIBER_NAME], nil)
end

g.test_log_length = function(g)
    local old_log_length = g.logf.log_length
    g.logf.log_length = 1

    g.logf:info('first')
    g.logf:info('second')
    local info = g.logf:get_info()[CURRENT_FIBER_NAME]
    t.assert_equals(#info.last_messages, 1)
    t.assert_str_contains(info.last_messages[1], 'second')

    g.logf.log_length = old_log_length
end

g.test_log_level_limit = function(g)
    local old_log_lvl = box.cfg.log_level
    box.cfg{log_level = 5}

    g.logf:info('info')
    g.logf:debug('verbose')
    local info = g.logf:get_info()[CURRENT_FIBER_NAME]
    t.assert_equals(#info.last_messages, 1)

    box.cfg{log_level = old_log_lvl}
end

g.test_log_format = function(g)
    g.logf:info(true)
    g.logf:info({key = 'value'})

    local info = g.logf:get_info()[CURRENT_FIBER_NAME]
    t.assert_equals(#info.last_messages, 2)
    t.assert_str_contains(info.last_messages[1], '{"key":"value"}')
    t.assert_str_contains(info.last_messages[2], 'true')
end
