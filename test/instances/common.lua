--
-- Functions which can be useful both for the router and the storage.
--

local t = require('luatest')
local fiber_clock = require('fiber').clock

local wait_timeout = require('test.luatest_helpers.vtest').wait_timeout

-- Helper function, which waits for the message to appear in the log_fiber.
--
-- Doesn't check all known logs by default. Just the last known message.
-- If 'is_all_scan' is true, then the function will check all logs before
-- waiting for the new ones.
local function wait_for_fiber_msg(logf, fiber_name, expected, is_all_scan)
    local passed = 0
    if is_all_scan then
        local start = fiber_clock()
        local info = logf.logs[fiber_name]
        if not info then
            -- There's no info about the requested fiber.
            -- Let's wait for messages
            goto continue
        end

        for _, msg in ipairs(info.messages.data) do
            local ok = pcall(t.assert_str_contains, msg.data, expected)
            if ok then
                return
            end
        end

        ::continue::
        passed = fiber_clock() - start
    end

    local last_seen_msg_time
    t.helpers.retrying({timeout = wait_timeout - passed}, function()
        local info = logf.logs[fiber_name]
        local msg = info.messages.data[info.messages:count()]
        -- Do not compare strings if it isn't really needed
        if msg.time ~= last_seen_msg_time then
            last_seen_msg_time = msg.time
            t.assert_str_contains(msg.data, expected)
        else
            error('Already checked that message')
        end
    end)
end

local function wait_for_fiber_state(logf, fiber_name, expected)
    t.helpers.retrying({timeout = wait_timeout}, function()
    end)
end

return {
  wait_for_fiber_msg = wait_for_fiber_msg,
}
