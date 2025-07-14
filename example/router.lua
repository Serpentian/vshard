#!/usr/bin/env tarantool

require('strict').on()
fiber = require('fiber')

local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')

-- Check if we are running under test-run
if os.getenv('ADMIN') then
    test_run = require('test_run').new()
    require('console').listen(os.getenv('ADMIN'))
end

replicasets = {'cbf06940-0790-498b-948d-042b62cf3d29',
               'ac522f65-aa94-4134-9f64-51ee384f1a54'}

local listen = {
    ['router_1'] = 3305,
    ['router_2'] = 3306,
    ['router_3'] = 3307,
    ['router_4'] = 3308,
    ['router_5'] = 3309,
    ['router_6'] = 3310,
    ['router_7'] = 3311,
    ['router_8'] = 3312,
    ['router_9'] = 3313,
    ['router_10'] = 3314,
    ['router_11'] = 3315,
    ['router_12'] = 3316,
}

-- Call a configuration provider
cfg = dofile('localcfg.lua')
if arg[1] == 'discovery_disable' then
    cfg.discovery_mode = 'off'
end

if not os.getenv('ADMIN') then
    cfg.listen = listen[NAME]
end
-- Start the database with sharding
vshard = require('vshard')
vshard.router.cfg(cfg)
if not os.getenv('ADMIN') then
    -- Allow load generator to execute arbitrary functions.
    box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
end

vshard_callro = vshard.router.callro

function crud_call(bid, func_name, args)
    local rs = vshard.router.route(bid)
    return rs:callro(func_name, args)
end

function crud_call_storage(bid, func_name, args)
    local rs = vshard.router.route(bid)
    local ok, res, err = rs:callro('vshard.storage.call', {bid, 'read', func_name, args})
    return res, err
end
