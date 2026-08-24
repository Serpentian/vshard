--
-- The 2x2 matrix of the storage call implementations:
--
--   wrapper:       vshard.storage.call (Lua) / vshard.storage_c.call (C)
--   user function: registered C stored function / plain Lua function
--
-- The wrapper is selected by the function name of the direct
-- net.box call, so both are tested in one cluster. The C user
-- functions are the example module test/perf/c_call/bench_c.so;
-- both it and vshard/storage_c.so must be built, otherwise the
-- whole group is skipped.
--
local fio = require('fio')
local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local test_group = t.group('storage_c_call')

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 20,
}
local global_cfg

local source_dir = fio.abspath(os.getenv('PACKPACK_GIT_SOURCEDIR') or
                               os.getenv('SOURCEDIR') or
                               debug.getinfo(1).source:match('@?(.*/)')..
                               '../..')

local WRAPPERS = {
    lua = 'vshard.storage.call',
    c = 'vshard.storage_c.call',
}

local FUNCS = {
    lua = {replace = 'user_lua_replace', get = 'user_lua_get'},
    c = {replace = 'bench_c.replace', get = 'bench_c.get'},
}

local saved_env = {}

local function env_set(name, value)
    saved_env[name] = os.getenv(name)
    os.setenv(name, value)
end

local function env_restore()
    for name, value in pairs(saved_env) do
        os.setenv(name, value or '')
    end
end

test_group.before_all(function(g)
    -- The .so files must be reachable by the storage instances:
    -- vshard/storage_c.so via the repo root, bench_c.so via the
    -- example dir. VSHARD_C_CALL enables the C implementation.
    env_set('VSHARD_C_CALL', '1')
    env_set('LUA_CPATH', source_dir..'/?.so;'..
            source_dir..'/test/perf/c_call/?.so;;')
    global_cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(g, global_cfg)
    vtest.cluster_bootstrap(g, global_cfg)
    g.is_c_available = g.replica_1_a:exec(function()
        return ivshard.storage.internal.is_storage_c_enabled
    end) and fio.path.exists(source_dir..'/test/perf/c_call/bench_c.so')
    if not g.is_c_available then
        return
    end
    g.sid = g.replica_1_a:exec(function()
        local s = box.schema.space.create('test_c_call')
        s:format({{'id', 'unsigned'}, {'bucket_id', 'unsigned'},
                  {'payload', 'string'}})
        s:create_index('pk')
        for _, name in ipairs({'bench_c.replace', 'bench_c.get'}) do
            box.schema.func.create(name, {language = 'C',
                                          if_not_exists = true})
        end
        rawset(_G, 'user_lua_replace', function(sid, tuple)
            box.space[sid]:replace(tuple)
        end)
        rawset(_G, 'user_lua_get', function(sid, key)
            return box.space[sid]:get(key)
        end)
        rawset(_G, 'user_lua_echo', function(...)
            return ...
        end)
        rawset(_G, 'user_lua_err', function()
            error('user_lua_err failed')
        end)
        return box.space.test_c_call.id
    end)
    g.bid = vtest.storage_first_bucket(g.replica_1_a)
end)

test_group.after_all(function(g)
    g.cluster:drop()
    env_restore()
end)

test_group.before_each(function(g)
    t.skip_if(not g.is_c_available,
              'C storage.call is not available - build '..
              'vshard/storage_c/build.sh and test/perf/c_call/build.sh')
end)

--
-- One matrix cell: write a tuple, read it back, check the reply
-- convention, check the user function error and the WRONG_BUCKET
-- error of the given wrapper.
--
local function check_cell(g, wrapper, funcs, id)
    local s = g.replica_1_a
    local payload = 'payload_'..id
    --
    -- Write. The functions return nothing - the reply must be
    -- the lone status with no trailing nils.
    --
    local r = {s:call(wrapper,
                      {g.bid, 'write', funcs.replace,
                       {g.sid, {id, g.bid, payload}}})}
    t.assert_equals(r, {true}, wrapper..' + '..funcs.replace)
    --
    -- Read back.
    --
    r = {s:call(wrapper, {g.bid, 'read', funcs.get, {g.sid, {id}}})}
    t.assert_equals(r[1], true)
    t.assert_equals(r[2], {id, g.bid, payload})
    t.assert_equals(r[3], nil)
    --
    -- Read of a missing key: (true) - nothing is returned, the
    -- nil result must be truncated, not turned into box.NULL.
    --
    r = {s:call(wrapper, {g.bid, 'read', funcs.get, {g.sid, {id + 500}}})}
    t.assert_equals(r, {true})
    --
    -- WRONG_BUCKET: (nil, err).
    --
    r = {s:call(wrapper, {1000000, 'read', funcs.get, {g.sid, {id}}})}
    t.assert_equals(r[1], nil)
    t.assert_equals(r[2].type, 'ShardingError')
    t.assert_equals(r[2].name, 'WRONG_BUCKET')
    t.assert_str_contains(r[2].message, 'Not found')
    --
    -- An error raised by the user function: (false, err).
    --
    if funcs.replace == FUNCS.lua.replace then
        r = {s:call(wrapper, {g.bid, 'read', 'user_lua_err', {}})}
        t.assert_equals(r[1], false)
        t.assert_str_contains(r[2].message, 'user_lua_err failed')
    else
        -- The C function fails on an unknown space.
        r = {s:call(wrapper,
                    {g.bid, 'read', funcs.get, {'no_such_space', {id}}})}
        t.assert_equals(r[1], false)
        t.assert_str_contains(r[2].message, 'no_such_space')
    end
end

test_group.test_lua_wrapper_lua_func = function(g)
    check_cell(g, WRAPPERS.lua, FUNCS.lua, 1)
end

test_group.test_lua_wrapper_c_func = function(g)
    check_cell(g, WRAPPERS.lua, FUNCS.c, 2)
end

test_group.test_c_wrapper_lua_func = function(g)
    check_cell(g, WRAPPERS.c, FUNCS.lua, 3)
end

test_group.test_c_wrapper_c_func = function(g)
    check_cell(g, WRAPPERS.c, FUNCS.c, 4)
end

--
-- The two wrappers must return byte-identical replies for the
-- same request - success, multireturn and errors alike.
--
test_group.test_wrapper_parity = function(g)
    local s = g.replica_1_a
    local requests = {
        -- Multireturn pass-through.
        {g.bid, 'read', 'user_lua_echo', {1, 'x', false}},
        -- No return values.
        {g.bid, 'write', 'user_lua_replace', {g.sid, {10, g.bid, 'p'}}},
        -- C function result.
        {g.bid, 'read', 'bench_c.get', {g.sid, {10}}},
        -- WRONG_BUCKET.
        {1000000, 'read', 'user_lua_echo', {}},
        -- Unknown function.
        {g.bid, 'read', 'no_such_function', {}},
        -- Lua error. The 'line'/'trace' fields of the unpacked
        -- error may differ by raise site, so compare the stable
        -- fields only.
        {g.bid, 'read', 'user_lua_err', {}},
    }
    local function project(reply)
        local err = reply[2]
        if type(err) == 'table' then
            reply[2] = {
                type = err.type,
                code = err.code,
                name = err.name,
                message = err.message,
            }
        end
        return reply
    end
    for i, req in ipairs(requests) do
        local rl = project({s:call(WRAPPERS.lua, req)})
        local rc = project({s:call(WRAPPERS.c, req)})
        t.assert_equals(rc, rl, 'request #'..i)
    end
end

--
-- Both wrappers work on one shared ref state: a bucket sending
-- protection engaged through one wrapper is visible through the
-- other.
--
test_group.test_shared_refs = function(g)
    local s = g.replica_1_a
    local r = {s:call(WRAPPERS.c, {g.bid, 'read', 'user_lua_echo', {1}})}
    t.assert_equals(r, {true, 1})
    s:exec(function(bid)
        -- After any call through any wrapper the ref object must
        -- exist in the shared C storage with zero counters.
        local ref = ivshard.storage.internal.bucket_refs[bid]
        ilt.assert_not_equals(ref, nil)
        ilt.assert_equals(ref.ro, 0)
        ilt.assert_equals(ref.rw, 0)
        -- A Lua-side ref is visible to the C wrapper state and
        -- vice versa - the counters live in one place.
        ilt.assert(ivshard.storage.bucket_refro(bid))
        ilt.assert_equals(ref.ro, 1)
        ilt.assert(ivshard.storage.bucket_unrefro(bid))
        ilt.assert_equals(ref.ro, 0)
    end, {g.bid})
end
