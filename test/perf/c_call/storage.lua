-- Storage instance for the C storage.call benchmark. Launched by
-- run.sh with VSHARD_C_CALL=1 and LUA_PATH/LUA_CPATH pointing at
-- the vshard repo.
local fio = require('fio')

local RS_UUID = 'cbf06940-0790-498b-948d-042b62cf3d29'
local INST_UUID = '8a274925-a26d-47fc-9e1b-af88ce939412'
local BUCKET_COUNT = 3000

local cfg = {
    bucket_count = BUCKET_COUNT,
    sharding = {
        [RS_UUID] = {
            replicas = {
                [INST_UUID] = {
                    uri = 'storage:storage@127.0.0.1:3311',
                    name = 'storage_1_a',
                    master = true,
                },
            },
        },
    },
}

local vshard = require('vshard')
rawset(_G, 'vshard', vshard)
vshard.storage.cfg(cfg, INST_UUID)
vshard.storage.bucket_force_create(1, BUCKET_COUNT)

box.schema.space.create('bench', {id = 777, if_not_exists = true})
box.space.bench:format({
    {'id', 'unsigned'}, {'bucket_id', 'unsigned'}, {'payload', 'string'}})
box.space.bench:create_index('pk', {if_not_exists = true})

-- The user's own C functions from bench_c.so (see user_funcs.c),
-- registered like any C stored procedure. vshard knows nothing
-- about them - the C storage.call resolves them through the
-- _func registry.
for _, name in ipairs({'bench_c.replace', 'bench_c.get'}) do
    box.schema.func.create(name, {language = 'C', if_not_exists = true})
    box.schema.user.grant('storage', 'execute', 'function', name,
                          {if_not_exists = true})
end

-- Lua twins of the C bench functions, for the fallback-path
-- comparison.
rawset(_G, 'bench_replace_lua', function(tuple)
    box.space.bench:replace(tuple)
end)
rawset(_G, 'bench_get_lua', function(id)
    return box.space.bench:get(id)
end)
rawset(_G, 'echo_lua', function(...)
    return ...
end)

local f = fio.open('storage.pid', {'O_CREAT', 'O_WRONLY', 'O_TRUNC'},
                   tonumber('644', 8))
f:write(tostring(box.info.pid))
f:close()
print('storage is ready')
