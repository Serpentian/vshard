--
-- Lua side of the handshake with the vshard.storage_c C module
-- (prototype). The C module owns the bucket ref counters; Lua
-- reaches them through a vtable pointer returned by the
-- 'vshard.storage_c.setup' stored C function.
--
-- The .so can not be ffi.load()ed: tarantool's module cache
-- copies the file to a temp location before dlopen, so an
-- ffi.load would create a second, unrelated instance. All the
-- sharing goes through the pointer below.
--
local ffi = require('ffi')

pcall(ffi.cdef, [[
struct vsc_ref {
    uint32_t ro;
    uint32_t rw;
    bool rw_lock;
    bool ro_lock;
    bool exists;
};

struct vshard_storage_c_api {
    struct vsc_ref *(*ref_get)(uint32_t bucket_id);
    struct vsc_ref *(*ref_new)(uint32_t bucket_id);
    void (*ref_del)(uint32_t bucket_id);
    void (*refs_clear)(void);
    void (*set_identity)(const char *replica_id, const char *replicaset_id,
                         const char *master_id, bool is_master);
};
]])

local M = {
    -- Casted 'struct vshard_storage_c_api *'. Not nil only after
    -- a successful setup().
    api = nil,
}

--
-- Call the C setup function and remember the api vtable. The
-- callbacks are passed through _G: the C function picks them up
-- and clears the globals.
--
-- @param opts bucket_count, replica_id, replicaset_id,
--        master_id, is_master, on_event, helpers.
--
function M.setup(opts)
    local func = box.func and box.func['vshard.storage_c.setup']
    if func == nil then
        return nil, 'function vshard.storage_c.setup is not registered'
    end
    rawset(_G, '__vshard_storage_c_on_event', opts.on_event)
    rawset(_G, '__vshard_storage_c_helpers', opts.helpers)
    local ok, ptr = pcall(func.call, func, {
        opts.bucket_count, opts.replica_id, opts.replicaset_id,
        opts.master_id, opts.is_master})
    rawset(_G, '__vshard_storage_c_on_event', nil)
    rawset(_G, '__vshard_storage_c_helpers', nil)
    if not ok then
        return nil, ptr
    end
    M.api = ffi.cast('struct vshard_storage_c_api *', ptr)
    return true
end

function M.ref_get(bucket_id)
    local ref = M.api.ref_get(bucket_id)
    if ref == nil then
        return nil
    end
    return ref
end

function M.ref_new(bucket_id)
    local ref = M.api.ref_new(bucket_id)
    if ref == nil then
        return nil
    end
    return ref
end

function M.ref_del(bucket_id)
    M.api.ref_del(bucket_id)
end

function M.refs_clear()
    M.api.refs_clear()
end

function M.set_identity(replica_id, replicaset_id, master_id, is_master)
    M.api.set_identity(replica_id, replicaset_id, master_id, is_master)
end

return M
