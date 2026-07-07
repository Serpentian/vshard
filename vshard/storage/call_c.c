/*
 * C fast path for vshard.storage.call.
 *
 * The module is loaded as a regular Lua-C library
 * (require('vshard.storage.call_c')) and its call() function is registered
 * in the core's function registry under the name 'vshard.storage.call' via
 * box.iproto.export(). The core consults the registry in box_lua_find()
 * before walking _G, so every IPROTO_CALL of 'vshard.storage.call' lands
 * here while the _func entry, its grants and setuid stay intact.
 *
 * The function receives already-decoded Lua arguments and returns Lua
 * multivalues - the core encodes the reply, so the wire format can't
 * diverge from the Lua implementation.
 *
 * The hot path covers exactly the branches storage_call() would pass
 * through without touching anything cold: an existing non-locked bucket
 * ref, a plain '.'-path global Lua function, scalar bookkeeping. Every
 * other condition tail-calls the public Lua vshard.storage.call closure
 * with the original arguments, so the fallback behavior is the Lua
 * behavior by construction. The cold post-call work (user error wrapping,
 * slow unref) can't be restarted and goes through dedicated Lua helpers
 * instead - the same code the Lua path runs.
 *
 * Semantics contract: storage_call() in vshard/storage/init.lua and
 * netbox self:call() in tarantool src/box/lua/net_box.lua.
 */
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#include <module.h>
#include <lauxlib.h>

/* Must match the ffi.cdef in vshard/storage/init.lua. */
struct bucket_ref {
	uint32_t ro;
	uint32_t rw;
	bool rw_lock;
	bool ro_lock;
};

struct vshard_call_gate {
	bool is_fast;
};

enum fallback_reason {
	/* Storage is not loaded/configured/enabled yet - the gate is shut. */
	FALLBACK_NOT_READY = 0,
	/* The (bucket_id, mode, name, args) envelope is not the common one. */
	FALLBACK_BAD_ARGS,
	/* box.ctl.is_recovery_finished() hasn't turned true yet. */
	FALLBACK_RECOVERY,
	/* Function is in box.func, has an exotic name or is not a plain
	 * global Lua function. */
	FALLBACK_FUNC_RESOLVE,
	/* No ref entry for the bucket, a lock is set or a write is attempted
	 * not on the master - all the _bucket-checking branches. */
	FALLBACK_BUCKET_STATE,
	FALLBACK_REASON_MAX,
};

static const char *fallback_reason_strs[] = {
	"not_ready",
	"bad_args",
	"recovery",
	"func_resolve",
	"bucket_state",
};

struct vshard_call_ctx {
	bool is_set_up;
	/*
	 * box.ctl.is_recovery_finished() is monotonic within a process, so
	 * once it is seen true, it is never called again.
	 */
	bool is_recovery_finished;
	/* Registry ref of __module_vshard_storage (stable across reloads). */
	int m_ref;
	/* Registry ref pinning the gate cdata against GC. */
	int gate_ref;
	struct vshard_call_gate *gate;
	/* box.ctl.is_recovery_finished */
	int recovery_check_ref;
	/* The public Lua vshard.storage.call closure. */
	int fallback_ref;
	/* Lua helper handling a failed user function: rollback, PROC_LUA
	 * wrap, lerror.make, unref, err.prev chaining. */
	int finish_error_ref;
	/* The Lua bucket_unref() for the cold unref branches. */
	int finish_unref_ref;
	/* Lua `function(v) return v == nil end` - see value_is_nil(). */
	int is_nil_ref;
	uint32_t ctid_bucket_ref;
	uint32_t ctid_gate;
	/* Introspection for tests and monitoring. */
	uint64_t fast_count;
	uint64_t finish_error_count;
	uint64_t slow_unref_count;
	uint64_t fallback_count[FALLBACK_REASON_MAX];
};

static struct vshard_call_ctx ctx = {
	.is_set_up = false,
	.m_ref = LUA_NOREF,
	.gate_ref = LUA_NOREF,
	.recovery_check_ref = LUA_NOREF,
	.fallback_ref = LUA_NOREF,
	.finish_error_ref = LUA_NOREF,
	.finish_unref_ref = LUA_NOREF,
	.is_nil_ref = LUA_NOREF,
};

/*
 * Tail-call the public Lua vshard.storage.call with the original arguments.
 * Raised errors are intentionally not caught - the pure-Lua path raises
 * them from the same protected context (execute_lua_call).
 */
static int
fallback(struct lua_State *L, int top, enum fallback_reason reason)
{
	ctx.fallback_count[reason]++;
	lua_settop(L, top);
	lua_rawgeti(L, LUA_REGISTRYINDEX, ctx.fallback_ref);
	lua_insert(L, 1);
	lua_call(L, top, LUA_MULTRET);
	return lua_gettop(L);
}

/*
 * `value == nil` with Lua semantics: box.NULL (and any NULL pointer cdata)
 * is equal to nil in LuaJIT, which is exactly what the trailing-nil
 * truncation in storage_call() relies upon. lua_equal() can't be used: it
 * bails out on differing value types without consulting the cdata
 * equality, so the check is delegated to a Lua helper. Only cdata values
 * pay for that call - non-cdata non-nil values can never be equal to nil
 * (cross-type __eq does not fire).
 */
static bool
value_is_nil(struct lua_State *L, int idx)
{
	if (lua_isnil(L, idx))
		return true;
	if (!luaL_iscdata(L, idx))
		return false;
	lua_rawgeti(L, LUA_REGISTRYINDEX, ctx.is_nil_ref);
	lua_pushvalue(L, idx);
	lua_call(L, 1, 1);
	bool res = lua_toboolean(L, -1);
	lua_pop(L, 1);
	return res;
}

/*
 * Fetch M.bucket_refs[bucket_id] and return the pointer to its payload, or
 * NULL when there is no entry (or it is not the expected cdata). The refs
 * table and its entries are dynamic (GC deletes entries, _bucket truncate
 * replaces the whole table), so the lookup must be repeated on every use -
 * pointers must not be cached across the user function call.
 *
 * Leaves the stack unchanged.
 */
static struct bucket_ref *
bucket_ref_find(struct lua_State *L)
{
	struct bucket_ref *ref = NULL;
	lua_rawgeti(L, LUA_REGISTRYINDEX, ctx.m_ref);
	lua_getfield(L, -1, "bucket_refs");
	if (lua_istable(L, -1)) {
		lua_pushvalue(L, 1);
		lua_rawget(L, -2);
		if (luaL_iscdata(L, -1)) {
			uint32_t ctid;
			void *cd = luaL_checkcdata(L, -1, &ctid);
			if (ctid == ctx.ctid_bucket_ref)
				ref = cd;
		}
		lua_pop(L, 1);
	}
	lua_pop(L, 2);
	return ref;
}

static bool
recovery_is_finished(struct lua_State *L)
{
	if (ctx.is_recovery_finished)
		return true;
	lua_rawgeti(L, LUA_REGISTRYINDEX, ctx.recovery_check_ref);
	if (lua_pcall(L, 0, 1, 0) != 0) {
		lua_pop(L, 1);
		return false;
	}
	bool res = lua_toboolean(L, -1);
	lua_pop(L, 1);
	if (res)
		ctx.is_recovery_finished = true;
	return res;
}

/*
 * Resolve the '.'-separated path of a global Lua function, mirroring the
 * plain-path subset of the core's box_lua_find(). On success pushes the
 * function and returns true. On any deviation (method syntax, indexing,
 * non-function target) restores the stack and returns false - the Lua
 * path handles those.
 */
static bool
func_resolve(struct lua_State *L, const char *name, size_t name_len)
{
	int top = lua_gettop(L);
	const char *name_end = name + name_len;
	if (memchr(name, ':', name_len) != NULL ||
	    memchr(name, '[', name_len) != NULL)
		return false;
	const char *start = name;
	const char *end = memchr(start, '.', name_end - start);
	if (end == NULL)
		end = name_end;
	lua_pushlstring(L, start, end - start);
	lua_gettable(L, LUA_GLOBALSINDEX);
	while (end != name_end) {
		if (!lua_istable(L, -1) && !lua_islightuserdata(L, -1) &&
		    !lua_isuserdata(L, -1))
			goto not_found;
		start = end + 1;
		end = memchr(start, '.', name_end - start);
		if (end == NULL)
			end = name_end;
		lua_pushlstring(L, start, end - start);
		lua_gettable(L, -2);
		lua_remove(L, -2);
	}
	if (!lua_isfunction(L, -1))
		goto not_found;
	return true;
not_found:
	lua_settop(L, top);
	return false;
}

/*
 * The fast vshard.storage.call(bucket_id, mode, name, args).
 */
static int
vshard_storage_call(struct lua_State *L)
{
	int top = lua_gettop(L);
	if (!ctx.is_set_up)
		return luaL_error(L, "vshard.storage.call_c is not set up");
	if (!ctx.gate->is_fast)
		return fallback(L, top, FALLBACK_NOT_READY);
	/*
	 * The envelope. bucket_id must be a real number: it is used as a
	 * table key, so a numeric string or a cdata number would address a
	 * different key than in the Lua implementation.
	 */
	if (top < 3 || lua_type(L, 1) != LUA_TNUMBER ||
	    lua_type(L, 2) != LUA_TSTRING || lua_type(L, 3) != LUA_TSTRING)
		return fallback(L, top, FALLBACK_BAD_ARGS);
	size_t mode_len;
	const char *mode = lua_tolstring(L, 2, &mode_len);
	bool is_read;
	if (mode_len == 4 && memcmp(mode, "read", 4) == 0)
		is_read = true;
	else if (mode_len == 5 && memcmp(mode, "write", 5) == 0)
		is_read = false;
	else
		return fallback(L, top, FALLBACK_BAD_ARGS);
	int nargs = 0;
	if (top >= 4 && !lua_isnil(L, 4)) {
		if (lua_type(L, 4) != LUA_TTABLE)
			return fallback(L, top, FALLBACK_BAD_ARGS);
		/* unpack() semantics: raw length, raw element access. */
		nargs = (int)lua_objlen(L, 4);
	}
	if (!lua_checkstack(L, nargs + LUA_MINSTACK))
		return fallback(L, top, FALLBACK_BAD_ARGS);
	/*
	 * netbox self:call consults box.func only after recovery is
	 * finished; before that the whole resolution differs - not fast
	 * path material.
	 */
	if (!recovery_is_finished(L))
		return fallback(L, top, FALLBACK_RECOVERY);
	size_t name_len;
	const char *name = lua_tolstring(L, 3, &name_len);
	/*
	 * A function present in box.func (persistent, stored C, SQL) is
	 * invoked through f:call() machinery - let the Lua path do it.
	 * box.func is a plain table, the lookup is cheap.
	 */
	lua_getfield(L, LUA_GLOBALSINDEX, "box");
	lua_getfield(L, -1, "func");
	bool in_box_func = false;
	if (lua_istable(L, -1)) {
		lua_pushvalue(L, 3);
		lua_rawget(L, -2);
		in_box_func = !lua_isnil(L, -1);
		lua_pop(L, 1);
	}
	lua_pop(L, 2);
	if (in_box_func)
		return fallback(L, top, FALLBACK_FUNC_RESOLVE);
	/*
	 * Resolution is done before taking the bucket ref: it is pure, and
	 * on any failure the whole call is re-run in Lua, whose ref/unref
	 * is net-zero, so the reordering is unobservable.
	 */
	if (!func_resolve(L, name, name_len))
		return fallback(L, top, FALLBACK_FUNC_RESOLVE);
	/* Stack: args..., func. */

	/*
	 * bucket_ref() fast branches, exactly as in storage_call():
	 * an existing entry, no locks, and the instance is a master for
	 * a write. Everything else needs _bucket checks, error objects or
	 * GC signalling - the Lua path covers it.
	 */
	struct bucket_ref *ref = bucket_ref_find(L);
	if (ref == NULL)
		return fallback(L, top, FALLBACK_BUCKET_STATE);
	if (is_read) {
		if (ref->ro_lock)
			return fallback(L, top, FALLBACK_BUCKET_STATE);
	} else {
		if (ref->rw_lock)
			return fallback(L, top, FALLBACK_BUCKET_STATE);
		lua_rawgeti(L, LUA_REGISTRYINDEX, ctx.m_ref);
		lua_getfield(L, -1, "is_master");
		bool is_master = lua_toboolean(L, -1);
		lua_pop(L, 2);
		if (!is_master)
			return fallback(L, top, FALLBACK_BUCKET_STATE);
	}
	if (is_read)
		ref->ro++;
	else
		ref->rw++;
	ctx.fast_count++;

	for (int i = 1; i <= nargs; i++)
		lua_rawgeti(L, 4, i);
	if (lua_pcall(L, nargs, LUA_MULTRET, 0) != 0) {
		/*
		 * The user function failed. Everything left - rollback,
		 * PROC_LUA wrapping, lerror.make, unref and the err.prev
		 * chain on a half-deleted bucket - is cold and lives in
		 * the Lua helper. It returns the final reply pair.
		 */
		ctx.finish_error_count++;
		lua_rawgeti(L, LUA_REGISTRYINDEX, ctx.finish_error_ref);
		lua_pushvalue(L, 1);
		lua_pushvalue(L, 2);
		lua_pushvalue(L, -4);
		lua_call(L, 3, 2);
		return 2;
	}
	int nres = lua_gettop(L) - top;

	/*
	 * bucket_unref() fast branches: a plain decrement with no lock
	 * flags involved. The entry is re-fetched: the user function could
	 * have yielded, and the refs table or the entry could be gone.
	 */
	ref = bucket_ref_find(L);
	bool unref_is_done = false;
	if (ref != NULL) {
		if (is_read) {
			if (ref->ro > 1) {
				ref->ro--;
				unref_is_done = true;
			} else if (ref->ro == 1 && !ref->ro_lock) {
				ref->ro = 0;
				unref_is_done = true;
			}
		} else {
			if (ref->rw > 1) {
				ref->rw--;
				unref_is_done = true;
			} else if (ref->rw == 1 && !ref->rw_lock) {
				ref->rw = 0;
				unref_is_done = true;
			}
		}
	}
	if (!unref_is_done) {
		ctx.slow_unref_count++;
		lua_rawgeti(L, LUA_REGISTRYINDEX, ctx.finish_unref_ref);
		lua_pushvalue(L, 1);
		lua_pushvalue(L, 2);
		lua_call(L, 2, 2);
		if (!lua_toboolean(L, -2)) {
			/*
			 * The bucket is half-deleted. The user results are
			 * discarded, the reply is nil, err - and that is
			 * exactly the two values on top of the stack.
			 */
			return 2;
		}
		lua_pop(L, 2);
	}

	/*
	 * The reply framing: true, ret1, ret2, ret3 with the returns capped
	 * at 3 and the trailing nils (including box.NULL) truncated.
	 */
	if (nres > 3) {
		lua_settop(L, top + 3);
		nres = 3;
	}
	while (nres > 0 && value_is_nil(L, top + nres))
		nres--;
	lua_settop(L, top + nres);
	lua_pushboolean(L, true);
	lua_insert(L, top + 1);
	return nres + 1;
}

static void
ctx_field_ref(struct lua_State *L, int idx, const char *field, int type,
	      int *ref)
{
	lua_getfield(L, idx, field);
	if (lua_type(L, -1) != type)
		luaL_error(L, "setup: '%s' must be a %s", field,
			   lua_typename(L, type));
	luaL_unref(L, LUA_REGISTRYINDEX, *ref);
	*ref = luaL_ref(L, LUA_REGISTRYINDEX);
}

/*
 * setup{m, gate, call_fallback, finish_error, finish_unref,
 *       recovery_check} - (re)binds the module to the current storage
 * internals. Called on every storage_cfg(), including after a hot reload:
 * M is the same table object, but the closures are new.
 */
static int
vshard_storage_call_setup(struct lua_State *L)
{
	if (lua_gettop(L) != 1 || !lua_istable(L, 1))
		return luaL_error(L, "Usage: call_c.setup(table)");
	ctx.ctid_bucket_ref = luaL_ctypeid(L, "struct bucket_ref");
	ctx.ctid_gate = luaL_ctypeid(L, "struct vshard_call_gate");

	lua_getfield(L, 1, "gate");
	uint32_t ctid = 0;
	if (!luaL_iscdata(L, -1) ||
	    (luaL_checkcdata(L, -1, &ctid), ctid != ctx.ctid_gate))
		return luaL_error(L, "setup: 'gate' must be a struct "
				  "vshard_call_gate cdata");
	ctx.gate = luaL_checkcdata(L, -1, &ctid);
	luaL_unref(L, LUA_REGISTRYINDEX, ctx.gate_ref);
	ctx.gate_ref = luaL_ref(L, LUA_REGISTRYINDEX);

	ctx_field_ref(L, 1, "m", LUA_TTABLE, &ctx.m_ref);
	ctx_field_ref(L, 1, "call_fallback", LUA_TFUNCTION, &ctx.fallback_ref);
	ctx_field_ref(L, 1, "finish_error", LUA_TFUNCTION,
		      &ctx.finish_error_ref);
	ctx_field_ref(L, 1, "finish_unref", LUA_TFUNCTION,
		      &ctx.finish_unref_ref);
	ctx_field_ref(L, 1, "recovery_check", LUA_TFUNCTION,
		      &ctx.recovery_check_ref);
	ctx_field_ref(L, 1, "is_nil", LUA_TFUNCTION, &ctx.is_nil_ref);
	ctx.is_set_up = true;
	return 0;
}

static int
vshard_storage_call_info(struct lua_State *L)
{
	lua_createtable(L, 0, 3);
	lua_pushboolean(L, ctx.is_set_up);
	lua_setfield(L, -2, "is_set_up");
	lua_pushboolean(L, ctx.is_set_up && ctx.gate->is_fast);
	lua_setfield(L, -2, "is_fast");
	lua_createtable(L, 0, FALLBACK_REASON_MAX + 3);
	lua_pushnumber(L, ctx.fast_count);
	lua_setfield(L, -2, "fast");
	lua_pushnumber(L, ctx.finish_error_count);
	lua_setfield(L, -2, "finish_error");
	lua_pushnumber(L, ctx.slow_unref_count);
	lua_setfield(L, -2, "slow_unref");
	for (int i = 0; i < FALLBACK_REASON_MAX; i++) {
		lua_pushnumber(L, ctx.fallback_count[i]);
		lua_setfield(L, -2, fallback_reason_strs[i]);
	}
	lua_setfield(L, -2, "counters");
	return 1;
}

#if defined(__GNUC__)
#define VSHARD_CALL_C_EXPORT __attribute__((visibility("default")))
#else
#define VSHARD_CALL_C_EXPORT
#endif

VSHARD_CALL_C_EXPORT int
luaopen_vshard_storage_call_c(struct lua_State *L)
{
	lua_createtable(L, 0, 3);
	lua_pushcfunction(L, vshard_storage_call);
	lua_setfield(L, -2, "call");
	lua_pushcfunction(L, vshard_storage_call_setup);
	lua_setfield(L, -2, "setup");
	lua_pushcfunction(L, vshard_storage_call_info);
	lua_setfield(L, -2, "info");
	return 1;
}
