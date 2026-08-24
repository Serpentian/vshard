/**
 * C implementation of the storage-side vshard.storage.call():
 * a stored C procedure processing a router request without
 * entering Lua, as long as the request is well-formed, the
 * bucket ref succeeds and the target function is a registered
 * C function. Everything else - malformed requests, ref
 * failures, Lua user functions - is delegated to the Lua
 * implementation (storage_call in vshard/storage/init.lua),
 * which reproduces the outcome and encodes the reply, giving
 * exact behavior parity at zero maintenance cost. The only
 * carve-out is a function which already ran and failed: it must
 * not be re-executed, so only its error encoding goes to Lua.
 *
 * Registered in _func as 'vshard.storage_c.call' etc: the
 * package is 'vshard.storage_c' -> file vshard/storage_c.so,
 * symbols are the last name part.
 */
#include "refs.h"

#include <stdlib.h>
#include <string.h>

#include <msgpuck.h>
#include "module.h"
#include <lauxlib.h>

#define VSC_EXPORT __attribute__((visibility("default")))

/* From src/box/errcode.h. Not exposed in module.h. */
enum {
	VSC_ER_PROC_C = 102,
};

static const char mp_true = '\xc3';

/** Names of the Lua helpers in the stashed helpers table. */
#define HELPER_FULL_CALL "full_call"
#define HELPER_ENCODE_ERROR "encode_error"
#define HELPER_ENCODE_UNREF_ERROR "encode_unref_error"

/**
 * The handshake vtable. vshard/storage/c_api.lua ffi.casts the
 * pointer returned by setup() and calls these to manipulate the
 * C-owned bucket refs from Lua (triggers, GC, rebalancer).
 */
struct vshard_storage_c_api {
	struct vsc_ref *(*ref_get)(uint32_t bucket_id);
	struct vsc_ref *(*ref_new)(uint32_t bucket_id);
	void (*ref_del)(uint32_t bucket_id);
	void (*refs_clear)(void);
	void (*set_identity)(const char *replica_id,
			     const char *replicaset_id,
			     const char *master_id, bool is_master);
};

static struct vshard_storage_c_api vsc_api = {
	.ref_get = vsc_ref_get,
	.ref_new = vsc_ref_new,
	.ref_del = vsc_ref_del,
	.refs_clear = vsc_refs_clear,
	.set_identity = vsc_set_identity,
};

void
vsc_fire_event(int ev)
{
	if (vsc.on_event_ref == LUA_NOREF || vsc.event_coro == NULL)
		return;
	lua_State *co = vsc.event_coro;
	int top = lua_gettop(co);
	lua_rawgeti(co, LUA_REGISTRYINDEX, vsc.on_event_ref);
	lua_pushinteger(co, ev);
	/*
	 * The callback must not yield. An error in it must not
	 * break the unref which fired the event.
	 */
	luaT_call(co, 1, 0);
	lua_settop(co, top);
}

/*
 * Coroutine pool for the Lua helpers. The full_call helper can
 * yield, so each in-flight call needs its own coro.
 */
struct coro_slot {
	lua_State *co;
	int ref;
};

enum { CORO_POOL_CAP = 16 };
static struct coro_slot coro_pool[CORO_POOL_CAP];
static int coro_pool_size = 0;

static int
coro_take(struct coro_slot *out)
{
	if (coro_pool_size > 0) {
		*out = coro_pool[--coro_pool_size];
		return 0;
	}
	lua_State *L = luaT_state();
	out->co = lua_newthread(L);
	if (out->co == NULL) {
		box_error_set(__FILE__, __LINE__, VSC_ER_PROC_C,
			      "can not create a Lua coroutine");
		return -1;
	}
	out->ref = luaL_ref(L, LUA_REGISTRYINDEX);
	return 0;
}

static void
coro_release(struct coro_slot *slot)
{
	lua_settop(slot->co, 0);
	if (coro_pool_size < CORO_POOL_CAP) {
		coro_pool[coro_pool_size++] = *slot;
	} else {
		luaL_unref(luaT_state(), LUA_REGISTRYINDEX, slot->ref);
	}
}

/**
 * Call a Lua helper from the stashed helpers table with nargs
 * arguments already described by the caller-provided pusher.
 * The helper returns one complete msgpack-encoded reply array;
 * it is copied onto the box region. Returns -1 with the diag set
 * when the helper itself fails - the caller propagates that as
 * the client-visible error, which for full_call exactly matches
 * the error the Lua wrapper would raise.
 */
typedef void (*vsc_push_args_f)(lua_State *co, void *ctx);

static int
helper_call(const char *helper, vsc_push_args_f push_args, void *ctx,
	    int nargs, const char **out, const char **out_end)
{
	if (vsc.helpers_ref == LUA_NOREF) {
		box_error_set(__FILE__, __LINE__, VSC_ER_PROC_C,
			      "vshard C helpers are not initialized");
		return -1;
	}
	struct coro_slot slot;
	if (coro_take(&slot) != 0)
		return -1;
	lua_State *co = slot.co;
	lua_rawgeti(co, LUA_REGISTRYINDEX, vsc.helpers_ref);
	lua_getfield(co, -1, helper);
	lua_remove(co, -2);
	if (push_args != NULL)
		push_args(co, ctx);
	if (luaT_call(co, nargs, 1) != 0) {
		coro_release(&slot);
		return -1;
	}
	size_t len;
	const char *s = lua_tolstring(co, -1, &len);
	int rc = -1;
	if (s == NULL || len == 0) {
		box_error_set(__FILE__, __LINE__, VSC_ER_PROC_C,
			      "vshard C helper '%s' must return "
			      "a msgpack string", helper);
	} else {
		char *copy = box_region_alloc(len);
		if (copy != NULL) {
			memcpy(copy, s, len);
			*out = copy;
			*out_end = copy + len;
			rc = 0;
		}
	}
	coro_release(&slot);
	return rc;
}

/** Emit a helper reply: one port entry per array element. */
static int
reply_emit(box_function_ctx_t *ctx, const char *reply, const char *reply_end)
{
	(void)reply_end;
	if (mp_typeof(*reply) != MP_ARRAY) {
		box_error_set(__FILE__, __LINE__, VSC_ER_PROC_C,
			      "malformed vshard C helper reply");
		return -1;
	}
	const char *q = reply;
	uint32_t cnt = mp_decode_array(&q);
	for (uint32_t i = 0; i < cnt; i++) {
		const char *e = q;
		mp_next(&q);
		box_return_mp(ctx, e, q);
	}
	return 0;
}

struct mp_slice {
	const char *data;
	const char *data_end;
};

static void
push_mp_slice(lua_State *co, void *ctx)
{
	struct mp_slice *s = ctx;
	lua_pushlstring(co, s->data, s->data_end - s->data);
}

/** Delegate the whole request to the Lua storage_call(). */
static int
full_lua_call(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	struct mp_slice s = {args, args_end};
	const char *reply, *reply_end;
	if (helper_call(HELPER_FULL_CALL, push_mp_slice, &s, 1, &reply,
			&reply_end) != 0)
		return -1;
	return reply_emit(ctx, reply, reply_end);
}

struct unref_err_args {
	uint64_t bucket_id;
	bool is_write;
	/* The error of the failed call, if any, else NULL. */
	const char *call_err;
	const char *call_err_end;
};

static void
push_unref_err_args(lua_State *co, void *ctx)
{
	struct unref_err_args *a = ctx;
	lua_pushinteger(co, a->bucket_id);
	lua_pushlstring(co, a->is_write ? "write" : "read", a->is_write ?
			5 : 4);
	if (a->call_err != NULL) {
		lua_pushlstring(co, a->call_err,
				a->call_err_end - a->call_err);
	} else {
		lua_pushnil(co);
	}
}

/**
 * vshard.storage_c.call(bucket_id, mode, name[, args])
 *
 * The fast path of storage_call() with the same reply
 * convention. Success: [true, ret1..ret3] with trailing nils
 * dropped. Anything unusual goes back to Lua (see the file
 * header).
 */
VSC_EXPORT int
call(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	const char *p = args;
	if (mp_typeof(*p) != MP_ARRAY || mp_decode_array(&p) < 3 ||
	    mp_typeof(*p) != MP_UINT)
		return full_lua_call(ctx, args, args_end);
	uint64_t bucket_id = mp_decode_uint(&p);
	if (mp_typeof(*p) != MP_STR)
		return full_lua_call(ctx, args, args_end);
	uint32_t mode_len;
	const char *mode = mp_decode_str(&p, &mode_len);
	bool is_write;
	if (mode_len == 4 && memcmp(mode, "read", 4) == 0)
		is_write = false;
	else if (mode_len == 5 && memcmp(mode, "write", 5) == 0)
		is_write = true;
	else
		return full_lua_call(ctx, args, args_end);
	if (mp_typeof(*p) != MP_STR)
		return full_lua_call(ctx, args, args_end);
	uint32_t name_len;
	const char *name = mp_decode_str(&p, &name_len);
	const char *uargs, *uargs_end;
	static const char empty_args[] = {(char)0x90};
	if (p < args_end && mp_typeof(*p) == MP_ARRAY) {
		uargs = p;
		mp_next(&p);
		uargs_end = p;
	} else if (p >= args_end || mp_typeof(*p) == MP_NIL) {
		uargs = empty_args;
		uargs_end = empty_args + 1;
	} else {
		return full_lua_call(ctx, args, args_end);
	}

	if ((is_write ? vsc_refrw(bucket_id) : vsc_refro(bucket_id)) != 0)
		return full_lua_call(ctx, args, args_end);

	const char *ret, *ret_end;
	int rc = box_func_call_by_name(name, name_len, uargs, uargs_end,
				       &ret, &ret_end);
	if (rc == 1) {
		/*
		 * Not in the _func registry - a Lua function.
		 * Release the ref and delegate the whole call:
		 * there is no yield until the Lua side re-refs,
		 * so the bucket state can not change in between.
		 * The unref of a held ref can not fail.
		 */
		is_write ? vsc_unrefrw(bucket_id) : vsc_unrefro(bucket_id);
		return full_lua_call(ctx, args, args_end);
	}

	const char *err_reply = NULL, *err_reply_end = NULL;
	if (rc != 0) {
		/*
		 * The function ran and failed - must not be
		 * re-executed. Only the error encoding goes to
		 * Lua (it also does the box.rollback() of a
		 * leaked transaction, as handle_results() does).
		 */
		if (helper_call(HELPER_ENCODE_ERROR, NULL, NULL, 0,
				&err_reply, &err_reply_end) != 0)
			return -1;
	}

	rc = is_write ? vsc_unrefrw(bucket_id) : vsc_unrefro(bucket_id);
	if (rc != 0) {
		/* Should not normally happen. */
		struct unref_err_args a = {
			.bucket_id = bucket_id,
			.is_write = is_write,
			.call_err = NULL,
			.call_err_end = NULL,
		};
		if (err_reply != NULL) {
			/* Pass the [false, err] err part as prev. */
			const char *q = err_reply;
			if (mp_typeof(*q) == MP_ARRAY &&
			    mp_decode_array(&q) >= 2) {
				mp_next(&q);
				a.call_err = q;
				mp_next(&q);
				a.call_err_end = q;
			}
		}
		const char *reply, *reply_end;
		if (helper_call(HELPER_ENCODE_UNREF_ERROR,
				push_unref_err_args, &a, 3, &reply,
				&reply_end) != 0)
			return -1;
		return reply_emit(ctx, reply, reply_end);
	}

	if (err_reply != NULL)
		return reply_emit(ctx, err_reply, err_reply_end);

	/* Success: [true, r1..rk], k <= 3, trailing nils dropped. */
	const char *r[3], *r_end[3];
	int nres = 0;
	const char *q = ret;
	if (mp_typeof(*q) == MP_ARRAY) {
		uint32_t cnt = mp_decode_array(&q);
		for (uint32_t i = 0; i < cnt && i < 3; i++) {
			r[i] = q;
			mp_next(&q);
			r_end[i] = q;
		}
		nres = cnt < 3 ? (int)cnt : 3;
		/*
		 * Mirrors the trailing nil truncation in
		 * storage_call(): do not pad the reply with
		 * box.NULLs.
		 */
		while (nres > 0 && mp_typeof(*r[nres - 1]) == MP_NIL)
			nres--;
	}
	box_return_mp(ctx, &mp_true, &mp_true + 1);
	for (int i = 0; i < nres; i++)
		box_return_mp(ctx, r[i], r_end[i]);
	return 0;
}

/**
 * vshard.storage_c.setup({bucket_count, replica_id,
 *                         replicaset_id, master_id, is_master})
 *
 * One-time handshake called from storage_cfg(). Returns the
 * address of the api vtable as an unsigned number for Lua to
 * ffi.cast. Also picks up the Lua callbacks stashed in _G by
 * c_api.lua right before the call.
 */
VSC_EXPORT int
setup(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	(void)args_end;
	const char *p = args;
	char replica[128], replicaset[128], master[128];
	bool has_master = false;
	if (mp_typeof(*p) != MP_ARRAY || mp_decode_array(&p) < 5)
		goto usage;
	if (mp_typeof(*p) != MP_UINT)
		goto usage;
	uint64_t bucket_count = mp_decode_uint(&p);
	for (int i = 0; i < 3; i++) {
		char *dst = i == 0 ? replica : (i == 1 ? replicaset : master);
		if (mp_typeof(*p) == MP_STR) {
			uint32_t len;
			const char *s = mp_decode_str(&p, &len);
			if (len > 127)
				goto usage;
			memcpy(dst, s, len);
			dst[len] = 0;
			if (i == 2)
				has_master = true;
		} else if (mp_typeof(*p) == MP_NIL) {
			mp_decode_nil(&p);
			dst[0] = 0;
		} else {
			goto usage;
		}
	}
	if (mp_typeof(*p) != MP_BOOL)
		goto usage;
	bool is_master = mp_decode_bool(&p);

	uint32_t space_id = box_space_id_by_name("_bucket", 7);
	if (space_id == BOX_ID_NIL) {
		box_error_set(__FILE__, __LINE__, VSC_ER_PROC_C,
			      "space _bucket is not found");
		return -1;
	}
	if (vsc.refs == NULL || vsc.bucket_count != bucket_count) {
		free(vsc.refs);
		vsc.refs = calloc(bucket_count, sizeof(vsc.refs[0]));
		if (vsc.refs == NULL) {
			vsc.bucket_count = 0;
			box_error_set(__FILE__, __LINE__, VSC_ER_PROC_C,
				      "can not allocate the bucket ref array");
			return -1;
		}
		vsc.bucket_count = bucket_count;
	}
	vsc.bucket_space_id = space_id;
	vsc_set_identity(replica, replicaset, has_master ? master : NULL,
			 is_master);

	lua_State *L = luaT_state();
	lua_getglobal(L, "__vshard_storage_c_on_event");
	if (lua_isfunction(L, -1)) {
		if (vsc.on_event_ref != LUA_NOREF)
			luaL_unref(L, LUA_REGISTRYINDEX, vsc.on_event_ref);
		vsc.on_event_ref = luaL_ref(L, LUA_REGISTRYINDEX);
		lua_pushnil(L);
		lua_setglobal(L, "__vshard_storage_c_on_event");
	} else {
		lua_pop(L, 1);
	}
	lua_getglobal(L, "__vshard_storage_c_helpers");
	if (lua_istable(L, -1)) {
		if (vsc.helpers_ref != LUA_NOREF)
			luaL_unref(L, LUA_REGISTRYINDEX, vsc.helpers_ref);
		vsc.helpers_ref = luaL_ref(L, LUA_REGISTRYINDEX);
		lua_pushnil(L);
		lua_setglobal(L, "__vshard_storage_c_helpers");
	} else {
		lua_pop(L, 1);
	}
	if (vsc.event_coro == NULL) {
		vsc.event_coro = lua_newthread(L);
		vsc.event_coro_ref = luaL_ref(L, LUA_REGISTRYINDEX);
	}

	char buf[16];
	char *e = mp_encode_uint(buf, (uintptr_t)&vsc_api);
	box_return_mp(ctx, buf, e);
	return 0;
usage:
	box_error_set(__FILE__, __LINE__, VSC_ER_PROC_C,
		      "Usage: vshard.storage_c.setup({bucket_count, "
		      "replica_id, replicaset_id, master_id, is_master})");
	return -1;
}

