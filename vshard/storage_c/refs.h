#ifndef VSHARD_STORAGE_C_REFS_H
#define VSHARD_STORAGE_C_REFS_H

#include <stdint.h>
#include <stdbool.h>

struct lua_State;

/**
 * Bucket reference counters. Layout must match the ffi.cdef in
 * vshard/storage/c_api.lua. Unlike the old Lua version, existence
 * is a flag instead of table membership: refs live in a flat
 * array indexed by bucket_id.
 */
struct vsc_ref {
	uint32_t ro;
	uint32_t rw;
	bool rw_lock;
	bool ro_lock;
	bool exists;
};

/** Events fired into Lua on cold unref paths. */
enum vsc_event {
	/** Last ro ref under ro_lock is gone - wake the GC. */
	VSC_EVENT_BUCKET_GENERATION = 1,
	/** Last rw ref under rw_lock is gone - wake bucket_send. */
	VSC_EVENT_RW_LOCK_READY = 2,
};

struct vsc_state {
	/** Flat array [0..bucket_count), entry i is bucket_id i + 1. */
	struct vsc_ref *refs;
	uint32_t bucket_count;
	/** box.space._bucket id, resolved at setup. */
	uint32_t bucket_space_id;
	/** Mirror of M.is_master, updated by Lua via the api. */
	bool is_master;
	/** Mirrors of M.this_replica.id and co, kept for the api. */
	char *replica_id;
	char *replicaset_id;
	char *master_id;
	/** Registry ref of the Lua event callback, LUA_NOREF if unset. */
	int on_event_ref;
	/** Registry ref of the Lua helpers table. */
	int helpers_ref;
	/** Persistent non-yielding coroutine for firing events. */
	struct lua_State *event_coro;
	int event_coro_ref;
};

extern struct vsc_state vsc;

/**
 * Ref/unref with vshard.storage semantics, the fast path only.
 * Return 0 on success, -1 on any failure - the caller is expected
 * to delegate the request to the Lua implementation, which
 * reproduces the failure and builds the proper error.
 */
int
vsc_refro(uint32_t bucket_id);

int
vsc_refrw(uint32_t bucket_id);

int
vsc_unrefro(uint32_t bucket_id);

int
vsc_unrefrw(uint32_t bucket_id);

/** FFI-facing accessors, exposed to Lua via the api struct. */
struct vsc_ref *
vsc_ref_get(uint32_t bucket_id);

struct vsc_ref *
vsc_ref_new(uint32_t bucket_id);

void
vsc_ref_del(uint32_t bucket_id);

void
vsc_refs_clear(void);

void
vsc_set_identity(const char *replica_id, const char *replicaset_id,
		 const char *master_id, bool is_master);

/** Implemented in storage_c.c - calls the Lua event callback. */
void
vsc_fire_event(int ev);

#endif /* VSHARD_STORAGE_C_REFS_H */
