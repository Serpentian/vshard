#include "refs.h"

#include <stdlib.h>
#include <string.h>

#include <msgpuck.h>
#include "module.h"
#include <lauxlib.h>

struct vsc_state vsc = {
	.refs = NULL,
	.bucket_count = 0,
	.bucket_space_id = 0,
	.is_master = false,
	.replica_id = NULL,
	.replicaset_id = NULL,
	.master_id = NULL,
	.on_event_ref = LUA_NOREF,
	.helpers_ref = LUA_NOREF,
	.event_coro = NULL,
	.event_coro_ref = LUA_NOREF,
};

/* Mirrors vshard.consts bucket statuses. */
enum vsc_bstatus {
	VSC_B_UNKNOWN = 0,
	VSC_B_ACTIVE,
	VSC_B_PINNED,
	VSC_B_SENDING,
	VSC_B_SENT,
	VSC_B_RECEIVING,
	VSC_B_GARBAGE,
	VSC_B_READONLY,
};

static enum vsc_bstatus
status_decode(const char *s, uint32_t len)
{
	switch (len) {
	case 4:
		if (memcmp(s, "sent", 4) == 0)
			return VSC_B_SENT;
		break;
	case 6:
		if (memcmp(s, "active", 6) == 0)
			return VSC_B_ACTIVE;
		if (memcmp(s, "pinned", 6) == 0)
			return VSC_B_PINNED;
		break;
	case 7:
		if (memcmp(s, "sending", 7) == 0)
			return VSC_B_SENDING;
		if (memcmp(s, "garbage", 7) == 0)
			return VSC_B_GARBAGE;
		break;
	case 8:
		if (memcmp(s, "readonly", 8) == 0)
			return VSC_B_READONLY;
		break;
	case 9:
		if (memcmp(s, "receiving", 9) == 0)
			return VSC_B_RECEIVING;
		break;
	}
	return VSC_B_UNKNOWN;
}

/* util.bucket_status_is_writable() */
static inline bool
status_is_writable(enum vsc_bstatus st)
{
	return st == VSC_B_ACTIVE || st == VSC_B_PINNED;
}

/* util.bucket_status_is_readable() */
static inline bool
status_is_readable(enum vsc_bstatus st)
{
	return status_is_writable(st) || st == VSC_B_SENDING ||
	       st == VSC_B_READONLY;
}

/**
 * Read the status of the bucket from _bucket. Mirrors the cold
 * path of bucket_check_state(), success cases only. Returns -1
 * when the bucket is missing or on a box error - error details
 * are not needed, the caller falls back to Lua.
 */
static int
bucket_fetch_status(uint32_t bucket_id, enum vsc_bstatus *out)
{
	char key[16];
	char *key_end = mp_encode_array(key, 1);
	key_end = mp_encode_uint(key_end, bucket_id);
	box_tuple_t *tuple;
	if (box_index_get(vsc.bucket_space_id, 0, key, key_end, &tuple) != 0)
		return -1;
	if (tuple == NULL)
		return -1;
	const char *field = box_tuple_field(tuple, 1);
	if (field == NULL || mp_typeof(*field) != MP_STR)
		return -1;
	uint32_t len;
	const char *s = mp_decode_str(&field, &len);
	*out = status_decode(s, len);
	return 0;
}

/*
 * Buckets with ids in [1, bucket_count] live in the flat array.
 * Ids outside of it (possible with manual _bucket edits and in
 * tests) go to this malloc'ed overflow list - each entry has a
 * stable address, which the flat array guarantees too.
 */
struct vsc_ref_ext {
	struct vsc_ref ref;
	uint32_t bucket_id;
	struct vsc_ref_ext *next;
};

static struct vsc_ref_ext *ext_refs = NULL;

static inline bool
is_in_range(uint32_t bucket_id)
{
	return vsc.refs != NULL && bucket_id >= 1 &&
	       bucket_id <= vsc.bucket_count;
}

static struct vsc_ref_ext *
ext_ref_find(uint32_t bucket_id)
{
	for (struct vsc_ref_ext *e = ext_refs; e != NULL; e = e->next) {
		if (e->bucket_id == bucket_id)
			return e;
	}
	return NULL;
}

struct vsc_ref *
vsc_ref_get(uint32_t bucket_id)
{
	if (!is_in_range(bucket_id)) {
		struct vsc_ref_ext *e = ext_ref_find(bucket_id);
		return e != NULL && e->ref.exists ? &e->ref : NULL;
	}
	struct vsc_ref *ref = &vsc.refs[bucket_id - 1];
	return ref->exists ? ref : NULL;
}

struct vsc_ref *
vsc_ref_new(uint32_t bucket_id)
{
	struct vsc_ref *ref;
	if (!is_in_range(bucket_id)) {
		struct vsc_ref_ext *e = ext_ref_find(bucket_id);
		if (e == NULL) {
			e = malloc(sizeof(*e));
			if (e == NULL)
				return NULL;
			e->bucket_id = bucket_id;
			e->next = ext_refs;
			ext_refs = e;
		}
		ref = &e->ref;
	} else {
		ref = &vsc.refs[bucket_id - 1];
	}
	memset(ref, 0, sizeof(*ref));
	ref->exists = true;
	return ref;
}

void
vsc_ref_del(uint32_t bucket_id)
{
	if (!is_in_range(bucket_id)) {
		struct vsc_ref_ext **ep = &ext_refs;
		while (*ep != NULL && (*ep)->bucket_id != bucket_id)
			ep = &(*ep)->next;
		if (*ep != NULL) {
			struct vsc_ref_ext *e = *ep;
			*ep = e->next;
			free(e);
		}
		return;
	}
	memset(&vsc.refs[bucket_id - 1], 0, sizeof(vsc.refs[0]));
}

void
vsc_refs_clear(void)
{
	if (vsc.refs != NULL)
		memset(vsc.refs, 0, vsc.bucket_count * sizeof(vsc.refs[0]));
	while (ext_refs != NULL) {
		struct vsc_ref_ext *e = ext_refs;
		ext_refs = e->next;
		free(e);
	}
}

static char *
strdup_or_null(const char *s)
{
	return s != NULL ? strdup(s) : NULL;
}

void
vsc_set_identity(const char *replica_id, const char *replicaset_id,
		 const char *master_id, bool is_master)
{
	free(vsc.replica_id);
	free(vsc.replicaset_id);
	free(vsc.master_id);
	vsc.replica_id = strdup_or_null(replica_id);
	vsc.replicaset_id = strdup_or_null(replicaset_id);
	vsc.master_id = strdup_or_null(master_id);
	vsc.is_master = is_master;
}

/* Fast path of bucket_refro(), vshard/storage/init.lua. */
int
vsc_refro(uint32_t bucket_id)
{
	struct vsc_ref *ref = vsc_ref_get(bucket_id);
	if (ref == NULL) {
		enum vsc_bstatus st;
		if (bucket_fetch_status(bucket_id, &st) != 0 ||
		    !status_is_readable(st))
			return -1;
		ref = vsc_ref_new(bucket_id);
		if (ref == NULL)
			return -1;
		ref->ro = 1;
		ref->rw_lock = !status_is_writable(st);
	} else if (ref->ro_lock) {
		return -1;
	} else {
		ref->ro++;
	}
	return 0;
}

/* Fast path of bucket_unrefro(). */
int
vsc_unrefro(uint32_t bucket_id)
{
	struct vsc_ref *ref = vsc_ref_get(bucket_id);
	uint32_t count = ref != NULL ? ref->ro : 0;
	if (count == 0)
		return -1;
	if (count == 1) {
		ref->ro = 0;
		if (ref->ro_lock) {
			/*
			 * GC is waiting for the bucket if RO is
			 * locked. It relies on the generation.
			 */
			vsc_fire_event(VSC_EVENT_BUCKET_GENERATION);
		}
		return 0;
	}
	ref->ro = count - 1;
	return 0;
}

/* Fast path of bucket_refrw(). */
int
vsc_refrw(uint32_t bucket_id)
{
	struct vsc_ref *ref = vsc_ref_get(bucket_id);
	if (ref == NULL) {
		enum vsc_bstatus st;
		if (bucket_fetch_status(bucket_id, &st) != 0 ||
		    !status_is_writable(st) || !vsc.is_master)
			return -1;
		ref = vsc_ref_new(bucket_id);
		if (ref == NULL)
			return -1;
		ref->rw = 1;
	} else if (ref->rw_lock || !vsc.is_master) {
		return -1;
	} else {
		ref->rw++;
	}
	return 0;
}

/* Fast path of bucket_unrefrw(). */
int
vsc_unrefrw(uint32_t bucket_id)
{
	struct vsc_ref *ref = vsc_ref_get(bucket_id);
	if (ref == NULL || ref->rw == 0)
		return -1;
	if (ref->rw == 1 && ref->rw_lock) {
		ref->rw = 0;
		vsc_fire_event(VSC_EVENT_RW_LOCK_READY);
	} else {
		ref->rw--;
	}
	return 0;
}
