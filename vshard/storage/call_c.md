# C acceleration of `vshard.storage.call`

## What it is

`vshard/storage/call_c.c` implements the hot path of `vshard.storage.call`
as a C function. It is compiled into `call_c.so`, loaded as a regular Lua-C
module (`require('vshard.storage.call_c')`) and registered in the core's
function registry:

    box.iproto.export('vshard.storage.call', cmod.call)

The core checks that registry in `box_lua_find()` **before** walking `_G`,
so every `IPROTO_CALL` of `'vshard.storage.call'` is served by the C
function while:

- the body-less `_func` entry `'vshard.storage.call'`, its grants and
  `setuid` stay exactly as before (access control is applied by
  `box_process_call()` before the body is resolved);
- routers - of any version - need no changes and notice no difference:
  arguments are decoded and results are encoded by the very same core code
  as for a Lua stored procedure, so the wire format is identical;
- local Lua callers keep using the Lua `vshard.storage.call` exported from
  the module - the C path accelerates only the iproto entry, which is the
  only hot one.

Why not a `language='C'` stored function: the core resolves a C function's
module through Lua `package.search()`, where Lua paths always win, so the
package `'vshard.storage'` resolves to `vshard/storage/init.lua` and can
never `dlopen`. Also `_func` is replicated, which would break mixed
clusters where only some instances have the `.so`. The registry-based
approach is per-instance and purely local.

## What the C path does

For the common case the C function performs, without a single Lua frame:

1. Checks the gate flag (`M.c_call_gate.is_fast` - a `bool` in an FFI
   struct shared with Lua by pointer). The flag is `true` exactly when the
   Lua API cache is in its "safe" state (`storage_api_call_safe`), i.e.
   the storage is loaded, configured and enabled.
2. Validates the `(bucket_id, mode, name, args)` envelope.
3. Resolves the user function: `box.func[name]` must be empty, then a
   plain `'.'`-path walk over `_G` (the same subset `box_lua_find()`
   implements).
4. Takes the bucket ref by incrementing the `struct bucket_ref` counter
   directly in the cdata stored in `M.bucket_refs[bucket_id]` - exactly
   the branches `bucket_refro()`/`bucket_refrw()` would take on an
   existing entry without lock flags (plus the `M.is_master` check for
   writes).
5. Calls the user function via `lua_pcall`.
6. Releases the ref (re-fetching the entry - the user function could have
   yielded and the refs table could have been changed under it).
7. Truncates trailing nils (including `box.NULL` - checked with real Lua
   `== nil` semantics), caps the results at 3 and returns
   `true, ret1..ret3`.

## Fallbacks: what still runs in Lua and why

Any condition outside the common case makes the C function tail-call the
public Lua `vshard.storage.call` closure with the original arguments, so
the behavior of those cases is the Lua behavior by construction:

| Condition | Why Lua |
|---|---|
| Core has no `box.iproto.export` (< 3.7.0) or no `.so` is installed | The C path is never registered at all. Detected per instance, mixed clusters are fine. |
| `VSHARD_NO_C_CALL` env var is set | Operator kill switch. |
| Storage not loaded/configured/enabled (gate is shut) | The gating checks live in `storage_api_call_unsafe()` and raise rich vshard errors; they also flip the api-call cache. Rare by definition - only around startup/disable. |
| `bucket_id` is not a plain number, `mode` is not `'read'`/`'write'`, `name` is not a string, `args` is not nil/table | Exotic envelopes (numeric strings, msgpack objects, tostring-able names) have subtle coercion semantics in `net_box.self.call`. |
| `box.ctl.is_recovery_finished()` is not yet true | netbox resolves names differently before recovery ends. Checked until true once, then cached (it is monotonic). |
| `name` is present in `box.func` | Persistent/stored-C/SQL functions are invoked through the `f:call()` machinery. |
| `name` contains `':'` or `'['`, or does not resolve to a plain Lua function over a `'.'`-path in `_G` | Method calls and callable tables have extra invocation semantics; missing functions must produce the exact `NO_SUCH_PROC` error. |
| No `M.bucket_refs` entry for the bucket | The first touch of a bucket needs a `_bucket` state check and error construction (`WRONG_BUCKET`, `TRANSFER_IS_IN_PROGRESS`, ...). Once the entry exists (it survives with zero counters), the C path serves it. |
| `ro_lock`/`rw_lock` is set | GC/transfer is waiting for the bucket - needs `BUCKET_IS_LOCKED` errors, generation bumps, cond broadcasts. |
| `mode == 'write'` and the instance is not the master | Needs the `NON_MASTER` error with master coordinates. |

All of the above are checked **before** any side effect, so re-running the
whole call in Lua is invisible.

Two cases cannot be re-run because the user function has already executed;
they are handled by dedicated Lua helpers (the same code the Lua path
uses):

- the user function failed: `box.rollback()` + `PROC_LUA` wrap +
  `lerror.make()` + unref with `err.prev` chaining on a half-deleted
  bucket;
- the fast unref could not be done (entry gone, counter zero, lock flags):
  the Lua `bucket_unref()` runs, and its failure turns the whole request
  into `nil, BUCKET_IS_CORRUPTED` with the user results discarded.

## Known divergences from the Lua path

- Error `trace`/`line`/`file` fields point at different source locations
  (they always differ between call paths anyway).
- A raising `__index` metamethod on the `_G` path of the user function
  name surfaces as a call error instead of a `[false, err]` reply.
- A returned value that the msgpack encoder rejects fails at
  encode time in both paths, but through different code (both produce an
  iproto error).
- Upgrading `call_c.so` on disk requires an instance restart; a vshard
  hot reload (package.reload) keeps using the already-loaded module and
  only rebinds it to the new Lua closures.

## Build

    cmake . && make        # builds vshard/storage/call_c.so
    # -DVSHARD_ENABLE_C=OFF to opt out

The build degrades to pure-Lua automatically when `tarantool/module.h` is
too old (no `luaL_iscdata`). A missing `.so` at runtime is logged once at
the info level and is not an error.

## Observability

    require('vshard.storage.call_c').info()

returns `is_set_up`, `is_fast` and per-reason counters (`fast`,
`not_ready`, `bad_args`, `recovery`, `func_resolve`, `bucket_state`,
`finish_error`, `slow_unref`).

## Performance

Measured on core 3.8 (RelWithDebInfo), echo workload, single storage:

- dispatch component alone (everything between iproto decode and the user
  function): ~232 ns/call vs ~297 ns/call in Lua - **1.28x**;
- end-to-end RPS with a netbox client on the same machine: **~1.1x**
  (the remaining cost is the iproto stack and the client itself, which
  this change does not touch).

The gain grows with how little the user function does; heavy user
functions dilute it.

## Router-side acceleration - analysis, no code

The router hot path is dominated by netbox encoding/decoding and wire I/O
that already live in core C; the Lua-side work per call (route lookup,
balancing, retries) is thin, and `return_raw` already avoids re-decoding
replies. The func-registry technique does not apply - `router.call` is a
local library call, not an iproto-served function - so acceleration would
mean rewriting `router_call_impl()` internals as a Lua-C module: high
risk, low expected gain. Revisit only if profiling shows router CPU (not
network) as the bottleneck. The C module here is structured so that a
`box_function_ctx_t` stored-function entry point could be added next to
the registry one, should the core's C-module resolution (Lua-before-C
`package.search` ordering) get fixed upstream - that would remove the
remaining Lua-value plumbing on the storage.
