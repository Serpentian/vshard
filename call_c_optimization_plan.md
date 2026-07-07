# Make the C vshard.storage.call fast path actually fast on real workloads

## Context

`example/perf` showed only ~3% improvement on the `replace` workload. Live inspection
of the running cluster (`storage_1_a.control`) explained it definitively:

- `is_set_up: true`, `is_fast: true` — the C path was registered and open, **but**
- `counters: fast: 0, func_resolve: 4634226` — **every one of 4.6M calls fell back
  at function resolution**.

The perf workload passes `func_name = 'box.space.customer:replace'`
(`example/perf/router.lua:33-45`) — method-call syntax with `':'`, which
`func_resolve()` in `vshard/storage/call_c.c` deliberately rejects. The measured 3%
was only the residual saving of the registry hit vs the `_G` dot-walk in
`box_lua_find`. Passing space methods (`box.space.X:replace/get/...`) is the single
most common vshard usage pattern, so the fast path currently misses the main
workload class.

Additional verified facts driving this plan (tarantool source + vshard):

- **Generation guard is sound**: every site that removes a `M.bucket_refs` entry or
  replaces the table bumps `M.bucket_generation` in the same critical section with
  no yield (`init.lua:535,:544` + `:693`; truncate `:737` + `:738`). Caching the
  ref cdata across the user call, pinned on the Lua stack and revalidated by
  generation compare, is safe (same technique as `bucket_guard_xc`,
  `init.lua:786-798`).
- **`box.func` table identity is stable** once created (created lazily-once in
  `lbox_func_new`, `tarantool/src/box/lua/call.c:1150-1160`; only mutated after).
  A cached registry ref is safe if taken after the table exists.
- **Latent probe bug**: `feature.iproto_func_registry` (`vshard/util.lua`) is
  computed eagerly when `vshard.util` loads — *before* `box.cfg`. On cores where
  `box.iproto` appears only after configuration this freezes `false` forever and
  `c_call.install()` silently no-ops (it logs nothing in that branch).
- **Observability gap**: successful registration logs nothing — this cost a whole
  debugging round.
- The remaining per-call core plumbing (fresh coroutine + 2 `lua_pcall` frames +
  arg/result marshalling in `box_process_lua`) is out of reach without the
  stored-C-ABI variant — explicitly **out of scope** (user decision); re-measure
  first, revisit later.

User decisions: quick wins + honest measurement (no stored-C ABI phase now); also
export the router public API into the func registry.

## Phase 1 — method-call resolution in the C fast path (the big one)

`vshard/storage/call_c.c`, `func_resolve()`:

- Accept names with exactly one `':'`: `obj.path:method`. Mirror `box_lua_find`'s
  `':'` branch (`tarantool/src/box/lua/call.c:148-152`): resolve the dot-path
  before `':'` to the object (must be table/userdata/lightuserdata), then
  `lua_gettable` the method from it, ensure `LUA_TFUNCTION`, and arrange the stack
  as `method, object` (`lua_insert`).
- Change `func_resolve` to return the number of pushed values (0 = fallback,
  1 = plain function, 2 = method + object); the call site then pushes args and does
  `lua_pcall(nargs + npushed - 1, LUA_MULTRET, 0)` — netbox self-call convention
  `pcall(proc, obj, unpack(args))` (`net_box.lua:1431-1435`).
- Keep rejecting `'['` and malformed names (empty segments, method name containing
  `'.'`) — fallback, as today.
- Note: the ref fast path must now run with 1 or 2 resolution values on the stack —
  adjust the index math (see Phase 3 restructure; do them together).

Tests (extend `test/storage-luatest/storage_call_c_test.lua`): create a sharded
test space in `before_all` on the master; parity cases for
`'box.space.<space>:replace'`, `':get'`, method on a nested plain table, missing
method (NO_SUCH_PROC via fallback), `':'` with non-table object, name with both
`':'` and `'['` (fallback). Assert `fast` counter delta covers the method cases.

## Phase 2 — observability + probe robustness

1. `vshard/storage/init.lua`, `c_call.install()`:
   - Log once on success: `log.info('C acceleration of vshard.storage.call is enabled')`.
   - Compute the capability *at install time* instead of using the frozen
     `util.feature` value: turn the feature into a function
     `feature.iproto_func_registry()` in `vshard/util.lua` (memoize on first true),
     and update the call sites (storage install, new router export, test file
     guard). Install always runs after `box.cfg`, so the probe is reliable there.
2. `example/perf`: make the harness prove which path ran. In
   `example/perf/generate_load.lua` (or a small helper in `router.lua`), accept the
   storage URIs (already known cluster: 3301/3303) or an explicit flag, fetch
   `require('vshard.storage.call_c').info()` from each master before/after the run
   via net.box, and print the `fast`/fallback deltas next to the RPS number. Warn
   loudly when `fast` did not move.

## Phase 3 — micro-optimizations in `call_c.c` (dispatch 232ns → target ~160-180ns)

Restructure the hot path once, implementing all of these together:

1. **Hot fields in the shared cdata.** Extend the (pre-release, safe to change in
   place) struct:
   `struct vshard_call_gate { bool is_fast; bool is_master; uint64_t bucket_generation; }`.
   - Lua side (`vshard/storage/init.lua`): route every `M.is_master = ...`
     assignment through one setter that also writes `M.c_call_gate.is_master`
     (enumerate sites by grep; there is a master-switch test to catch misses —
     `test_storage_callro_refrw_loss`). `bucket_generation_increment()`
     (`init.lua:484-491`) additionally stores the new value into the cdata; also
     initialize both fields at load/reload and in `storage_cfg`.
   - C side: the write-mode master check and all generation reads become direct
     memory reads (zero Lua API calls).
   - **Correctness invariant to verify in review**: a site setting
     `M.is_master = false` without the cdata sync would let the C path accept a
     write on a non-master. The setter must be exhaustive; add a
     write-through-C-after-master-switch test.
2. **Generation-guarded ref reuse.** Find the ref cdata *first*, keep it pinned on
   the Lua stack for the whole call (GC-safe), record `gate->bucket_generation`.
   After `lua_pcall`: if generation is unchanged — decrement directly through the
   saved pointer (no lookups at all); otherwise do today's full re-find/slow path.
3. **Lazy `box.func` ref cache.** On each call, if the cached ref is missing, do
   today's 3-step lookup and cache the table ref once it exists; afterwards it is
   one `rawgeti` + `rawget`.
4. **Skip `lua_checkstack`** when `nargs` is small (entry guarantees
   `LUA_MINSTACK` = 20 free slots; our fixed overhead is ≤ 8 slots, so only call
   checkstack when `nargs > 12`).

Re-run the dispatch microbench (`$JOB_TMP/smoke/dispatch.lua`) after each step to
attribute gains; keep only what measures.

## Phase 4 — router API export (no C)

`vshard/router/init.lua`: at the end of `router_cfg` (static router only), when
`feature.iproto_func_registry()`:

- `box.iproto.export('vshard.router.<name>', fn)` for the public iproto-called
  family (`call`, `callro`, `callrw`, `callre`, `callbro`, `callbre`) using the
  same module-level closures the module returns, with the same
  check-registry-first / warn-on-foreign-entry / never-raise pattern as the
  storage install.
- Same treatment for `'vshard.storage._call'` (service_call) on the storage side —
  one more `box.iproto.export` next to the C one; it serves router discovery/ref
  traffic.
- Test: small addition to the new suite (or router-luatest): registry entry
  exists on the router, an iproto `vshard.router.callrw` still works, double-cfg
  does not raise.

## Phase 5 — honest re-measurement + docs

1. `example/perf` replaces, A/B via `VSHARD_NO_C_CALL=1` vs unset (cluster restart
   between runs), with the new counters report proving `fast ≈ ops`. Also run a
   read op (add a `get` operation to `example/perf/router.lua` operations table if
   absent) — no WAL, so the dispatch share and the visible gain are larger.
2. Optional (if `perf` is available in the nix shell): flamegraph of the storage
   master during the replace run; report the dispatch share vs box/WAL vs iproto —
   this is the documented ceiling for any further dispatch work.
3. Update `vshard/storage/call_c.md`: method-call support (remove it from the
   fallback matrix), new counters, new numbers, router export note.
4. Full test sweep: new suite + `storage_call_test.lua` + `storage_1*` +
   `router_2_2_test.lua` on 3.8; graceful-degradation run under the old 3.7
   binary (`.rocks/bin/luatest` wrapper).

## Files

- `vshard/storage/call_c.c` — Phases 1, 3
- `vshard/storage/init.lua` — Phases 2.1, 3.1, 4 (`_call` export)
- `vshard/util.lua` — feature-as-function
- `vshard/router/init.lua` — Phase 4
- `example/perf/generate_load.lua`, `example/perf/router.lua` — Phase 2.2, Phase 5.1
- `test/storage-luatest/storage_call_c_test.lua` — new cases
- `vshard/storage/call_c.md` — Phase 5.3

## Verification

- Dispatch microbench before/after each Phase-3 step (isolated component numbers).
- New/existing luatest suites green on 3.8; degradation run on old 3.7 binary.
- Live example cluster: `require('vshard.storage.call_c').info()` on a master
  after a perf run shows `fast` ≈ number of ops and near-zero `func_resolve`.
- example/perf A/B numbers reported for `replace` and a read op, with the
  measured dispatch-share ceiling stated alongside.

## Expectations to communicate with the results

Even with the fast path fully engaged, `replace` is WAL-bound and double-hop
(client→router→storage): the dispatch saving is a small slice of the per-op cost.
The read op and the storage CPU capacity (ops/sec at saturation) are where the
gain is visible; the flamegraph number makes the ceiling explicit instead of
guessed.
