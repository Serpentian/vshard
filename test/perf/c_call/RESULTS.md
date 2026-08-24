# C storage.call prototype — benchmark results

## The 2x2 matrix: wrapper x user function language

RPS, average of two 15 s runs, 10 fibers, RelWithDebInfo
3.9.0-entrypoint-86. "direct" = net.box straight to the storage
(pure storage-side cost), "router" = through `vshard.router.call`.

| direct, RPS       | C func                          | Lua func              |
|-------------------|---------------------------------|-----------------------|
| **C wrapper**     | replace **117k** / get **192k** | replace 71k / get 85k |
| **Lua wrapper**   | replace 73k / get 95k           | replace 71k / get 82k |

| router, RPS       | C func                          | Lua func              |
|-------------------|---------------------------------|-----------------------|
| **C wrapper**     | replace **82k** / get **111k**  | replace 75k / get 58k |
| **Lua wrapper**   | replace 60k / get 65k           | replace 69k / get 65k |

p50 latency, direct get: C+C ~41–45 µs vs Lua+Lua ~92–113 µs.

Reading: the fully-C cell (C wrapper + C func) is the only one that
avoids Lua entirely and it dominates — ~2.3x RPS on storage-side
reads and ~2.5x lower p50 vs the all-Lua cell; writes gain ~1.6x
until the WAL bound. The mixed cells buy little: with a Lua user
function the C wrapper delegates the whole request back to Lua
(storage_call re-execution), and with the Lua wrapper a C function
still pays the full Lua wrapper overhead. Run-to-run variance on
this shared dev box is ±20-30% — compare cells within one table,
not across documents.

Date: 2026-08-24. Machine: local dev box, 16 cores. Tarantool:
3.9.0-entrypoint-86 RelWithDebInfo (`tnt/tarantool` checkout with the
`box_func_call_by_name` patch ported from `tnt/tarantool_clean`). One
storage instance (memtx, wal_mode=write) + one router/client process,
unix-localhost TCP. `run.sh` drives the matrix: wrapper (Lua
`vshard.storage.call` vs C `vshard.storage_c.call`) × user function
(C stored func vs `_G` Lua) × transport (through `vshard.router.call`
vs direct `net.box`). Storage always runs with `VSHARD_C_CALL=1` (refs
in C in both series); the wrapper is chosen by the router-side env.

## 10 fibers, 10 s per cell

| workload            | Lua wrapper RPS | C wrapper RPS | Δ    | Lua p50 | C p50 |
|---------------------|-----------------|---------------|------|---------|-------|
| router replace C    | 73 497          | 109 196       | +49% | 113 µs  | 79 µs |
| router replace Lua  | 73 061          | 84 932        | +16% | 104 µs  | 103µs |
| router get C        | 72 764          | 118 597       | +63% | 112 µs  | 67 µs |
| router get Lua      | 47 333          | 78 091        | +65% | 175 µs  | 104µs |
| direct replace C    | 117 630         | 118 763       | ~0%¹ | 75 µs   | 62 µs |
| direct get C        | 110 996         | 138 931       | +25% | 73 µs   | 67 µs |

¹ client-bound at 10 fibers; see the 50-fiber run.

## 50 fibers, 10 s per cell

| workload            | Lua wrapper RPS | C wrapper RPS | Δ     |
|---------------------|-----------------|---------------|-------|
| router replace C    | 78 958          | 130 358       | +65%  |
| router get C        | 104 485         | 143 719       | +38%  |
| router get Lua      | 67 361          | 121 000       | +80%  |
| direct replace C    | 132 403         | 149 555       | +13%  |
| direct get C        | 153 979         | **300 110**   | +95%  |

## Reading the numbers

- The fully Lua-free path (C wrapper + C user function) roughly
  doubles read throughput storage-side (300k vs 154k direct get) and
  gives +38..65% through the router; the router-side Lua is then the
  next bottleneck.
- Writes gain less at high concurrency — the WAL becomes the shared
  bound — but p50 latency still drops substantially.
- Even Lua user functions win under the C wrapper (+16..80%): the
  wrapper overhead (refs, dispatch, reply building) is gone even
  though the function itself still runs in Lua.
- Single-client caveat: the router/client is one process; cells at
  ~120-160k RPS with the Lua wrapper may be partially client-bound,
  so the storage-side gains are lower bounds there. Sequential cell
  ordering also adds some noise (the bench space grows during the
  run) — treat single-digit deltas as noise.

Reproduce: `test/perf/c_call/run.sh <tarantool-binary>` (env:
`BENCH_FIBERS`, `BENCH_DURATION`, `BENCH_WARMUP`).

Note: the C `replace`/`get` used above are USER-owned functions —
`user_funcs.c` in this directory, built into `bench_c.so` and
registered by `storage.lua` with `box.schema.func.create('bench_c.*',
{language = 'C'})`, exactly as a real user would. vshard's own .so
carries only `setup` and `call`; the dispatch finds user functions
through the `_func` registry (`box_func_call_by_name`), nothing is
hardcoded. (Earlier revisions of these tables used the then-built-in
names `vshard.storage_c.bench_*` — same code, same numbers.)

## Post-refactor note (Lua-delegated errors)

The C implementation was later simplified: on any off-fast-path event
(malformed request, failed bucket ref, function not in `_func`) the C
code re-executes the whole request through the Lua `storage_call`,
and when a registered function ran and *failed*, only the error
encoding is delegated (`lerror.make(box.error.last())` — full
`error:unpack()` parity). This removed the C error builders
(`verror.c`) entirely and erased the two previous divergences (box
errors were a `{type, code, message}` subset; missing buckets lacked
the `route_map` destination hint). Re-measured after the refactor:
C-function fast-path numbers are unchanged within noise (router get C
~126k, replace C ~97k at 10 fibers), and the C wrapper still beats
the Lua wrapper for Lua user functions (e.g. router replace Lua 96k
vs 53k in the same run) despite the request being re-executed in Lua
— run-to-run variance on a shared dev box exceeds the delegation
cost, so compare only cells from the same run.
