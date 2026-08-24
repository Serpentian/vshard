# How to run the C storage.call benchmark

Measures the storage-side request path: the Lua `vshard.storage.call`
against the C `vshard.storage_c.call`, each with a C and with a Lua
user function, through the router and directly to the storage.

## 1. Prerequisites: a patched tarantool

The C implementation dispatches user functions through
`box_func_call_by_name()`, a public module API function added by a core
patch (`src/box/box.h`, `src/box/box.cc`, `extra/exports`). Any
tarantool used to *run* the benchmark must export it:

```sh
nm -D <tarantool-binary> | grep box_func_call_by_name
```

Use a release build for measurements — a Debug build is dominated by
assertions and produces meaningless numbers.

## 2. Build the modules

```sh
# vshard's own module: vshard/storage_c.so
vshard/storage_c/build.sh [path-to-tarantool-src]

# the example user functions used by the bench: bench_c.so
test/perf/c_call/build.sh [path-to-tarantool-src]
```

Both default to `$HOME/Programming/tnt/tarantool_clean`. The path is
only used for headers (`src/module.h`, msgpuck, luajit) and for linking
`libmsgpuck.a` statically, so it does not have to be the same tree as
the binary you run — any source tree of a compatible version works.
`run.sh` builds `bench_c.so` itself if it is missing; the vshard
module it does not, build it once by hand.

## 3. Run

```sh
test/perf/c_call/run.sh <tarantool-binary>
```

It starts one storage process (always with `VSHARD_C_CALL=1`, so the
bucket refs live in C in both series) and runs the bench twice from an
in-process router: once with the Lua wrapper and once with the C one.

Environment:

| variable | default | meaning |
|---|---|---|
| `BENCH_FIBERS` | 10 | concurrent request fibers |
| `BENCH_DURATION` | 10 | measured seconds per cell |
| `BENCH_WARMUP` | 2 | warmup seconds before measuring |
| `BENCH_WORK` | `/tmp/vshard_c_bench` | working directory of the instances |

Example — a longer run with more concurrency:

```sh
BENCH_FIBERS=50 BENCH_DURATION=30 \
    test/perf/c_call/run.sh ~/Programming/tnt/tarantool/src/tarantool
```

## 4. Reading the output

Each row is one workload: `router *` goes through
`vshard.router.call`, `direct *` calls the storage over net.box
directly (isolates the storage-side cost). The `C`/`Lua` suffix is the
language of the *user* function. Columns: RPS, then p50/p95/p99 in
microseconds, then the error count (must be 0).

Run-to-run variance on a loaded machine is tens of percent, and the
client is a single process, so cells above ~150k RPS may be
client-bound. Compare cells within one run, and repeat the run before
trusting a small delta. Measured results are in `RESULTS.md`.

## 5. Cleanup

The storage process is killed by `run.sh` on exit. If a run was
interrupted, check for leftovers:

```sh
pkill -f 'c_call/storage.lua'
rm -rf /tmp/vshard_c_bench
```
