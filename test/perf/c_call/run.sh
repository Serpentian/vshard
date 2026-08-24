#!/bin/sh
# Benchmark of the C storage.call prototype, through the router.
# Usage: run.sh [path-to-tarantool]
set -e
TNT="${1:-tarantool}"
DIR="$(cd "$(dirname "$0")" && pwd)"
VSHARD="$(cd "$DIR/../../.." && pwd)"
WORK="${BENCH_WORK:-/tmp/vshard_c_bench}"

rm -rf "$WORK"
mkdir -p "$WORK/storage" "$WORK/router"
export LUA_PATH="$VSHARD/?.lua;$VSHARD/?/init.lua;;"
export LUA_CPATH="$VSHARD/?.so;$DIR/?.so;;"

[ -f "$DIR/bench_c.so" ] || "$DIR/build.sh"

echo "# tarantool: $($TNT --version | head -1)"
(cd "$WORK/storage" && VSHARD_C_CALL=1 "$TNT" "$DIR/storage.lua" \
    > storage.log 2>&1 &)
for i in $(seq 1 60); do
    [ -f "$WORK/storage/storage.pid" ] && break
    sleep 0.5
done
if ! [ -f "$WORK/storage/storage.pid" ]; then
    echo "storage failed to start:" >&2
    tail -20 "$WORK/storage/storage.log" >&2
    exit 1
fi

(cd "$WORK/router" && VSHARD_C_CALL= "$TNT" "$DIR/bench.lua" 2>/dev/null)
echo
(cd "$WORK/router" && VSHARD_C_CALL=1 "$TNT" "$DIR/bench.lua" 2>/dev/null)

kill "$(cat "$WORK/storage/storage.pid")" 2>/dev/null || true
