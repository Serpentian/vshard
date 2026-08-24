#!/bin/sh
# Build the example user C functions (bench_c.so) against an
# uninstalled tarantool source tree. Usage:
#   ./build.sh [path-to-tarantool-src]
set -e
TNT_DIR="${1:-$HOME/Programming/tnt/tarantool_clean}"
DIR="$(cd "$(dirname "$0")" && pwd)"
CC="${CC:-cc}"
$CC -shared -fPIC -fvisibility=hidden -std=gnu99 -Wall -Wextra -O2 -g \
    -I"$TNT_DIR/src" \
    -I"$TNT_DIR/src/lib/msgpuck" \
    -I"$TNT_DIR/third_party/luajit/src" \
    -o "$DIR/bench_c.so" \
    "$DIR/user_funcs.c" \
    "$TNT_DIR/src/lib/msgpuck/libmsgpuck.a"
echo "built $DIR/bench_c.so"
