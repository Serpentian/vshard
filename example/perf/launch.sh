#!/bin/sh

ITERATIONS=3
WORKERS=3

# Parse options
while getopts "i:w:" opt; do
    case $opt in
        i) ITERATIONS=$OPTARG ;;
        w) WORKERS=$OPTARG ;;
        *) echo "Usage: $0 [-i iterations] [-w workers]"; exit 1 ;;
    esac
done

total_sum=0

for ((i=1; i<=ITERATIONS; i++)); do
    echo "Round $i starting..."
    for ((w=1; w<=WORKERS; w++)); do
        tarantool generate_load.lua \
            --bucket_count 30000 \
            --op_type replace \
            --warmup \
            --uri localhost:3305,localhost:3306,localhost:3307,localhost:3308,localhost:3309,localhost:3310 \
            --fibers 50 \
            --ops 1000000 \
            --output load${w}.txt &
    done
    wait

    for ((w=1; w<=WORKERS; w++)); do
        # Get last line and extract the number (assuming it's just a number on the line)
        last_line=$(tail -n 1 load${w}.txt)
        value=$(echo "$last_line" | grep -o '[0-9.]\+')
        total_sum=$(echo "$total_sum + $value" | bc)
    done
done

average=$(echo "scale=4; $total_sum / $ITERATIONS" | bc)
echo "Average per iteration: $average"
