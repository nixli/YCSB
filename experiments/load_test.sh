#!/bin/bash

REDIS_TOP=${1:-../redis/src/}
REDIS_CLI=$REDIS_TOP/redis-cli

$REDIS_CLI flushall && \
    $REDIS_CLI config set list-max-ziplist-size -3  && \
    ./bin/ycsb load redis -s -P workloads/workload_debug -p "redis.host=127.0.0.1" -p "redis.port=6379"   -threads 16 && 
    $REDIS_CLI info memory

$REDIS_CLI flushall && \
    $REDIS_CLI config set list-max-ziplist-size 1  && \
    ./bin/ycsb load redis -s -P workloads/workload_debug -p "redis.host=127.0.0.1" -p "redis.port=6379"   -threads 16 && 
    $REDIS_CLI info memory
