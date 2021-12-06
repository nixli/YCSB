#!/bin/bash

#REDIS_TOP=${1:-../redis_csc2222/src/}
#NUM_THREADS=${2:-16}
REDIS_TOP="../redis_csc2222/src"
REDIS_CLI=$REDIS_TOP/redis-cli

#NUM_KEYS=$1
#NUM_FIELDS=$2
#FIELD_SIZE=$3
#echo "Number of keys: $1"
#echo "Number of fields per keys: $2"
#echo "Size of each field: $3"

#!!!!!!!!! change these plz
NUM_KEYS=100
NUM_FIELDS=110
FIELD_SIZE=280

BASE_REDIS="/home/kevinsong/Documents/fall2021/csc2222/project/redis-official/"
COMPRESS_REDIS="/home/kevinsong/Documents/fall2021/csc2222/project/redis_csc2222/"

WORKLOAD="workload_100keys_110fields_280bytesUniform"

NUM_THREADS="16"
#!!!!!!!!! change these plz end

LOGNAME="${NUM_KEYS}keys_${NUM_FIELDS}fields_${FIELD_SIZE}bytes_16threads"

#### Modify the YCSB workload

for iter in 4 5
do
    ##### Run ziplist
    $BASE_REDIS/src/redis-server $BASE_REDIS/redis.conf &
    
    sleep 1 # wait for redis server to go up
    
    $REDIS_CLI config set list-max-ziplist-size -3
    $REDIS_CLI config get list-max-ziplist-size
    
    ./bin/ycsb load redis -s -P workloads/$WORKLOAD -p "redis.host=127.0.0.1" -p "redis.port=6379" -threads $NUM_THREADS &> experiments/${LOGNAME}_ziplist_iter${iter}
    $REDIS_CLI info memory &>> experiments/${LOGNAME}_ziplist_iter${iter}
    ./bin/ycsb run  redis -s -P workloads/$WORKLOAD -p "redis.host=127.0.0.1" -p "redis.port=6379" -threads $NUM_THREADS &>> experiments/${LOGNAME}_ziplist_iter${iter}
    
    $REDIS_CLI shutdown
    
    ##### Run linked list
    $BASE_REDIS/src/redis-server $BASE_REDIS/redis.conf &
    
    sleep 1 # wait for redis server to go up
    
    $REDIS_CLI config set list-max-ziplist-size 1
    $REDIS_CLI config get list-max-ziplist-size
    
    ./bin/ycsb load redis -s -P workloads/$WORKLOAD -p "redis.host=127.0.0.1" -p "redis.port=6379" -threads $NUM_THREADS &> experiments/${LOGNAME}_linkedlist_iter${iter}
    $REDIS_CLI info memory &>> experiments/${LOGNAME}_linkedlist_iter${iter}
    ./bin/ycsb run  redis -s -P workloads/$WORKLOAD -p "redis.host=127.0.0.1" -p "redis.port=6379" -threads $NUM_THREADS &>> experiments/${LOGNAME}_linkedlist_iter${iter}
    
    $REDIS_CLI shutdown
    
    ##### Run compression
    $COMPRESS_REDIS/src/redis-server $COMPRESS_REDIS/redis.conf &
    
    sleep 1 # wait for redis server to go up
    
    $REDIS_CLI config set list-max-ziplist-size 1
    $REDIS_CLI config get list-max-ziplist-size
    
    ./bin/ycsb load redis -s -P workloads/$WORKLOAD -p "redis.host=127.0.0.1" -p "redis.port=6379" -threads $NUM_THREADS &> experiments/${LOGNAME}_compress_iter${iter}
    $REDIS_CLI info memory &>> experiments/${LOGNAME}_compress_iter${iter}
    ./bin/ycsb run  redis -s -P workloads/$WORKLOAD -p "redis.host=127.0.0.1" -p "redis.port=6379" -threads $NUM_THREADS &>> experiments/${LOGNAME}_compress_iter${iter}
    
    $REDIS_CLI shutdown
done



