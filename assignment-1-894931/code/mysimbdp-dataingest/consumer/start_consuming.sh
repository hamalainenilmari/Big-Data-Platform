#!/bin/bash

# arg 1: number of concurrent kafka consumers

stop() {
  kill -SIGTERM $(jobs -p)
  wait
}

trap stop SIGINT

for i in $(seq 1 $1)
do
  python3 kafka_consumer.py &
done

wait
