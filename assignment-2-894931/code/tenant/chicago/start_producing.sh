#!/bin/bash

# arg 1: number of concurrent producers
# arg 2: kafka broker ip:port (e.g. localhost:9092)

stop() {
  kill -SIGTERM $(jobs -p)
  wait
}

trap stop SIGINT
for i in $(seq 1 $1)
do
  python3 kafka_producer.py -b $2 -i ../../../data/sample$i.csv &
done

wait