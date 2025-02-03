#!/bin/bash

for i in $(seq 0 $1)
do
  python3 kafka_consumer.py &
done

#wait
