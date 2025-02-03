#!/bin/bash

for i in $(seq 0 $1)
do
  python3 kafka_producer.py -b $2 -i ../../data/sample$i.csv &
done

#wait
