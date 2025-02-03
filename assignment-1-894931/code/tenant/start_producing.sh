#!/bin/bash

for i in {0..9}
do
  python3 kafka_producer.py -i ../../data/sample$i.csv &
done

wait