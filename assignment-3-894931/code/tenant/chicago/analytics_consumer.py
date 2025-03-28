from confluent_kafka import Consumer
import argparse
from dotenv import load_dotenv
import time

# simple kafka consumer for simulating tenant consuming data quality alert from streaming application
if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-t', '--topic', default="chicagotenant_analytics", help='kafka topic')
    parser.add_argument('-g', '--consumer_group', default="g1", help='consumer group')

    args = parser.parse_args()
    broker=args.broker

    kafka_conf={
        'bootstrap.servers': broker,
        'group.id': args.consumer_group,
    }
        
    kafka_consumer = Consumer(kafka_conf)
    kafka_consumer.subscribe([args.topic])

    try:
        while True:
            msg = kafka_consumer.poll(5.0)
            current_time = time.time()
            
            if msg is None:
                continue
            print(msg.value())

    except KeyboardInterrupt:
        print("exiting")
    finally:
        kafka_consumer.close()