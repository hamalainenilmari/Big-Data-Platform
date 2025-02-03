'''
We use python client library from https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html.
Also see https://github.com/confluentinc/confluent-kafka-python
'''
from confluent_kafka import Consumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
import argparse
import json
from dotenv import load_dotenv
import os
import logging
import time

if __name__ == '__main__':
    load_dotenv()
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-t', '--topic', default="taxiTrips", help='kafka topic')
    parser.add_argument('-g', '--consumer_group', default="g1", help='kafka topic')
    parser.add_argument('--security_protocol', default='SASL_PLAINTEXT', help='security protocol')
    parser.add_argument('--sasl_mechanism', default='PLAIN', help='security protocol')
    parser.add_argument('--sasl_username', help='sasl user name')
    parser.add_argument('--sasl_password', help='sasl password')
    args = parser.parse_args()
    broker=args.broker
    #create configuration file for kafka connection
    if (args.sasl_username is None) and (args.sasl_password is None):
        kafka_conf={
            'bootstrap.servers': broker,
            'group.id': args.consumer_group,
        }
    else:
        kafka_conf={
            'bootstrap.servers': broker,
            'group.id': args.consumer_group,
            'security.protocol': args.security_protocol,
            'sasl.mechanism': args.sasl_mechanism,
            'sasl.username': args.sasl_username,
            'sasl.password': args.sasl_password
        }
        
    kafka_consumer = Consumer(kafka_conf)
    kafka_consumer.subscribe([args.topic])

    keyspace = os.getenv("CASSANDRA_KEYSPACE")
    table = os.getenv("CASSANDRA_TABLE")
    ip = os.getenv("CASSANDRA_IP")
    cluster = Cluster(
        [ip]
    )
    session = cluster.connect(keyspace)
    session.default_consistency_level = ConsistencyLevel.QUORUM # set the consistency for session

    logging.basicConfig(filename='ingest.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


    rowsConsumed = 0
    fails = 0

    print("Starting to ingest data")
    logging.info(f"Starting to ingest data")
    start_time = time.time()
    inactivity_timeout = 60  # timeout in seconds to end ingesting
    last_message_time = time.time()
    try:
        while True:
            # consume a message from kafka, wait 1 second
            msg = kafka_consumer.poll(1.0)
            current_time = time.time()
            if msg is None:
                if current_time - last_message_time > inactivity_timeout:
                    logging.info(f"No new messages for {inactivity_timeout} seconds. Stopping ingestion.")
                    break
                continue
            if msg.error():
                logging.error(f"KAFKA ERROR: Exception during kafka consuming: {msg.error()}")
                print(f'Consumer error: {msg.error()}')
                continue

            json_value =json.loads(msg.value().decode('utf-8'))

            keysToDelete = ["Pickup Census Tract", "Dropoff Census Tract", "Pickup Centroid Location", "Dropoff Centroid  Location"]
            for key in keysToDelete:
                if key in json_value:
                    del json_value[key]

            columns = []
            # turn keys from 'key example' to 'key_example'
            for key in json_value.keys():
                columns.append(key.lower().replace("  ", "_").replace(" ", "_"))
            
            columns = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(json_value))
            try:
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                statement = SimpleStatement(query)
                
                result = session.execute(statement, list(json_value.values()))
                rowsConsumed += 1
            except Exception as e:
                logging.error(f"CASSANDRA ERROR: Exception during database insert: {e}\ndata row: {json_value}")
                fails += 1
                print(f"fail: {e}\nrow: {json_value}")
    except KeyboardInterrupt:
        end_time = time.time()
        logging.info(f"Stopped ingesting - statistics:")
        logging.info(f"Time taken: {end_time - start_time:.2f} s")
        logging.info(f"Rows succesfully inserted: {rowsConsumed}")
        logging.info(f"Number of exceptions: {fails}")
        logging.info("------------------------------------------------")
        exit(0)
    finally:
        kafka_consumer.close()