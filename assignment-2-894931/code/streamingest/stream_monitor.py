from confluent_kafka import Consumer
import argparse
import json
from dotenv import load_dotenv
import logging
import time

def stopIngest():
    end_time = time.time()
    logging.info(f"Stopped ingesting - statistics:")
    logging.info(f"Time taken: {end_time - start_time:.2f} s")
    logging.info(f"Number of Kafka messages received: {messagesReceived}")
    logging.info(f"Rows succesfully inserted: {rowsConsumed}")
    logging.info(f"Rows succesfully inserted / s: {rowsConsumed / (end_time - start_time)}")
    logging.info(f"Number of Cassandra errors: {cassandraError}")
    logging.info(f"Number of Kafka errors: {kafkaError}")
    logging.info("------------------------------------------------")
    exit(0)

if __name__ == '__main__':
    load_dotenv()
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-t', '--topics', default=["chicago_ingestion_report", "chicago_ingestion_report_warning"], help='kafka topic')
    parser.add_argument('-g', '--consumer_group', default="monitor", help='consumer group')
    parser.add_argument('--security_protocol', default='SASL_PLAINTEXT', help='security protocol')
    parser.add_argument('--sasl_mechanism', default='PLAIN', help='security protocol')
    parser.add_argument('--sasl_username', help='sasl user name')
    parser.add_argument('--sasl_password', help='sasl password')
    args = parser.parse_args()
    broker=args.broker

    #create configuration file for kafka connection
    kafka_conf={
        'bootstrap.servers': broker,
        'group.id': args.consumer_group,
    }
    
    topics = args.topics
    
    kafka_consumer = Consumer(kafka_conf)
    kafka_consumer.subscribe(topics)

    #logging.basicConfig(filename='ingest.log', level=logging.INFO,
     #               format='%(asctime)s - %(levelname)s - %(message)s')

    rowsDiscrded = 0

    print("Listening to pipeline reports")

    try:
        while True:
            # consume a message from kafka, wait 10 second
            msg = kafka_consumer.poll(10.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                continue
            
            if msg:
                topic = msg.topic()
                tenant = topic.split("_")[0]
                json_value =json.loads(msg.value().decode('utf-8'))
                print("tenant: ", tenant)
                print("msg: ", json_value)

    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        kafka_consumer.close()