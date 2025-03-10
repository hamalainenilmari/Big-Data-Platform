import argparse
from confluent_kafka import Producer
import pandas as pd
import json
import time
import datetime

# This component is from assignment 1

def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.isoformat() + "Z"
    

def date_parser(date_str):
    return pd.to_datetime(date_str, format="%m/%d/%Y %I:%M:%S %p")

'''
A common way to get the error if something is wrong with
the delivery
'''
def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localgost:9092", help='Broker as "server:port"')
    parser.add_argument('-i', '--input_file', default="../../../data/sample0.csv", help='Input file')
    parser.add_argument('-c', '--chunksize', default=10, help='chunk size for big file')
    parser.add_argument('-s', '--sleeptime', default=0, help='sleep time in second')
    parser.add_argument('-t', '--topic', default="chicago_taxitrips", help='kafka topic')
    parser.add_argument('--security_protocol', default='SASL_PLAINTEXT', help='security protocol')
    parser.add_argument('--sasl_mechanism', default='PLAIN', help='security protocol')
    parser.add_argument('--sasl_username', help='sasl user name')
    parser.add_argument('--sasl_password', help='sasl password')
    
    args = parser.parse_args()

    KAFKA_BROKER=args.broker
    INPUT_DATA_FILE=args.input_file
    chunksize=int(args.chunksize)
    sleeptime =int(args.sleeptime)
    KAFKA_TOPIC =args.topic
    #create configuration file for kafka connection
    if (args.sasl_username is None) and (args.sasl_password is None):
        kafka_conf={
            'bootstrap.servers': KAFKA_BROKER
        } 
    else:
        kafka_conf={
            'bootstrap.servers': KAFKA_BROKER,
            'security.protocol': args.security_protocol,
            'sasl.mechanism': args.sasl_mechanism,
            'sasl.username': args.sasl_username,
            'sasl.password': args.sasl_password
        }

    input_data =pd.read_csv(INPUT_DATA_FILE,parse_dates=['Trip Start Timestamp','Trip End Timestamp'],date_parser=date_parser,iterator=True,chunksize=chunksize)
    kafka_producer = Producer(kafka_conf)

    start_time = time.time()
    print(f"started producing input data at {start_time}")
    i = 0
    for chunk_data in input_data:
        '''
        now process each chunk
        '''
        #chunk=chunk_data.dropna()
        #print(f'DEBUG: Send data to Kafka - chuck: {i}')
        for index, row in chunk_data.iterrows():
            json_data=json.dumps(row.to_dict(), default=datetime_converter)
            #print(json_data)
            print(f"produced to topic: {KAFKA_TOPIC}")
            kafka_producer.produce(KAFKA_TOPIC, json_data.encode('utf-8'), callback=kafka_delivery_error)
            kafka_producer.flush()
            time.sleep(sleeptime)
        i += 1
    end_time = time.time()
    print(f"finished producing input data at {end_time}, total runtime: {end_time - start_time:.2f} s")