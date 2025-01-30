import argparse
from confluent_kafka import Producer
import pandas as pd
import json
import time
import datetime

# this script is based on course material

def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.__str__()
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
    parser.add_argument('-b', '--broker', default="34.88.140.181:9092", help='Broker as "server:port"')
    parser.add_argument('-i', '--input_file', help='Input file')
    parser.add_argument('-c', '--chunksize', help='chunk size for big file')
    parser.add_argument('-s', '--sleeptime', help='sleep time in second')
    parser.add_argument('-t', '--topic', help='kafka topic')
    parser.add_argument('--security_protocol', default='SASL_PLAINTEXT', help='security protocol')
    parser.add_argument('--sasl_mechanism', default='PLAIN', help='security protocol')
    parser.add_argument('--sasl_username', help='sasl user name')
    parser.add_argument('--sasl_password', help='sasl password')
    
    args = parser.parse_args()
    '''
    Because the KPI file is big, we emulate by reading chunk, using iterator and chunksize
    '''
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
    '''
    we read data by chunk so we can handle a big sample data file
    '''
    input_data =pd.read_csv(INPUT_DATA_FILE,parse_dates=['Trip Start Timestamp','Trip End Timestamp'],iterator=True,chunksize=chunksize)
    kafka_producer = Producer(kafka_conf)
    for chunk_data in input_data:
        '''
        now process each chunk
        '''
        chunk=chunk_data.dropna()
        for index, row in chunk.iterrows():
            '''
            Assume that when some data is available, we send it to Kafka in JSON
            '''
            json_data=json.dumps(row.to_dict(), default=datetime_converter)
            #check if any event/error sent
            print(f'DEBUG: Send data to Kafka')
            kafka_producer.produce(KAFKA_TOPIC, json_data.encode('utf-8'), callback=kafka_delivery_error)
            kafka_producer.flush()
            # sleep a while, if needed as it is an emulation
            time.sleep(sleeptime)