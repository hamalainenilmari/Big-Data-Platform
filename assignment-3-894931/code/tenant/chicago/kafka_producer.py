import argparse
from confluent_kafka import Producer
import pandas as pd
import json
import time
import datetime
import os
from dotenv import load_dotenv

from datetime import datetime, timedelta
import pandas as pd



# This component is from assignment 1
# This component can be used for simulating real time data producing

def exit():
    print("exiting ...")

def datetime_converter(dt):
    if isinstance(dt, (datetime, pd.Timestamp)):
        return dt.isoformat() + "Z"

def date_parser(date_str):
    return pd.to_datetime(date_str, format="%m/%d/%Y %I:%M:%S %p")

def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

#../../../../assignment-1-894931/data/Taxi_Trips__2024-__20250204.csv
if __name__ == '__main__':
    load_dotenv()

    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    kafka_add = os.getenv("KAFKA_CFG_ADVERTISED_LISTENERS") + ":9092"
    print("kafka add: ", kafka_add)
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default=kafka_add, help='Broker as "server:port"')
    parser.add_argument('-i', '--input_file', default="../../../data/chicago/small_sample.csv", help='Input file')
    parser.add_argument('-c', '--chunksize', default=1, help='chunk size for big file')
    parser.add_argument('-s', '--sleeptime', default=1, help='sleep time in second')
    parser.add_argument('-t', '--topic', default="chicagotenant_trips", help='kafka topic')
    
    args = parser.parse_args()

    KAFKA_BROKER=args.broker
    INPUT_DATA_FILE=args.input_file
    chunksize=int(args.chunksize)
    sleeptime =int(args.sleeptime)
    KAFKA_TOPIC =args.topic

    # create configuration file for kafka connection
    kafka_conf={
        'bootstrap.servers': KAFKA_BROKER
    } 

    input_data =pd.read_csv(INPUT_DATA_FILE,parse_dates=['Trip Start Timestamp','Trip End Timestamp'],date_parser=date_parser,iterator=True,chunksize=chunksize)
    kafka_producer = Producer(kafka_conf)

    start_time = time.time()
    
    print(f"started producing input data at {start_time}")
    for chunk_data in input_data:
        for index, row in chunk_data.iterrows():
            print(row.to_dict())
            # Get the current time
            current_time = datetime.utcnow()
            # Set the timestamps
            trip_end = pd.Timestamp(current_time).floor("S")  # Current time
            trip_start = pd.Timestamp(current_time - timedelta(minutes=15)).floor("S")  # 15 minutes before
            json_data = row.to_dict()
            json_data["Trip Start Timestamp"] = trip_start
            json_data["Trip End Timestamp"] = trip_end
            #json_data["Pickup Community Area"] = 76

            json_data=json.dumps(json_data, default=datetime_converter)
            print(json_data)
            kafka_producer.produce(KAFKA_TOPIC, json_data.encode('utf-8'), callback=kafka_delivery_error)
            print("produced")
            kafka_producer.flush()
            time.sleep(sleeptime)
            stopTime = time.time() - start_time
    
    end_time = time.time()
    print(f"finished producing input data at {end_time}, total runtime: {end_time - start_time:.2f} s")
    