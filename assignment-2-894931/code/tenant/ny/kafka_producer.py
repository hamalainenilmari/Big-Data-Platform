import argparse
from confluent_kafka import Producer
import pandas as pd
import json
import time
import datetime
import pyarrow.parquet as pq

# This component is from assignment 1

# This component can be used for simulating real time data producing with ny taxi

def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.isoformat() + "Z"

def date_parser(date_str):
    return pd.to_datetime(date_str, format="%m/%d/%Y %I:%M:%S %p")

def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-i', '--input_file', default="../../../data/ny/yellow_tripdata_2024-01.parquet", help='Input file')
    parser.add_argument('-c', '--chunksize', default=5, help='chunk size for big file')
    parser.add_argument('-s', '--sleeptime', default=1, help='sleep time in second')
    parser.add_argument('-t', '--topic', default="nytenant_trips", help='kafka topic')
    
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
    parquet_file = pq.ParquetFile(INPUT_DATA_FILE)
    input_data = pd.read_parquet(INPUT_DATA_FILE, engine="pyarrow")
    kafka_producer = Producer(kafka_conf)

    start_time = time.time()
    print(f"started producing input data at {start_time}")
    i = 0
    
    for batch in parquet_file.iter_batches(batch_size=chunksize):
        df_chunk = batch.to_pandas()  # Convert to Pandas DataFrame
        json_records = df_chunk.to_dict(orient="records")  # Convert to JSON format (list of dicts)
        # Send each JSON record to Kafka
        for record in json_records:
            record["tpep_pickup_datetime"] = datetime_converter(record["tpep_pickup_datetime"])
            record["tpep_dropoff_datetime"] = datetime_converter(record["tpep_dropoff_datetime"])
            json_data=json.dumps(record)
            print(record)
            kafka_producer.produce(KAFKA_TOPIC, json_data.encode('utf-8'), callback=kafka_delivery_error)
            kafka_producer.flush()
            time.sleep(sleeptime)
    
        i += 1
    end_time = time.time()
    
    print(f"finished producing input data at {end_time}, total runtime: {end_time - start_time:.2f} s, \
          total number of rows: {chunksize * i}")
    