from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.common import Types, Row
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
import math
import json
from datetime import datetime
import logging
import time
import sys

def initialize():
    """
    logger = logging.getLogger("StreamIngestionChicago")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler('logs/streamingest.log')
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    
    The report could contain information for:

    * identification of tenant
    * pipeline component information
    * ingestion start time
    * ingestion end time
    * number of messages ingested from messaging system
    * average ingestion time/speed (messages/rows ingested per second)
    * total size of messages ingested
    * number of errors during ingestion
    """
    tenantId = "chicago"
    pipeline = "streamChicagoPipeline"
    startTime = time.time()
    endTime = startTime
    messageCount = 0
    ingestSpeed = 0
    totalSize = 0
    errorCount = 0
    execute()

def stop():
    endtime = time.time()
    #logger.



# Modify NaN values to Null/None, remove unneeded values
def transform(stream):
    jsonStream = json.loads(stream) # load the source string into json format

    for key,value in jsonStream.items():
        if isinstance(value, float) and math.isnan(value): # value is NaN, modify it to null
            jsonStream[key] = None

    # format timestamps
    timestampStart = datetime.strptime(jsonStream["Trip Start Timestamp"], '%Y-%m-%dT%H:%M:%SZ')
    timestampEnd = datetime.strptime(jsonStream["Trip End Timestamp"], '%Y-%m-%dT%H:%M:%SZ')

    # Create Flink Row from source, format ready for Cassandra insert
    # removed Pickup Census Tract, Dropoff Census Tract, Pickup Centroid Location, Dropoff Centroid Location
    filteredRow = Row(
            jsonStream["Pickup Community Area"],  
            jsonStream["Trip ID"],
            jsonStream["Company"],
            jsonStream["Dropoff Centroid Latitude"],
            jsonStream["Dropoff Centroid Longitude"],
            jsonStream["Dropoff Community Area"],
            jsonStream["Extras"],
            jsonStream["Fare"],
            jsonStream["Payment Type"],
            jsonStream["Pickup Centroid Latitude"],
            jsonStream["Pickup Centroid Longitude"],
            jsonStream["Taxi ID"],
            jsonStream["Tips"],
            jsonStream["Tolls"],
            timestampEnd,
            jsonStream["Trip Miles"],
            jsonStream["Trip Seconds"],
            timestampStart,
            jsonStream["Trip Total"]
            )
    print(filteredRow)
    return filteredRow

def execute():

    env = StreamExecutionEnvironment.get_execution_environment()
    # JARs of kafka (source) and Cassandra (sink) connectors
    env.add_jars(
        "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-sql-connector-kafka-3.4.0-1.20.jar",
        "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-connector-cassandra_2.12-3.2.0-1.19.jar",
        "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/opt/flink-python-1.20.1.jar"
        )
    #env.add_jars("file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-connector-cassandra_2.12-3.2.0-1.19.jar")
    #env.add_jars("file:///home/ilmarih/bdp_25_tech/flink-1.20.1/opt/flink-python-1.20.1.jar")
    # Kafka Source setup
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092,localhost:9093,localhost:9094") \
        .set_topics("chicago_taxiTrips", "chicago_ingestioncontrol") \
        .set_group_id("g1") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_properties({
         'fetch.max.wait.ms': '10000',
        }) \
        .build()
    
    # Input data stream
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    print("stream:")
    stream.print()
    #messageCount += 1
    

    totalSize = 0
    errorCount = 0

    # Process raw data
    processedStream = stream.map(
         lambda raw: transform(raw),
         output_type=Types.ROW([
            Types.FLOAT(),          # pickup_community_area
            Types.STRING(),         # trip_id
            Types.STRING(),         # company
            Types.DOUBLE(),         # dropoff_centroid_latitude
            Types.DOUBLE(),         # dropoff_centroid_longitude
            Types.FLOAT(),          # dropoff_community_area
            Types.FLOAT(),          # extras
            Types.FLOAT(),          # fare
            Types.STRING(),         # payment_type
            Types.DOUBLE(),         # pickup_centroid_latitude
            Types.DOUBLE(),         # pickup_centroid_longitude
            Types.STRING(),         # taxi_id
            Types.FLOAT(),          # tips
            Types.FLOAT(),          # tolls
            Types.SQL_TIMESTAMP(),  # trip_end_timestamp
            Types.FLOAT(),          # trip_miles
            Types.INT(),            # trip_seconds
            Types.SQL_TIMESTAMP(),  # trip_start_timestamp
            Types.FLOAT()           # trip_total
            ])
    )
    #totalSize += sys.getsizeof(processedStream)
    # Insert data into Cassandra Sink
    CassandraSink.add_sink(processedStream) \
        .set_host("localhost") \
        .set_query("INSERT INTO flink.trips (pickup_community_area, trip_id, company, \
            dropoff_centroid_latitude, dropoff_centroid_longitude, dropoff_community_area, extras, fare, \
            payment_type, pickup_centroid_latitude, pickup_centroid_longitude, taxi_id, tips, tolls, \
                trip_end_timestamp, trip_miles, trip_seconds, trip_start_timestamp, trip_total \
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") \
        .build()
    
    try:
        env.execute("Kafka Flink Cassandra pipeline")
    except Exception as e:
        print(f"Error while executing the Flink job: {e}")

if __name__ == "__main__":
    initialize()