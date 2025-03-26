from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.common import Types, Row
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import OutputTag, DataStream
from pyflink.datastream import MapFunction
import math
import json
from datetime import datetime
import time
import os
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
import threading
from cassandra.cluster import Cluster
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, Duration, TimestampAssigner
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
from pyflink.datastream.functions import AggregateFunction, ReduceFunction, WindowFunction
from pyflink.datastream.window import TimeWindow
from typing import Iterable
import pytz
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.types import DataTypes
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, RocksDBStateBackend, StateBackend, FsStateBackend
#from pyflink.datastream.connectors import StreamingFileSink
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import StreamingFileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors import BucketAssigner
from pyflink.table.descriptors import Schema

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
             jsonStream["Trip End Timestamp"],
             #timestampEnd,
             jsonStream["Trip Miles"],
             jsonStream["Trip Seconds"],
             jsonStream["Trip Start Timestamp"],
             #timestampStart,
             jsonStream["Trip Total"]
             )
     return filteredRow
 
def checkPrimaryKeys(stream):
     global incorrectRowsAmount
     try:
         jsonStream = json.loads(stream) # load the source string into json format
         if isinstance(jsonStream["Pickup Community Area"], float) and math.isnan(jsonStream["Pickup Community Area"]): 
             # Primary key is NaN, discard input
             return False
         elif isinstance(jsonStream["Trip ID"], float) and math.isnan(jsonStream["Trip ID"]):
             return False
         else:
             # valid data
             return True
     except json.JSONDecodeError as e:
         print(f"Json decoding error: {e}")


class TaxiAnalyticsProcessWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):

        # all records of this key
        elements_list = list(elements)

        if not elements_list:
            return
        
        # window information
        window = context.window()
        
        # Aggregate total number of trips and price
        trip_count = len(elements_list)
        total_price = sum(elem[1] for elem in elements_list)

        result = Row(key,trip_count,total_price,window.start,window.end)
        yield result


def parse_datetime(iso_string):
    try:
        if isinstance(iso_string, str):
            if iso_string.endswith('Z'):
                return datetime.fromisoformat(iso_string.replace("Z", "+00:00")).astimezone(pytz.UTC)
            return datetime.fromisoformat(iso_string).astimezone(pytz.UTC)
        return datetime.now(pytz.UTC)
    except Exception as e:
        print(f"Datetime parsing error: {e} for input {iso_string}")
        return datetime.now(pytz.UTC)
    
class AssignTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        try:
            timestamp = int(parse_datetime(element[1]).timestamp() * 1000)
            return timestamp
        except Exception as e:
            print(f"Timestamp extraction error: {e}")
            return 0
        

def cleanse_analytics(element):
    try:
        # Parse the JSON string
        data = json.loads(element)
        
        # Extract Pickup Community Area
        community_area = data.get("Pickup Community Area")
        if community_area is None or community_area == 'NaN':
            print(f"Skipping element due to missing pickup communnity area: {element}")
            return None
        
        # Extract Trip Start Timestamp
        timestamp = data.get("Trip Start Timestamp")
        if not timestamp or timestamp == 'NaN':
            print(f"Skipping element due to invalid trip start timestamp: {element}")
            return None
        
        # Extract total fare of trip
        total_price = data.get("Trip Total")
        if total_price is None or total_price == 'NaN':
            # no need to skip value
            total_price = 0.0
        
        result = (community_area, total_price)
        return result
    except Exception as e:
        print(f"Error while cleansing element: {element}, Error: {e}")
        return None


def execute():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    env.enable_checkpointing(20000, CheckpointingMode.EXACTLY_ONCE)
    env.set_state_backend(FsStateBackend("hdfs://localhost:9000/flink/checkpoints"))

    # JARs of kafka (source) and Cassandra (sink) connectors
    kafka_jar = os.getenv("KAFKA_JAR")
    cassandra_jar = os.getenv("CASSANDRA_JAR")
    python_jar = os.getenv("PYTHON_JAR")

    env.add_jars(kafka_jar, cassandra_jar, python_jar,
                    "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-shaded-hadoop-2-uber-2.7.5-9.0.jar",
                    "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-hadoop-fs-1.19.0.jar",
                     "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/hadoop-common-3.4.1.jar",
                     "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-hadoop-compatibility_2.12-1.20.1.jar",
                     "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/hadoop-hdfs-3.3.6.jar",
                     "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/hadoop-hdfs-client-3.3.6.jar",
                     "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/hadoop-auth-3.3.6.jar",
                     "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/hadoop-core-1.2.1.jar",
                     "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-connector-cassandra_2.12-3.2.0-1.19.jar"
                     )
    
    # Kafka Source for input data
    kafka_ip = os.getenv("KAFKA_BROKER_ADDRESS")
    kafka_add = f"{kafka_ip}:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_add) \
        .set_topics("chicagotenant_trips") \
        .set_group_id("g1") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_properties({
        'fetch.max.wait.ms': '100',
        }) \
        .build()
    
    # HDFS sink for silver data, this will create a new output file for each window
    hdfs_sink = StreamingFileSink.for_row_format(
        base_path="hdfs://localhost:9000/chicagoTenant/silverData",  # HDFS base directory
        encoder=Encoder.simple_string_encoder()
    ) \
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_suffix(".csv")
        .build()) \
    .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy()) \
    .build()
    
    # tenant input streaming data from kafka
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # cleanse input data; skip if missing key values
    stream = stream.map(cleanse_analytics)

    # assign timestamp for windowing
    stream = stream.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
            .with_timestamp_assigner(AssignTimestampAssigner())
            )
    
    
    # generate analytics in tumbling windows
    windowed_stream = (stream
        .filter(lambda x: x is not None)                        # Filter out None values
        .key_by(lambda x: x[0])                                 # Key by community area
        .window(TumblingEventTimeWindows.of(Time.seconds(20)))  # tumbling window TODO check this
        .process(                                               # Aggregate number of trips per area & total fares
            TaxiAnalyticsProcessWindowFunction(),
            output_type=Types.ROW([
                Types.INT(),                                    # community area
                Types.INT(),                                    # number of trips per window
                Types.FLOAT(),                                  # total fares
                Types.LONG(),                                   # window start timestamp
                Types.LONG()                                    # window end timestamp
                ]))
    )

    windowed_stream.print()

    # Add the sink to the stream
    csv_stream = windowed_stream.map(lambda row: f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}", Types.STRING())
    csv_stream.add_sink(hdfs_sink)

    """
    stream = stream.map(lambda x: to_tuple(x))
    stream.print()


    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())
    #for_bounded_out_of_orderness(Duration.of_seconds(10)) \
    
    stream = stream.assign_timestamps_and_watermarks(watermark_strategy)

    windowed_stream = (
        stream
        .key_by(lambda x: x[0])  # Key by Trip ID
        .window(TumblingEventTimeWindows.of(Time.seconds(20)))  # Apply 60s event time window
        .reduce(SumReduceFunction())  # Reduce by summing values
    )
    windowed_stream.print()  
    """
    #stream.print()

    #stream.print()

    #filteredStream = stream.filter(checkPrimaryKeys)
    #filteredStream.print()
    """
    processedStream = filteredStream.map(
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
            Types.LONG(),  # trip_end_timestamp
            Types.FLOAT(),          # trip_miles
            Types.INT(),            # trip_seconds
            Types.LONG(),  # trip_start_timestamp
            Types.FLOAT()           # trip_total
         ]))
    
    processedStream.print()
    """
    
    
    
    """
    filteredStream = stream.filter(checkPrimaryKeys)
    
    processedStream = filteredStream.map(
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
         ]))
    
    #processedStream.print()
    
   """
    try:
        res = env.execute("Kafka Flink Cassandra pipeline")
        current_job = res
        job_id = res.get_job_id()
        print("Flink job id: ", job_id)
    except Exception as e:
        print(f"Error while executing the Flink job: {e}")

if __name__ == "__main__":
    load_dotenv()

    execute()