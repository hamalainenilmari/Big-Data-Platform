from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common import Types, Row
from pyflink.datastream.functions import RuntimeContext
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import SimpleStringSchema, Encoder
import json
from datetime import datetime
import os
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, ReduceFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
import pytz
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, FsStateBackend
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import StreamingFileSink, OutputFileConfig, RollingPolicy
import math
import logging

# this function generates the aggregated analytical data of taxi trips per window
class TaxiAnalyticsProcessWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        # window information
        window = context.window()

        # all records of this key
        elements_list = list(elements)
        
        # log errors of input data
        num_pickup_area_errors = 0
        num_timestamp_errors = 0
        cleansed_elements = []

        for element in elements_list:
            data = json.loads(element) # load input string to json

            # Extract Community area
            community_area = data.get("Pickup Community Area")
            if community_area is None or (isinstance(key, float) and math.isnan(key)) or key == 'NaN' or key == 'nan':
                print(f"Skipping element due to missing pickup communnity area")
                num_pickup_area_errors += 1
                continue

            # Extract Trip Start Timestamp
            timestamp = data.get("Trip Start Timestamp")
            if timestamp is None or (isinstance(timestamp, float) and math.isnan(timestamp)) or timestamp == 'NaN' or timestamp == 'nan':
                print(f"Skipping element due to invalid trip start timestamp")
                num_timestamp_errors += 1
                continue
            
            # Extract total fare of trip
            total_price = data.get("Trip Total")
            if total_price is None or total_price == 'NaN':
                # no need to skip value
                total_price = 0.0
            
            # if good data, add to analytics list
            cleansed_elements.append((community_area, timestamp, total_price))

        # side output for analytics processing metrics
        metrics_output_tag = OutputTag("metrics", Types.TUPLE([Types.LONG(), Types.INT(), Types.INT(), Types.INT(), Types.INT()]))

        # numbers of total records of input data, rows matching enforced schema and rows discarded due to schema mismatch
        total_row_count = len(elements_list) or 0
        good_row_count = len(cleansed_elements) or 0
        discarded_row_count = total_row_count - good_row_count

        # side output for window start timestmap, number of rows per window per key, number of discarded rows,
        # relation of good rows per bad rows (data quality), number of pickup area errors, 
        # number of timestamp errors
        yield metrics_output_tag, (window.start, total_row_count, discarded_row_count, num_pickup_area_errors, num_timestamp_errors)
        
        if not elements_list or len(elements_list) == 0:
            # should not happen
            print("empty list")
            return
        
        if key is None or (isinstance(key, float) and math.isnan(key)) or key == 'NaN' or key == 'nan':
            return
        
        # Aggregate total number of trips and prices
        trip_count = len(cleansed_elements)
        total_price = sum(elem[2] for elem in cleansed_elements)
        
        if total_price is None or total_price == 'NaN':
            print("error with total price")
            total_price = 0

        # final aggregated data of this window
        result = Row(key,trip_count,total_price,window.start,window.end)
        yield result


# parse time to correct format for assigning correct format timestamp to record for windowing
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
            data = json.loads(element)
            ts = data.get("Trip Start Timestamp")
            timestamp = int(parse_datetime(ts).timestamp() * 1000)
            return timestamp
        except Exception as e:
            print(f"Timestamp extraction error: {e}")
            return 0


def execute():
    # set stream execution configurations
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # TODO check this
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    env.enable_checkpointing(20000, CheckpointingMode.EXACTLY_ONCE) # TODO check this
    #env.set_state_backend(FsStateBackend("hdfs://localhost:9000/flink/checkpoints"))

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
    """
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
    """
    log_sink = StreamingFileSink.for_row_format(
        base_path="/home/ilmarih/bdp_25/assignment-3-894931/logs",  # HDFS base directory
        encoder=Encoder.simple_string_encoder()
    ) \
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_suffix(".log")
        .build()) \
    .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy()) \
    .build()
    # tenant input streaming data from kafka
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # assign timestamp for windowing
    stream = stream.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
            .with_timestamp_assigner(AssignTimestampAssigner())
            )
    
    # generate analytics in tumbling windows
    windowed_stream = (stream
        .filter(lambda x: x is not None)                        # Filter out None values
        .key_by(lambda x: json.loads(x).get("Pickup Community Area"))                                 # Key by community area
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))  # tumbling window TODO check this
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
    metrics_output_tag = OutputTag("metrics", Types.TUPLE([Types.LONG(), Types.INT(), Types.INT(), Types.INT(), Types.INT()]))
    metrics_stream = windowed_stream.get_side_output(metrics_output_tag)
    
    # sum each keyed data metrics to get total metrics of window
    summed_metrics = (
            metrics_stream
            .key_by(lambda x: 1)  # Single key for global sum
            .window(TumblingEventTimeWindows.of(Time.seconds(30)))  # Same windowing
            .reduce(lambda a, b: (a[0], a[1]+b[1], a[2]+b[2], a[3]+b[3], a[4]+b[4]),
                    output_type=Types.TUPLE([Types.LONG(), Types.INT(), Types.INT(), Types.INT(), Types.INT()]))  # Sum up all counts
            )
    
    # transform the metrics into log format
    log_stream = summed_metrics.map(lambda row: 
        f"window: {row[0]}\nrecords per window: {row[1]}\n" \
        + f"records processed per second: {row[1]/30}\nrows discarded: {row[2]}\n" \
        + f"data quality: {1 - row[2]/row[1]}\npickup area errors: {row[3]}\ntimestamp errors: {row[4]}",\
    Types.STRING())
    
    # write to log file
    log_stream.add_sink(log_sink)
    
    # Add the sink to the stream
    csv_stream = windowed_stream.map(lambda row: f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}", Types.STRING())
    #csv_stream.add_sink(hdfs_sink)

    try:
        res = env.execute("FlinkSilverDataAnalyticsPipeline")
        current_job = res
        job_id = res.get_job_id()
        print("Flink job id: ", job_id)
    except Exception as e:
        print(f"Error while executing the Flink job: {e}")

if __name__ == "__main__":
    load_dotenv()
    execute()