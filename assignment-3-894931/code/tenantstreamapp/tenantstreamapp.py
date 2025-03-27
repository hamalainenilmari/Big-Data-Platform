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
from pyflink.datastream.functions import ProcessWindowFunction
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

class TaxiAnalyticsProcessWindowFunction(ProcessWindowFunction):
    def __init__(self):
        self.row_count_state = None
        self.row_count_side_output_tag = OutputTag("row_count_tag", Types.LONG())

    def open(self, runtime_context: RuntimeContext):
        # initialize counts
        count_descriptor = ValueStateDescriptor("row_count", Types.LONG())
        self.row_count_state = runtime_context.get_state(count_descriptor)

    def process(self, key, context, elements):
        # all records of this key
        elements_list = list(elements)

        #row_count_side_output_tag = OutputTag("row_count_tag", Types.LONG())

        current_count = self.row_count_state.value() or 0
        row_count = current_count + len(elements_list)
        self.row_count_state.update(row_count)

        #print("row count: ", self.row_count_state.value())
        yield self.row_count_side_output_tag, row_count
        #context.side_output()
        
        if not elements_list or len(elements_list) == 0:
            print("empty list")
            return
        
        if key is None or (isinstance(key, float) and math.isnan(key)) or key == 'NaN' or key == 'nan':
            print("error with key")
            return

        # window information
        window = context.window()
        #context.output(row_count_side_output_tag, row_count)
        
        # Aggregate total number of trips and price
        trip_count = len(elements_list)
        total_price = sum(elem[2] for elem in elements_list)

        if total_price is None or total_price == 'NaN':
            print("error with total price")
            total_price = 0

        result = Row(key,trip_count,total_price,window.start,window.end)
        yield result

        #yield row_count_side_output_tag, "something"


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
        
        result = (community_area, timestamp, total_price)
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

    logFile = f"../../logs/_stream_analytics.log"
    logger = logging.getLogger(f"chicago")
    logger.setLevel(logging.INFO)
    
    fileHandler = logging.FileHandler(logFile)
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fileHandler)
   
    logger.info(f"Starting stream analytics ...")

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
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))  # tumbling window TODO check this
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
    row_count_side_output_tag = OutputTag("row_count_tag", Types.LONG())

    row_count_side_stream = windowed_stream.get_side_output(row_count_side_output_tag)

    #row_count_side_stream.print()
    summed = row_count_side_stream.key_by(lambda x: 1).sum()
    summed.print()
    #windowed_stream.print()

    # Add the sink to the stream
    csv_stream = windowed_stream.map(lambda row: f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}", Types.STRING())
    csv_stream.add_sink(hdfs_sink)
    


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