from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.common import Types, Row
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import OutputTag
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

# event to stop pipeline exeution running as thread
stop_event = threading.Event()
# current flink job
current_job = None

class MetricsCollector(KeyedProcessFunction):
    def __init__(self, report_interval_ms=10000):
        self.report_interval_ms = report_interval_ms
        self.row_count_state = None
        self.discarded_count_state = None
        self.timer_registered_state = None
        
    def open(self, runtime_context: RuntimeContext):
        # initialize counts
        count_descriptor = ValueStateDescriptor("row_count", Types.LONG())
        self.row_count_state = runtime_context.get_state(count_descriptor)

        discarded_count_descriptor = ValueStateDescriptor("discarded_count", Types.LONG())
        self.discarded_count_state = runtime_context.get_state(discarded_count_descriptor)
        
        timer_descriptor = ValueStateDescriptor("timer_registered", Types.BOOLEAN())
        self.timer_registered_state = runtime_context.get_state(timer_descriptor)
        
    def process_element(self, value, ctx):
        # aggregate row count
        current_count = self.row_count_state.value()
        if current_count is None:
            current_count = 0
        self.row_count_state.update(current_count + 1)
        
        # timer for generating metrics in 10 second interval
        is_timer_registered = self.timer_registered_state.value()
        if is_timer_registered is None or is_timer_registered is False:
            ctx.timer_service().register_processing_time_timer(
                ctx.timer_service().current_processing_time() + self.report_interval_ms)
            self.timer_registered_state.update(True)

        data = value[1]

        try:
            if not self.check_primary_keys(data):
                # unvalid data, log it
                current_discarded_count = self.discarded_count_state.value()
                if current_discarded_count is None:
                    current_discarded_count = 0
                self.discarded_count_state.update(current_discarded_count + 1)
            else:
                # valid data
                transformed = self.transform(data)
                # return a tuple with 0 meaning that contains actual data, then the data, and empty string for metrics
                yield (0, transformed, "")
        except Exception as e:
            error_msg = f"Error processing record: {str(e)}"
            print(error_msg)
        
    def on_timer(self, timestamp, ctx):
        # generate metrics
        current_count = self.row_count_state.value()
        current_discarded_count = self.discarded_count_state.value()

        if current_count is None:
            current_count = 0
        if current_discarded_count is None:
            current_discarded_count = 0

        metrics = {
            "tenant_id": "chicagotenant",
            "timestamp": datetime.now().isoformat(),
            "rows_processed": current_count,
            "rows_per_second": current_count * 1000 / self.report_interval_ms,
            "discarded_rows": current_discarded_count,
        }
        
        # create empty data row because we are generating metrics, but to comply with flink data format
        emptyRow = Row(0.0, "", "", 0.0, 0.0, 0.0, 0.0, 0.0, "", 0.0, 0.0, "", 0.0, 0.0, datetime(1970, 1, 1),\
                        0.0, 0, datetime(1970, 1, 1), 0.0)
        
        # 1 means that this data is metrics, not actual ingested data
        yield (1, emptyRow, json.dumps(metrics))

        # reset counters
        self.row_count_state.update(0)
        self.discarded_count_state.update(0)
        
        # register next timer for 10 seconds later
        ctx.timer_service().register_processing_time_timer(timestamp + self.report_interval_ms)
    
    def check_primary_keys(self, stream):
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

    def transform(self, stream):
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
        return filteredRow
    
class MapToDataRowFunction(MapFunction):
    def map(self, value):
        return value[1]
    
class MapToMetricsRowFunction(MapFunction):
    def map(self, value):
        return value[2]

def execute():
    global current_job

    env = StreamExecutionEnvironment.get_execution_environment()

    # JARs of kafka (source) and Cassandra (sink) connectors
    kafka_jar = os.getenv("KAFKA_JAR")
    cassandra_jar = os.getenv("CASSANDRA_JAR")
    python_jar = os.getenv("PYTHON_JAR")

    env.add_jars(kafka_jar, cassandra_jar, python_jar)

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
        'fetch.max.wait.ms': '10000',
        }) \
        .build()

    metrics_sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_add) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("chicagotenant_ingestion_status")
                .set_value_serialization_schema(SimpleStringSchema()) 
                .build()
        ) \
        .build()

    # Input data stream
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # process the data, generate metrics in intervals
    processed_stream = stream \
        .map(lambda x: (1, x), output_type=Types.TUPLE([Types.INT(), Types.STRING()])) \
        .key_by(lambda x: x[0]) \
        .process(
            MetricsCollector(report_interval_ms=10000),
            output_type=Types.TUPLE([
            Types.INT(), # 0 is data, 1 is metrics    
            Types.ROW([
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
            ]),
            Types.STRING()
            ])
        )

    # Insert processed data into Cassandra Sink
    cassandra_ip = os.getenv("CASSANDRA_ADDRESS")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
    cassandra_table = os.getenv("CASSANDRA_TABLE")

    # get the actual ingested data from the datastream
    data = processed_stream.filter(lambda x: x[0] == 0).map(MapToDataRowFunction(), output_type=Types.ROW([
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

    # get the metrics data from the datastream
    metrics = processed_stream.filter(lambda x: x[0] == 1).map(MapToMetricsRowFunction(), output_type=Types.STRING())

    # insert data into cassandra    
    CassandraSink.add_sink(data) \
        .set_host(cassandra_ip) \
        .set_query(f"INSERT INTO {cassandra_keyspace}.{cassandra_table} (pickup_community_area, trip_id, company, \
            dropoff_centroid_latitude, dropoff_centroid_longitude, dropoff_community_area, extras, fare, \
            payment_type, pickup_centroid_latitude, pickup_centroid_longitude, taxi_id, tips, tolls, \
                trip_end_timestamp, trip_miles, trip_seconds, trip_start_timestamp, trip_total \
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") \
        .build()
    
    # send metrics to monitor
    metrics.sink_to(metrics_sink)
    
    try:
        res = env.execute_async("Kafka Flink Cassandra pipeline")
        current_job = res
        job_id = res.get_job_id()
        print("Flink job id: ", job_id)
    except Exception as e:
        print(f"Error while executing the Flink job: {e}")


# Stopped pipeline execution, send run statistics to monitor
def finishExecution(startTime, totalRows, kafka_producer):
    endTime = time.time()
    totalTime = endTime - startTime # - 60 # stopping since no new messages for 60 seconds
    totalSize = ((totalRows*48)/1000)
    speed = (totalSize/totalTime)

    msg = json.dumps({
        "start_time": startTime,
        "end_time": endTime,
        "total_time": totalTime,
        "rows": totalRows,
        "total_size": totalSize,
        "speed": speed
        }) 
    
    topic = f"chicagotenant_ingestion_report" 
    kafka_producer.produce(topic, msg.encode('utf-8'))
    kafka_producer.flush()

# Main loop, check for pipeline action calls and perform them
def run():
    global current_job, startTime
    load_dotenv()

    thread = None # thread for running pipeline
    startTime = 0

    casRowsStart = 0 # cassandra table row count at beginning
    casRowsEnd = 0

    # set up Kafka consumer to listen to pipeline execution start/stop messages
    k_add = f'{os.getenv("KAFKA_BROKER_ADDRESS")}:9092'
    kafka_conf = {
            'bootstrap.servers': k_add,
            'group.id': 'control',
            'auto.offset.reset': 'latest'
        }
    
    consumer = Consumer(kafka_conf)
    consumer.subscribe(["chicagotenant_ingestioncontrol"])

    kafka_conf_prod = {
            'bootstrap.servers': k_add,
        }
    
    kafka_producer = Producer(kafka_conf_prod)
    
    # Cassandra table to get num of rows
    cassandra_ip = os.getenv("CASSANDRA_ADDRESS")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
    cassandra_table = os.getenv("CASSANDRA_TABLE")
    cluster = Cluster([f"{cassandra_ip}"])
    session = cluster.connect(f"{cassandra_keyspace}")
    
    # start execution with thread
    try:
        while True: # consume in a loop
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                print(f"kafka error: {msg.error()}")
                continue

            try:
                if msg:
                    # get the action from message
                    json_value =json.loads(msg.value().decode('utf-8'))
                    action = json_value["action"]
                    
                    if (action == "start" and (thread is None or not thread.is_alive())): # start execution
                        if current_job is not None:
                            # pipeline running already, no need to do anything
                            print("Start action called, pipeline already running")
                            continue
                        
                        # get number of rows before ingestion
                        result = session.execute(f"SELECT COUNT(*) FROM {cassandra_table};") 
                        rows = result.one()[0]
                        casRowsStart = rows
                        
                        current_job = None
                        stop_event.clear()                          # clear stop
                        thread = threading.Thread(target=execute)   # initialize thread
                        thread.start()                              # start execution with thread
                        startTime = time.time()
                        print("Starting pipeline execution")

                    if (action == "stop"):
                        if current_job is not None:
                            # is job running
                            print("Received call to stop execution")
                            
                            result = session.execute(f"SELECT COUNT(*) FROM {cassandra_table};")
                            rows = result.one()[0]
                            casRowsEnd = rows
                            totalRows = casRowsEnd - casRowsStart
                            
                            finishExecution(startTime, totalRows, kafka_producer)
                            
                            # clean up
                            if current_job is not None:
                                print("job running, stop it")
                                try:
                                    current_job.cancel()
                                    print(f"Cancelled job {current_job.get_job_id()}")
                                except Exception as e:
                                    print(f"Error canceling job: {e}")
                            stop_event.set()
                            if thread:
                                thread.join(timeout=5)
                            current_job = None
                            startTime = 0

            except Exception as e:
                print(f"error while consuming message: {e}")
    except Exception as e:
        print(f"Error in main loop: {e}")
    except KeyboardInterrupt:
        if current_job is not None:
            try:
                current_job.cancel()
                print(f"Cancelled job {current_job.get_job_id()}")
            except:
                pass
        stop_event.set()
        if thread:
            thread.join()
        consumer.close()
        print("Exiting...")

if __name__ == "__main__":
    run()
