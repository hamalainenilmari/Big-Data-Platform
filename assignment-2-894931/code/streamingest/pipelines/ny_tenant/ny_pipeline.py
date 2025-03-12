from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.common import Types, Row
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
import math
import json
from datetime import datetime
import time
import os
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
import threading
from cassandra.cluster import Cluster

# event to stop pipeline exeution running as thread
stop_event = threading.Event()
# current flink job
current_job = None

# Modify NaN values to Null/None, remove unneeded values
def transform(stream):
    jsonStream = json.loads(stream) # load the source string into json format
    for key,value in jsonStream.items():
        if isinstance(value, float) and math.isnan(value): # value is NaN, modify it to null
            jsonStream[key] = None

    # format timestamps
    timestampStart = datetime.strptime(jsonStream["tpep_pickup_datetime"], '%Y-%m-%dT%H:%M:%SZ')
    timestampEnd = datetime.strptime(jsonStream["tpep_dropoff_datetime"], '%Y-%m-%dT%H:%M:%SZ')

    # Create Flink Row from source, format ready for Cassandra insert
    # removed congestion_surcharge, improvement_surcharge, store_and_fwd_flag

    filteredRow = Row(
        int(jsonStream["VendorID"]),
        timestampStart,
        timestampEnd,
        jsonStream["passenger_count"],
        jsonStream["trip_distance"],
        jsonStream["RatecodeID"],
        jsonStream["PULocationID"],
        jsonStream["DOLocationID"],
        jsonStream["payment_type"],
        jsonStream["fare_amount"],
        jsonStream["extra"],
        jsonStream["mta_tax"],
        jsonStream["tip_amount"],
        jsonStream["tolls_amount"],
        jsonStream["total_amount"],
        jsonStream["Airport_fee"]
    )
    return filteredRow

def checkPrimaryKeys(stream):
    try:
        jsonStream = json.loads(stream) # load the source string into json format
        if isinstance(jsonStream["tpep_dropoff_datetime"], int) and math.isnan(jsonStream["tpep_dropoff_datetime"]): 
            # Primary key is NaN, discard input
            return False
        elif isinstance(jsonStream["VendorID"], int) and math.isnan(jsonStream["VendorID"]):
            return False
        else:
            # valid data
            return True
    except json.JSONDecodeError as e:
        print(f"Json decoding error: {e}")

# keep only input data which does not comply with schema for logging purposes
def incorrectFormat(stream):
    global current_job, incorrectRowsAmount, incorrectRowsSent
    try:
        jsonStream = json.loads(stream) # load the source string into json format
        if isinstance(jsonStream["tpep_dropoff_datetime"], int) and math.isnan(jsonStream["tpep_dropoff_datetime"]): 
            # Primary key is NaN, discard input
            return True
        elif isinstance(jsonStream["VendorID"], int) and math.isnan(jsonStream["VendorID"]):
            return True
        else:
            return False
    except json.JSONDecodeError as e:
        print(f"Json decoding error: {e}")

def execute():
    global current_job

    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_jar = os.getenv("KAFKA_JAR")
    cassandra_jar = os.getenv("CASSANDRA_JAR")
    python_jar = os.getenv("PYTHON_JAR")

    # JARs of kafka (source) and Cassandra (sink) connectors
    env.add_jars(
        kafka_jar,
        cassandra_jar,
        python_jar
        )

    # Kafka Source setup
    kafka_ip = os.getenv("KAFKA_BROKER_ADDRESS")
    kafka_add = f"{kafka_ip}:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_add) \
        .set_topics("nytenant_trips") \
        .set_group_id("ny") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_properties({
        'fetch.max.wait.ms': '10000',
        }) \
        .build()

    # Kafka sink for sending incorrect rows to stream monitor
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_add) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("nytenant_ingestion_report_warning")
                .set_value_serialization_schema(SimpleStringSchema()) 
                .build()
        ) \
        .build()
    
    # Input data stream
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # if NaN values for primary keys, filter
    filteredStream = stream.filter(checkPrimaryKeys)
    # send incorrect rows to monitor component
    incorrectStream = stream.filter(incorrectFormat)
    incorrectStream.filter(lambda x: str(x)).sink_to(kafka_sink)
    """
        vendor_id uuid,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count int,
    trip_distance float,
    ratecode_id int,
    pu_location_id int,
    do_location_id int,
    payment_type int,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    total_amount float,
    airport_fee float,"
    """
    # Process raw, filtered data
    processedStream = filteredStream.map(
        lambda raw: transform(raw),
        output_type=Types.ROW([
                    Types.INT(),         # vendor_id (uuid)
                    Types.SQL_TIMESTAMP(),  # tpep_pickup_datetime (timestamp)
                    Types.SQL_TIMESTAMP(),  # tpep_dropoff_datetime (timestamp)
                    Types.INT(),            # passenger_count (int)
                    Types.FLOAT(),          # trip_distance (float)
                    Types.INT(),            # ratecode_id (int)
                    Types.INT(),            # pu_location_id (int)
                    Types.INT(),            # do_location_id (int)
                    Types.INT(),            # payment_type (int)
                    Types.FLOAT(),          # fare_amount (float)
                    Types.FLOAT(),          # extra (float)
                    Types.FLOAT(),          # mta_tax (float)
                    Types.FLOAT(),          # tip_amount (float)
                    Types.FLOAT(),          # tolls_amount (float)
                    Types.FLOAT(),          # total_amount (float)
                    Types.FLOAT()           # airport_fee (float)
                ])
    )
    # Insert processed data into Cassandra Sink
    cassandra_ip = os.getenv("CASSANDRA_ADDRESS")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
    cassandra_table = os.getenv("CASSANDRA_TABLE")
    
    CassandraSink.add_sink(processedStream) \
        .set_host(cassandra_ip) \
        .set_query(f"INSERT INTO {cassandra_keyspace}.{cassandra_table} (vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, \
            passenger_count, trip_distance, ratecode_id, pu_location_id, do_location_id, payment_type, fare_amount, \
            extra, mta_tax, tip_amount, tolls_amount, total_amount, airport_fee) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") \
        .build()
    
    
    try:
        res = env.execute_async("NY - Kafka Flink Cassandra pipeline")
        current_job = res
        job_id = res.get_job_id()
        print("Flink job id: ", job_id)
    except Exception as e:
        print(f"Error while executing the Flink job: {e}")


# Stopped pipeline execution, send run statistics to monitor
def finishExecution(startTime, totalRows, kafka_producer):
    endTime = time.time()
    totalTime = endTime - startTime - 60 # stopping since no new messages for 60 seconds
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
    
    topic = f"nytenant_ingestion_report" 
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
    consumer.subscribe(["nytenant_ingestioncontrol"])

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
    
    try:
        while True: # consume in a loop
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                print("error")
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
