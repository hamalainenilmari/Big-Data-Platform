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

# keep only input data which does not comply with schema for logging purposes
def incorrectFormat(stream):
    global current_job, incorrectRowsAmount, incorrectRowsSent
    try:
        jsonStream = json.loads(stream) # load the source string into json format
        if isinstance(jsonStream["Pickup Community Area"], float) and math.isnan(jsonStream["Pickup Community Area"]): 
            # Primary key is NaN, discard input
            return True
        elif isinstance(jsonStream["Trip ID"], float) and math.isnan(jsonStream["Trip ID"]):
            return True
        else:
            return False
    except json.JSONDecodeError as e:
        print(f"Json decoding error: {e}")

def execute():
    global current_job

    env = StreamExecutionEnvironment.get_execution_environment()
    # JARs of kafka (source) and Cassandra (sink) connectors
    env.add_jars(
        "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-sql-connector-kafka-3.4.0-1.20.jar",
        "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-connector-cassandra_2.12-3.2.0-1.19.jar",
        "file:///home/ilmarih/bdp_25_tech/flink-1.20.1/opt/flink-python-1.20.1.jar"
        )

    # Kafka Source setup
    kafka_ip = os.getenv("KAFKA_BROKER_ADDRESS")
    kafka_add = f"{kafka_ip}:9092"
    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_add) \
        .set_topics("chicago_taxitrips") \
        .set_group_id("g1") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_properties({
        'fetch.max.wait.ms': '10000',
        }) \
        .build()

    # Kafka sink for sending incorrect rows to stream monitor
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("chicago_ingestion_report_warning")
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

    # Process raw, filtered data
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
            ])
    )

    # Insert processed data into Cassandra Sink
    cassandra_ip = os.getenv("CASSANDRA_ADDRESS")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
    cassandra_table = os.getenv("CASSANDRA_TABLE")
    
    CassandraSink.add_sink(processedStream) \
        .set_host(cassandra_ip) \
        .set_query(f"INSERT INTO {cassandra_keyspace}.{cassandra_table} (pickup_community_area, trip_id, company, \
            dropoff_centroid_latitude, dropoff_centroid_longitude, dropoff_community_area, extras, fare, \
            payment_type, pickup_centroid_latitude, pickup_centroid_longitude, taxi_id, tips, tolls, \
                trip_end_timestamp, trip_miles, trip_seconds, trip_start_timestamp, trip_total \
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") \
        .build()
    
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
    totalTime = endTime - startTime
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
    
    topic = f"chicago_ingestion_report" 
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
    consumer.subscribe(["chicago_ingestioncontrol"])

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
