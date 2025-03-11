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
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv
import threading
from cassandra.cluster import Cluster

# event to stop pipeline exeution running as thread
stop_event = threading.Event()
# current flink job
current_job = None

# logging data
startTime = 0
endTime = 0
inboundMessagesAmount = 0
ingestionSpeed = 0
totalIngestionSize = 0
numMessagesError = 0
incorrectRowsAmount = 0

casRowsStart = 0
casRowsEnd = 0


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
    global current_job, incorrectRowsAmount #, inboundMessagesAmount, totalIngestionSize, numMessagesError, incorrectRowsAmount
    try:
        jsonStream = json.loads(stream) # load the source string into json format
        if isinstance(jsonStream["Pickup Community Area"], float) and math.isnan(jsonStream["Pickup Community Area"]): 
            # Primary key is NaN, discard input
            incorrectRowsAmount += 1
            return False
        elif isinstance(jsonStream["Trip ID"], float) and math.isnan(jsonStream["Trip ID"]):
            incorrectRowsAmount += 1
            return False
        else:
            return True
    except json.JSONDecodeError as e:
        print(f"json error: {e}")


def execute():
    global current_job #, inboundMessagesAmount, totalIngestionSize, numMessagesError, incorrectRowsAmount

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
    
    # Input data stream
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # if NaN values for primary keys, filter
    filteredStream = stream.filter(checkPrimaryKeys)

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

    # Insert data into Cassandra Sink
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

# Create unique log file for ingestion execution
def initializeLogging():
    global startTime
    timeStmp = int(time.time())
    logFile = f"../../../logs/stream/chicago_{timeStmp}_stream_ingestion.log"
    logger = logging.getLogger(f"tenant_chicago_{timeStmp}")
    logger.setLevel(logging.INFO)

    fileHandler = logging.FileHandler(logFile)
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fileHandler)

    logger.info("######################################################")
    logger.info(f"Starting tenant chicago stream pipeline execution ...")
    startTime = time.time()
    return logger

# Stopped pipeline execution, write statistics to logfile
def finishLogging(logger, totalRows):
    global inboundMessagesAmount, startTime, incorrectRowsAmount, totalIngestionSize

    endTime = time.time()
    totalTime = endTime - startTime
    logger.info("--------------------------------")
    logger.info(f"Total ingestion time: {totalTime:.2f} s")
    logger.info(f"Total number of rows inserted: {totalRows}")
    logger.info(f"Total ingestion size: {((totalRows*48)/1000):.2f} kB")
    if (totalTime != 0):
        logger.info(f"Ingestion speed: {(((totalRows*48)/1000)/totalTime):.2f} kB/s")
    logger.info(f"Number of inconsistent data rows: {incorrectRowsAmount}")
    logger = None

# Main loop, check for pipeline action calls and perform them
def run():
    global current_job
    load_dotenv()

    logger = None # logger component
    thread = None # thread for running pipeline

    # set up Kafka consumer to listen to pipeline execution start/stop messages
    k_add = f'{os.getenv("KAFKA_BROKER_ADDRESS")}:9092'
    kafka_conf = {
            'bootstrap.servers': k_add,
            'group.id': 'control',
            'auto.offset.reset': 'latest'
        }
    
    consumer = Consumer(kafka_conf)
    consumer.subscribe(["chicago_ingestioncontrol"])

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
                        logger = None
                        logger = initializeLogging()

                        current_job = None
                        stop_event.clear()                          # clear stop
                        thread = threading.Thread(target=execute)   # initialize thread
                        thread.start()                              # start execution with thread

                        print("Starting pipeline execution")

                    if (action == "stop"):
                        if current_job is not None:
                            # is job running
                            print("Received call to stop execution")
                            
                            result = session.execute(f"SELECT COUNT(*) FROM {cassandra_table};")
                            rows = result.one()[0]
                            casRowsEnd = rows
                            totalRows = casRowsEnd - casRowsStart
                            finishLogging(logger, totalRows)
                            logger = None
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
