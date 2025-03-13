from confluent_kafka import Consumer, Producer
import argparse
import json
from dotenv import load_dotenv
import logging
from datetime import datetime

tenant_pipelines = {
    "chicagotenant": {
        "minimumIngestionSpeed": 10,      # kB/s
        "minRowsProcessed": 1,            # min number of rows ingested during execution before a problem
        "maxDiscardedRowsRelation": 0.001 # % of how many rows of input data can be discarded before a problem
    },
    "nytenant": {
        "minimumIngestionSpeed": 2,
        "minRowsProcessed": 10,
        "maxDiscardedRowsRelation": 0.01 # lower value, should not have many
    }
}

def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

def handleStatistics(statistics, producer):
    tenantId = statistics["tenant_id"]
    time = statistics["timestamp"]
    rows = statistics["rows_processed"]
    if "rows_per_second" in statistics:
        speed = statistics["rows_per_second"] * 48
    else:
        speed = 0
    #speed = statistics["rows_per_second"] * 48
    badRows = statistics["discarded_rows"]
    if rows > 0:
        relation = badRows / (badRows + rows) # discarded data amount divided by all consumed data
    else:
        relation = 1
    print(statistics)
     
    if relation > tenant_pipelines[tenantId]["maxDiscardedRowsRelation"]:
        print(f"Max amount of bad format rows in relation to total rows in ingestion exceeded \
               {relation:.2f} / {tenant_pipelines[tenantId]['maxDiscardedRowsRelation']} -> informing manager")
        msg = {
            "tenant": tenantId,
            "warning": "maxDiscardedRowsRelation"
            }
        json_data=json.dumps(msg)
        producer.produce("pipeline_execution_warning", json_data.encode('utf-8'), callback=kafka_delivery_error)
        producer.flush()

    elif rows < tenant_pipelines[tenantId]["minRowsProcessed"]:
        print(f"Below min amount of rows processed in ingestion {rows} / {tenant_pipelines[tenantId]['minRowsProcessed']} -> informing manager")
        msg = {
            "tenant": tenantId,
            "warning": "minRowsProcessed"
            }
        json_data=json.dumps(msg)
        producer.produce("pipeline_execution_warning", json_data.encode('utf-8'), callback=kafka_delivery_error)
        producer.flush()

    elif speed < tenant_pipelines[tenantId]["minimumIngestionSpeed"]:
        print(f"Ingestion performing below min ingestion speed {speed:.2f} / {tenant_pipelines[tenantId]['minimumIngestionSpeed']} -> informing manager")
        msg = {
            "tenant": tenantId,
            "warning": "minimumIngestionSpeed"
            }
        json_data=json.dumps(msg)
        producer.produce("pipeline_execution_warning", json_data.encode('utf-8'), callback=kafka_delivery_error)
        producer.flush()


def generateReport(tenant, statistics, badRows, producer):
    start = datetime.fromtimestamp(statistics["start_time"]).strftime('%Y-%m-%d %H:%M:%S')
    end = datetime.fromtimestamp(statistics["end_time"]).strftime('%Y-%m-%d %H:%M:%S')
    totalTime = statistics["total_time"]
    rows = statistics["rows"]
    size = statistics["total_size"]
    speed = statistics["speed"] * 48
    print(statistics)

    logFile = f"../../logs/stream/{tenant}_{start}_stream_ingestion.log"
    logger = logging.getLogger(f"{tenant}_{start}")
    logger.setLevel(logging.INFO)
    
    fileHandler = logging.FileHandler(logFile)
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fileHandler)
   
    logger.info(f"{tenant} - Stream ingestion statistics:")
    logger.info(f"Ingestion started at: {start}")
    logger.info(f"Ingestion ended at: {end}")
    logger.info(f"Total ingestion time: {totalTime:.2f} s")
    logger.info(f"Total number of rows inserted: {rows}")
    logger.info(f"Total ingestion size: {size} kB")
    logger.info(f"Ingestion speed: {speed:.2f} kB/s")
    logger.info(f"Number of rows not inserted due to format not matching schema: {badRows}")

if __name__ == '__main__':
    load_dotenv()
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-t', '--topics', default=["chicagotenant_ingestion_report", "chicagotenant_ingestion_report_warning", \
                                                   "chicagotenant_ingestion_status","nytenant_ingestion_report", \
                                                    "nytenant_ingestion_report_warning", "ingestion_status"], help='kafka topics')
    parser.add_argument('-g', '--consumer_group', default="monitor", help='consumer group')
    args = parser.parse_args()
    broker=args.broker

    #create configuration file for kafka connection
    kafka_conf={
        'bootstrap.servers': broker,
        'group.id': args.consumer_group,
    }
    
    topics = args.topics
    
    kafka_consumer = Consumer(kafka_conf)
    kafka_consumer.subscribe(topics)

    kafka_conf_p={
        'bootstrap.servers': broker,
    }
    producer = Producer(kafka_conf_p)

    tenants = {}
    for item in topics:
        tenants[item.split("_")[0]] = (False, 0) # final execution report has came, number of discarded rows

    print("Listening to tenants pipeline reports")
    try:
        while True:
            # consume a message from kafka, wait 10 second
            msg = kafka_consumer.poll(10.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                continue
            
            if msg:
                topic = msg.topic()
                split = topic.split("_", 1)
                tenant = split[0]
                report = split[1]
                print(msg.value())
                #print("tenant: ", tenant)
                #print("report: ", report)
                value = json.loads(msg.value().decode('utf-8'))
                if report == "ingestion_status":
                    print(f"Reveived ingestion status from tenant {value['tenant_id']} pipeline:")
                    handleStatistics(value, producer)

                # final execution statistics
                if report == "ingestion_report":
                    print("got full execution statistics, generate reports")
                    stats = json.loads(msg.value().decode('utf-8'))
                    rows = tenants[tenant][1]
                    tenants[tenant] = (False, 0)
                    generateReport(tenant, stats, rows, producer)

    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        kafka_consumer.close()