from confluent_kafka import Consumer
import argparse
import json
from dotenv import load_dotenv
import logging
from datetime import datetime

def generateReport(tenant, statistics, badRows):
    start = datetime.fromtimestamp(statistics["start_time"]).strftime('%Y-%m-%d %H:%M:%S')
    end = datetime.fromtimestamp(statistics["end_time"]).strftime('%Y-%m-%d %H:%M:%S')
    totalTime = statistics["total_time"]
    rows = statistics["rows"]
    size = statistics["total_size"]
    speed = statistics["speed"]
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
                                                   "nytenant_ingestion_report", "nytenant_ingestion_report_warning"], help='kafka topics')
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
                print("tenant: ", tenant)
                print("report: ", report)
                # discarded row information
                if report == "ingestion_report_warning":
                    rowsDiscarded = tenants[tenant][1]
                    tenants[tenant] = (False, rowsDiscarded + 1)

                # final execution statistics
                if report == "ingestion_report":
                    print("got full execution statistics, generate reports")
                    stats = json.loads(msg.value().decode('utf-8'))
                    rows = tenants[tenant][1]
                    tenants[tenant] = (False, 0)
                    generateReport(tenant, stats, rows)

    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        kafka_consumer.close()