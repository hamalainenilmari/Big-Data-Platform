import subprocess
from multiprocessing import Process
import json
import os
import logging
import time
from hdfs import InsecureClient
from hdfs.util import HdfsError

# Batch ingest manager, starts the tenant specific batch data ingestion pipeline

# Start executing pipeline of tenant
def startExecution(tenant):
    # Initialize logging for pipeline execution loop
    logFile = f"logs/{tenant['id']}_{int(time.time())}_ingestion.log"

    logger = logging.getLogger(f"tenant_{tenant['id']}")
    logger.setLevel(logging.INFO)

    fileHandler = logging.FileHandler(logFile)
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fileHandler)
    # execute pipeline
    executePipeline(tenant, logger)

# Run the pipeline
def executePipeline(tenant, logger):
    total_ingest_time = 0
    total_ingest_size = 0
    logger.info("######################################################")
    logger.info(f"Starting tenant {tenant['id']} pipeline execution ...")
    try:
        # Connect to HDFS: staging input directory
        client = InsecureClient('http://localhost:9870', user='ilmarih')

        # Get the tenant staging input directory location from service agreement and check for input data
        folder = tenant['fileStorageLocation']
        files = client.list(folder)

        numFiles = len(files)
        if (numFiles == 0):
            logger.info("No files in staging input directory to ingest. Stopping pipeline.")
            stopPipeline(0,0,logger,tenant)

        combinedStorageUsed = 0 # staging input dir usage
        filesToIngest = [] # ingest only files up to max ingestion interval size
        ingestedFilesSize = 0
        start_time_total = 0.0

        logger.info(f"Files:")
        for file in files:
            file_size = client.status(f"{folder}/{file}")['length']
            logger.info(f"* {file} - {(file_size/1000000):.2f} MB")
            type = file.split(".")[1]
            # Check if file type is supported by service agreement
            if (not tenant['fileTypesSupported'][type]):
                logger.warning(f"Tenant Service Agreement failure: input file type '{type}' not supported")
                logger.warning(f"Removing file {file}")
                client.delete(f"{folder}/{file}") # not supported, delete
                files.remove(file)
            """
            else:
                if ((filesToIngestSize + file_size) < tenant["maxIngestionIntervalSize"]*1000000):
                    #File type supported, ingestion limit not reached -> add the file to ingestion list
                    filesToIngest.append(file)
                    filesToIngestSize += file_size
                    logger.info(f"added {file} to ingest list")
                """
            combinedStorageUsed += (file_size)

        logger.info(f"Statistics:\n  *  Amount of files in tenant's staging input directory: {numFiles}\n  *  Combined Storage Used: {(combinedStorageUsed/1000000):.2f} /{tenant['maxStorage']} MB")
        
        if (numFiles > tenant["maxNumFiles"]):
            logger.warning("Tenant Service Agreement limit reached: input file amount limit exceeded")
            logger.warning(f"Remove exceeding files to start ingestion.")
            stopPipeline(0,0, logger, tenant)
        elif (combinedStorageUsed > tenant["maxStorage"]*1000000):
            logger.warning("Tenant Service Agreement limit reached: storage limit exceeded")
            logger.warning(f"Storage used: {combinedStorageUsed / 1000000} MB")
            logger.warning(f"Storage limit: {tenant['maxStorage']} MB")
            logger.warning(f"Remove exceeding files to start ingestion.")
            stopPipeline(0,0, logger, tenant)
        else:
            start_time_total = time.time()
            for file in files:
                # If we have ingested the max amount of tenant, sleep
                if (ingestedFilesSize >= tenant["maxIngestionIntervalSize"]*1000000):
                    logger.info(f"Ingestion interval limit reached. Starting ingestion again after {tenant['interval']} seconds ...")
                    time.sleep(tenant['interval'])
                    ingestedFilesSize = 0
                
                start_time_inv_file = time.time()
                logger.info(f"Ingesting {file}")
                # TODO add spark location to env variable
                cmd = f"/home/ilmarih/bdp_25_tech/spark-3.5.5-bin-hadoop3/bin/spark-submit --master local[*] \
                --conf spark.cassandra.connection.host=localhost \
                --conf spark.cassandra.connection.port=9042 \
                --conf spark.cassandra.connection.local_dc=DC1 \
                --conf spark.cassandra.connection.timeoutMS=30000 \
                --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15,com.github.jnr:jnr-ffi:2.2.11 \
                code/tenant/{tenant['pipeline']} \
                --input_file {folder}/{file} \
                --keyspace {tenant['coredmsKeyspace']} \
                --table {tenant['coredmsTable']}"
                subprocess.run(cmd, shell=True)
                end_time_inv_file = time.time()
                ingestion_time = end_time_inv_file - start_time_inv_file
                total_ingest_time += ingestion_time
                fSize = client.status(f"{folder}/{file}")['length']
                total_ingest_size += fSize
                ingestedFilesSize += fSize
                client.delete(f"{folder}/{file}")
                logger.info(f"Ingested {file}. Time taken: {ingestion_time:.2f}s. Deleted from staging dir.")

        end_time_total = time.time()
        total_time = end_time_total - start_time_total
        logger.info(f"Ingestion ended, amount of files ingested: {len(files)}")
        stopPipeline(total_time, total_ingest_size, logger, tenant)
    except HdfsError as e:
        logger.warning(f"Error trying to access tenant HDFS storage location: {e}")
    except Exception as e:
        logger.warning(f"Error: {e}")

def stopPipeline(total_time, total_size, logger, tenant):
    logger.info("--------------------------------")
    logger.info(f"Total ingestion time: {total_time:.2f} s")
    logger.info(f"Total ingestion size: {(total_size/1000000):.2f} MB")
    if (total_time != 0):
        logger.info(f"Ingestion speed: {((total_size/1000000)/total_time):.2f} MB/s")
    if (total_size == 0 and total_time == 0):

        logger.info(f"Checking for new files after {tenant['interval']} seconds ...")
        time.sleep(tenant['interval'])
        executePipeline(tenant, logger)
    
def main():
    # Get the service agreements of tenants
    tenants = []
    for filename in os.listdir("code/batchingest/service_agreements"):
        with open(f'code/batchingest/service_agreements/{filename}', 'r') as file:
           tenants.append(json.load(file))
    
    processes = []
    # Parallel execution of tenant pipelines
    for tenant in tenants:
        p = Process(target=startExecution, args=(tenant,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()