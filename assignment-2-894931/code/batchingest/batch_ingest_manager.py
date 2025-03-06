import subprocess
import multiprocessing
import json
import os
from hdfs import InsecureClient
from hdfs.util import HdfsError

# Batch ingest manager, starts the tenant specific batch data ingestion pipeline

def exit():
    print("exiting ...")

def executePipeline(tenant):
    print(f"Executing tenant {tenant['id']} pipeline ...")
    # Connect to HDFS
    try:
        client = InsecureClient('http://localhost:9870', user='ilmarih')

        # Get the service agreement
        folder = tenant['fileStorageLocation']
        # Check tenants staging input directory for input data
        files = client.list(folder)

        numFiles = len(files)
        combinedStorage = 0
        for file in files:
            type = file.split(".")[1]
            if (not tenant['fileTypesSupported'][type]):
                print(f"Tenant Service Agreement failure: input file type '{type}' not supported")
                print(f"Removing file {file}")
                client.delete(f"{folder}/{file}")
                files.remove(file)
            else:
                combinedStorage += (client.status(f"{folder}/{file}")['length'])

        print(f"Statistics:\nAmount of files: {numFiles}\nCombined Storage: {combinedStorage}\nFiles:")
        for file in files:
            print(f"- {file}")
        
        if (numFiles > tenant["maxNumFiles"]):
            print("Tenant Service Agreement limit reached: too many input files")
            exit()
        elif (combinedStorage > tenant["maxStorage"]*1000000):
            print("Tenant Service Agreement limit reached: too much storage used")
            print(f"Storage used: {combinedStorage / 1000000} MB")
            print(f"Storage limit: {tenant['maxStorage']} MB")
            exit()
        else:
            for file in files:
                print(f"Ingesting {file}")
                # TODO add spark location to env variable
                cmd = f"/home/ilmarih/bdp_25_tech/spark-3.5.5-bin-hadoop3/bin/spark-submit --master local[*] \
                --conf spark.cassandra.connection.host=localhost \
                --conf spark.cassandra.connection.port=9042 \
                --conf spark.cassandra.connection.local_dc=DC1 \
                --conf spark.cassandra.connection.timeoutMS=30000 \
                --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15,com.github.jnr:jnr-ffi:2.2.11 \
                code/tenant/{tenant['pipeline']} \
                --input_file {folder}/{file}"
                subprocess.run(cmd, shell=True)
                print("Ingested")
    except HdfsError as e:
        print(f"Error trying to access tenant HDFS storage location: {e}")
    except Exception as e:
        print(f"Error: {e}")
    exit()
    
def main():
    # Get the service agreements of tenants
    tenants = []
    for filename in os.listdir("code/batchingest/service_agreements"):
        with open(f'code/batchingest/service_agreements/{filename}', 'r') as file:
           tenants.append(json.load(file))
    
    numTenants = len(tenants)
    # Parallel execution of tenant pipelines
    with multiprocessing.Pool(processes=numTenants) as pool:
        pool.map(executePipeline, tenants)

if __name__ == "__main__":
    main()