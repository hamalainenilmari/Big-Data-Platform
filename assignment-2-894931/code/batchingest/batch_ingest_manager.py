import subprocess

# Batch ingest manager, starts the tenant specific batch data ingestion pipeline

def main():
    cmd = f"/home/ilmarih/bdp_25_tech/spark-3.5.5-bin-hadoop3/bin/spark-submit --master local[*] \
    --conf spark.cassandra.connection.host=localhost \
    --conf spark.cassandra.connection.port=9042 \
    --conf spark.cassandra.connection.local_dc=DC1 \
    --conf spark.cassandra.connection.timeoutMS=30000 \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15,com.github.jnr:jnr-ffi:2.2.11 \
    code/tenant/batch_ingest_pipeline.py"
    subprocess.run(cmd, shell=True)
    

if __name__ == "__main__":
    main()