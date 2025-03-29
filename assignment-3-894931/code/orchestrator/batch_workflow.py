from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import subprocess
import pytz
import os
from dotenv import load_dotenv

# workflow args
default_args = {
    "owner": "mysimbdp",
    "retries": 0,
    "retry_delay": timedelta(minutes=1)
}

load_dotenv()

# this step looks for new silver data input files from HDFS
def get_new_files(ti):
    hadoop_path = os.getenv("HADOOP_PATH")
    time_zone = pytz.timezone('Europe/Helsinki')
    current_time = datetime.now(time_zone)

    current_date = current_time.date()
    current_hour = current_time.hour
    
    # shell script to check HDFS
    cmd = f"{hadoop_path}/bin/hdfs dfs -ls /chicagoTenant/silverData/{current_date}--{current_hour}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        output = result.stdout
        print("Command output:", output)

        filenames = []
        for file in output.splitlines():
            # check if file is not already processed, and correct format
            if "processed" not in file:
                if "csv" in file:
                    filename = "hdfs://localhost:9000" + file.split()[-1]
                    print("adding file: ", filename, " to process list")
                    filenames.append(filename)

        print("Files to process:", filenames)

        if not filenames or len(filenames) == 0:
            return False
        
        ti.xcom_push(key="new_files_output", value=filenames)
        return filenames
    else:
        print("Error executing command:", result.stderr)
        return False

# final step, rename processed files to avoid duplicate processing
def rename_processed_files(ti):
    hadoop_path = os.getenv("HADOOP_PATH")

    time_zone = pytz.timezone('Europe/Helsinki')

    # files to rename
    files = ti.xcom_pull(task_ids="get_new_silverdata_files", key="new_files_output")

    for file in files:
        split = file.split(".")
        file_name = split[0]
        file_format = split[1]
        updated_name = file_name + "_processed"

        # turn the silver data file name from file0-0.csv to file0-0_processed.csv
        cmd = f"{hadoop_path}/bin/hdfs dfs -mv {file} {updated_name}.{file_format}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(result)


# main step, submit batch analytics job for spark
def submit_spark_job(ti):
    spark_path = os.getenv("SPARK_PATH")

    # get files to process
    files = ti.xcom_pull(task_ids="get_new_silverdata_files", key="new_files_output")

    cmd = [
         f"{spark_path}/bin/spark-submit", 
         "--master", "local[4]", 
         "/home/ilmarih/airflow/dags/batch.py"
    ] + ["--input_files"] + files
    
    # Run the command to submit the Spark job
    subprocess.run(cmd, check=True)


with DAG(
    default_args=default_args,
    dag_id="spark_pipeline",
    description="get silverdata from HDFS silver, process it, put output gold data to HDFS gold",
    start_date=datetime(2024, 3,26),
    schedule="* * * * *",  # run every minute
    catchup=False, # if true, this will try to catchup missed executions
) as dag:
    process_silverdata = PythonOperator(
        task_id="get_silver_produce_gold",
        python_callable=submit_spark_job,
        provide_context = True
    )
    
    get_new_silverdata = ShortCircuitOperator(
        task_id="get_new_silverdata_files",
        python_callable=get_new_files
    )
    
    rename_processed_silverdata = PythonOperator(
        task_id="rename_old_silverdata_files",
        python_callable=rename_processed_files
    )

    # workflow
    get_new_silverdata >> process_silverdata >> rename_processed_silverdata