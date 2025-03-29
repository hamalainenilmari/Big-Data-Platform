#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from dotenv import load_dotenv
from pyspark.sql.functions import udf, from_unixtime, trim
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

def execute():
        load_dotenv()
        parser = argparse.ArgumentParser()
        parser.add_argument("--input_files", nargs="+", help="input data files")
        #parser.add_argument("--hour", help="hour of data")

        args = parser.parse_args()
        files = args.input_files
        #print(files)
        splitted_name = files[0].split("silverData")
        gold_location = splitted_name[0] + "goldData" + "/" + splitted_name[1].split("/")[1]
        
        # Create spark session
        spark = SparkSession.builder.appName("BatchAnalytics")\
                .getOrCreate()

        df = spark.read.csv(files, header=False, inferSchema=True)
        df = df.toDF("pickup_location_area", "trips", "total_fares", "window_start_ts", "window_end_ts")
        
        aggregated_df = df.groupBy("pickup_location_area").agg(
                F.sum("trips").alias("trips"),
                F.sum("total_fares").alias("sum_fares"),
                F.avg("total_fares").alias("avg_fares"),
                F.min("window_start_ts").alias("window_start_ts"),  
                F.max("window_end_ts").alias("window_end_ts")
        )
        
        aggregated_df.show()
        
        aggregated_df.write \
        .mode("append") \
        .option("header", "true") \
        .csv(f"{gold_location}/")
        
if __name__ == "__main__":
    execute()