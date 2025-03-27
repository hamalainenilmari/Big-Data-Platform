#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

def execute():
        load_dotenv()
        parser = argparse.ArgumentParser()
        #parser.add_argument("--input_file", help="input data file")
        #parser.add_argument("--hour", help="hour of data")


        args = parser.parse_args()

        # Create spark session
        spark = SparkSession.builder.appName("BatchAnalytics")\
                .getOrCreate()

        df = spark.read.csv("hdfs://localhost:9000/chicagoTenant/silverData/2025-03-26--17/*", header=False)
        unique_values = df.select(df.columns[0]).distinct()

        aggregated_df = df.groupBy(df.columns[0]) \
                        .agg(
                        F.sum(df.columns[1]).alias("sum_trips"),
                        F.sum(df.columns[2]).alias("sum_fares"),
                        F.avg(df.columns[2]).alias("avg_fares")
                        )

        aggregated_df.show()

        aggregated_df.write \
        .mode("overwrite") \
        .option("header", "false") \
        .csv("hdfs://localhost:9000/chicagoTenant/goldData/2025-03-26--17/")


        # TODO ).dropDuplicates()

        """
        # Data wrangling: remove unneeded, change key format, modify some types
        df = df.drop('Pickup Census Tract', 'Dropoff Census Tract', 'Pickup Centroid Location', 'Dropoff Centroid  Location')
        df = df.toDF(*[key.replace(" ", "_").lower() for key in df.columns])

        df = df.withColumn("trip_start_timestamp", col("trip_start_timestamp").cast("timestamp"))
        df = df.withColumn("pickup_community_area", col("pickup_community_area").cast("double"))
        df = df.withColumn("dropoff_community_area", col("dropoff_community_area").cast("double"))

        df = df.withColumn("trip_end_timestamp", col("trip_end_timestamp").cast("timestamp"))
        df = df.fillna({"pickup_community_area": -1.0})

        # Insert processed data into Cassandra
        df.write.format("org.apache.spark.sql.cassandra").mode('append').\
                options(table=args.table, keyspace=args.keyspace).save()"
                ""
        """

if __name__ == "__main__":
    execute()