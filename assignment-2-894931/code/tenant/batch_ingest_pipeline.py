#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

# TODO process multiple files from input folder, get argument for which files

parser = argparse.ArgumentParser()
parser.add_argument("--input_file", help="input data file")
parser.add_argument("--keyspace", help="data storage cassandra keyspace")
parser.add_argument("--table", help="data storage cassandra table")

args = parser.parse_args()

# Create spark session
spark = SparkSession.builder.appName("HDFSSparkCassandraIngest")\
    .config("spark.cassandra.connection.localDC", "DC1")\
    .config("spark.cassandra.input.consistency.level", "LOCAL_ONE")\
    .getOrCreate()

# Get input data from HDFS staging file directory
df = spark.read.csv(f"hdfs://localhost:9000/{args.input_file}", header=True, inferSchema=True)

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
        options(table=args.table, keyspace=args.keyspace).save()
