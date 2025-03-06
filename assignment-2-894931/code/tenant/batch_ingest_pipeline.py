#!/usr/bin/env python3
import argparse
from hdfs import InsecureClient

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
parser.add_argument("--input_file", help="input data file")
#parser.add_argument("--output_dir", help="output dir")

args = parser.parse_args()

#client = InsecureClient('http://localhost:9870', user='ilmarih')
#with client.read("/texiTenant/sample1.csv") as reader:
 #   content = reader.read()

# Create spark session
spark = SparkSession.builder.appName("HDFSSparkCassandraIngest")\
.config("spark.cassandra.connection.local_dc", "DC1")\
    .config("spark.cassandra.input.consistency.level", "LOCAL_ONE")\
    .getOrCreate()

input_file = args.input_file
# Get input data from HDFS staging file directory
df = spark.read.csv("hdfs://localhost:9000/texiTenant/sample2.csv", header=True, inferSchema=True)

# Data wrangling
df = df.drop('Pickup Census Tract', 'Dropoff Census Tract', 'Pickup Centroid Location', 'Dropoff Centroid  Location')
df = df.toDF(*[key.replace(" ", "_").lower() for key in df.columns])

df = df.withColumn("trip_start_timestamp", col("trip_start_timestamp").cast("timestamp"))
df = df.withColumn("pickup_community_area", col("pickup_community_area").cast("double"))
df = df.withColumn("dropoff_community_area", col("dropoff_community_area").cast("double"))

df = df.withColumn("trip_end_timestamp", col("trip_end_timestamp").cast("timestamp"))
df = df.fillna({"pickup_community_area": -1.0})

# Insert processed data into Cassandra
df.write.format("org.apache.spark.sql.cassandra").mode('append').\
        options(table="trips", keyspace="taxiservices").save()

# df.show()
"""
print("Number of trips", df.count())
# number of passenger count per vendor and total amount of money
#passenser_exprs = {"passenger_count": "sum", "total_amount": "sum"}
#df2 = df.groupBy("VendorID").agg(passenser_exprs)
# Where do you want to write the output
df.repartition(1).write.csv("hdfs://localhost:9000/texiTenant/result", header=True)

"""