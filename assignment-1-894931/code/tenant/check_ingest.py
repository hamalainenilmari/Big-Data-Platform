'''
We use python client library from https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html.
Also see https://github.com/confluentinc/confluent-kafka-python
'''
from confluent_kafka import Consumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
import argparse
import json
from dotenv import load_dotenv
import os
import pandas as pd

if __name__ == '__main__':
    load_dotenv()
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-t', '--topic', default="taxiTrips", help='kafka topic')
    parser.add_argument('-g', '--consumer_group', default="g1", help='kafka topic')

    parser.add_argument('-i', '--input_file', default="../../data/sample100000.csv", help='input file to from which succesful data ingest is checked')

    parser.add_argument('--security_protocol', default='SASL_PLAINTEXT', help='security protocol')
    parser.add_argument('--sasl_mechanism', default='PLAIN', help='security protocol')
    parser.add_argument('--sasl_username', help='sasl user name')
    parser.add_argument('--sasl_password', help='sasl password')
    args = parser.parse_args()
    broker=args.broker

    # cassandra config
    """
    cassandra_hosts = [
        'localhost:9042',
        'localhost:9043', 
        'localhost:9044'
    ]
    """
    keyspace = os.getenv("CASSANDRA_KEYSPACE")
    table = os.getenv("CASSANDRA_TABLE")
    ip = os.getenv("CASSANDRA_IP")
    cluster = Cluster(
        [ip]
    )

    session = cluster.connect(keyspace)
    session.default_consistency_level = ConsistencyLevel.QUORUM # set the read consistency for session
    
    input_data =pd.read_csv(args.input_file,iterator=True,chunksize=5)
    notFound = 0
    for chunk in input_data:
        for index, row in chunk.iterrows():
            row_dict = row.to_dict()
            id = row_dict['Trip ID']

            query = f"SELECT taxi_id FROM {keyspace}.{table} WHERE trip_id='{id}' ALLOW FILTERING"
            statement = SimpleStatement(query)

            result = session.execute(statement)


            if result:
                found = False
                for row in result:
                    found = True
                if not found: 
                    print(f"did not find any row with trip_id: {id}")
                    notFound += 1

            else: 
                print(f"did not find any row with trip_id: {id}")
                notFound += 1

    print(f"did not find {notFound} rows")