# Cassandra data storage instructions

## This platform component is designed and implemented in assignment 1

### This component is not needed for simulating streaming analytics

This component is the operational data storage of the platform. Technology is Apache Cassandra. The Cassandra cluster of 4 nodes can be started by running:

``$ docker compose up -d``

After the containers are healthy and up you can enter one of them and create the following keyspace and table to the Cassandra cluster by running

``$ docker exec -it <container_name> /bin/bash``

You can check the node status by running:

``$ nodetool status``

If everything is okay, each 4 nodes should be UN (status=UP, state=NORMAL)

Then enter cassandra shell by running:

``$ cqlsh``

Then to create keyspaces run:

``$ CREATE KEYSPACE chicagotenant WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 2, 'DC2': 1} AND durable_writes = true;``

And then to create the tables run:

``$
CREATE TABLE chicagotenant.trips (
    trip_id text,
    taxi_id text,
    trip_start_timestamp timestamp,
    trip_end_timestamp timestamp,
    trip_seconds int,
    trip_miles float,
    pickup_community_area float,
    dropoff_community_area float,
    fare float,
    tips float,
    tolls float,
    extras float,
    trip_total float,
    payment_type text,
    company text,
    pickup_centroid_latitude double,
    pickup_centroid_longitude double,
    dropoff_centroid_latitude double,
    dropoff_centroid_longitude double,
    PRIMARY KEY (pickup_community_area, trip_id)
);
``

To test that ingestion is working correctly, you can try to query rows in cqlsh after running the ingestion pipeline.

``$ SELECT COUNT(*) FROM chicagotenant.trips;``
