# Mysimbdp-coredms instructions

This component is the data storage of the platform. Technology is Apache Cassandra.
The Cassandra cluster of 4 nodes can be started by running docker compose up.
After the nodes are healthy and up you can enter one of them and create the following keyspace and table to the Cassandra cluster by running

``$ Docker exec -it <container_name> /bin/bash``
``$ cqlsh``

Then inside cqlsh run:

``$ CREATE KEYSPACE taxiservices WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 2, 'DC2': 1} AND durable_writes = true;``

And:

``
$ CREATE TABLE taxiservices.trips (
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

``$ SELECT COUNT(*) FROM taxiservices.trips;``
