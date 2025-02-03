Use these following Cassandra settings:

CREATE KEYSPACE taxiservices WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 2, 'DC2': 1} AND durable_writes = true;

CREATE TABLE taxiservices.trips (
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

Clustering key trip_start_timestamp sorts the taxi trips by start time in descending order, enabling efficient queries of latest trips. Good for optimizating rides.