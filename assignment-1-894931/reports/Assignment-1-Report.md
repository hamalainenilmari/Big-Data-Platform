# Assignent 1 report - Building Your Big Data Platforms

It is a free form. you can add:

* your designs
* your answers to questions in the assignment
* your test results
* etc.

The best way is to have your report written in the form of point-to-point answering the assignment.

## Part 1 - Design

1. Explain your choice of the application domain and generic types of data to be supported and
technologies for mysimbdp-coredms. Explain your assumption about the tenant data sources and
how one could get data from the sources. Explain under which situations/assumptions, your platform
serves for big data workloads. (1 point)

The application domain for this big data platform is transportation. The reason for this domain is the relevance, with multiple worldwide taxi services including Uber, Yango and Bolt dealing with massive volumes of real-time data. The platform supports structured and semi-structured data, which means that new fields/columns can be added to the data storage tables dynamically. This is because of the generated IoT data in transportation domain, as data unit structures may vary geologically and over time.

Technology for data storage (mysimbdp-coredms) of the platform is Apache Cassandra, a distributed NoSQL database. Cassandra is chosen for this data storage as it offers scalability and high availability, which are essential assets for a serving as a big data -data storage. It also excels in high-speed write operations, which is essential in handling real-time data streams.

Tenant data sources are (e.g. the IoT taxi trip data) ingested into the data storage of the platform using Apache Kafka, distributed event streaming platform. Tenants send the generated data with Kafka Producer into the Kafka server this platform provides, using predetermined Kafka topics and configurations.

The platform is focused on real-time data streams. Out of the 4Vs in big data, this platform serves especially for Velocity: the platform is optimized to handle lots of data coming in real-time. The platform also supports Variety and Veracity, as the data units handled are not required to be strictly in the same structure. The data storage of the platform handles the Volume, as the amount of data gathered can grow huge.

2. Design and explain the interactions among main platform components in your architecture of mysimbdp. Explain how would the data from the sources will be ingested into the platform. Explain which would be the third parties (services/infrastructures) that you do not develop for your platform

The platform consists of two components:

* **mysimbdp-dataingest**: this component is responsible ETL. It ingests the source data from tenants into the platform. Ensures reliable data transmission and does the necessary processing including modifying data types and column names to match Cassandra table schemas.
* **mysimbdp-coredms**: this component is responsible for managing and storing the data.

Data ingestion pipeline:

Tenants use Kafka Producer Python library provided by [Confluent](https://developer.confluent.io/get-started/python/#introduction) to send the source data into mysimbdp-dataingest, which hosts a Kafka server and Kafka Consumer (by Confluent). The consumer subscribes and listens to the relevant topics continuously. Upon receiving the raw data, the consumer parses, validates and transforms it and inserts the data into a corresponding Cassandra table of mysimbdp-coredms.

The services not developed to the platform at this point include external data processing and analytics components. Additionally, proper security and logging mechanisms are not developed.

![Platform architecture](../architecture.png)

3. Explain a conguration of a cluster of nodes for mysimbdp-coredms so that you prevent a singlepoint-of-failure problem for mysimbdp-coredms for your tenants. (1 point)

be deployed across 2 data centers. This ensures that if one node goes down in a data center, there will be another node available to serve data in the data center. Additionally, even if an entire data center fails, there will still be nodes serving in the other data center left ensuring high availability and fault tolerance.
Nodes are dependent only on nodes within same data center, meaning scaling up or down the data centers won’t affect other centers or nodes. While this configuration provides good availability and fault tolerance, there could be more nodes and data centers configured for even better availability and fault tolerance, however in this case hardware limits the amount of nodes to 4.

4. You decide a pre-dened level of data replication for your tenants/customers. Explain the level of replication in your design, how many nodes are needed in the deployment of mysimbdp-coredms for your choice so that this component can work property (e.g., the system still supports redundancy in the case of a failure of a node). (1 point)

In this design, the replication factor for Cassandra is chosen to be 3, which means that each data unit will be replicated on three different nodes in the Cassandra cluster. For this replication to work, there will need to at least 4 Cassandra nodes deployed to ensure that data is replicated to 3 nodes even if one node fails and there are still multiple nodes left to serve the data. This replication ensures high availability and fault tolerance. Cassandra is configured to use NetworkTopologyStrategy where 2 replicas are chosen to be in the data center 1 and 1 replica in data center 2.

5. Consider the data center hosting your platform, the locations of tenant data sources and the network between them. Explain where would you deploy mysimbdp-dataingest to allow your tenants using mysimbdp-dataingest to push data into mysimbdp, based on which assumptions you have. Explain the performance pros and cons of the deployment place, given the possibilities you have. (1 point)

The platform is deployed and hosted in Google Cloud Platform with two virtual machines, one for mysimbdp-dataingest and one for mysimbdp-coredms. The tenant data sources are expected to be geographically distributed as the domain is transportation (taxi trips etc), meaning that the data would be sent to this platforms Kafka server from different locations. Alternatively, the transportation data could first be sent to a tenants own server, and then forwarded to this platforms Kafka server.
Pros of deploying dataingest in cloud contain especially the scalability. As source data is coming in almost real-time, during high traffic peaks (huge events, holidays etc.) the number of Kafka brokers could be scaled up to ensure high availability and throughput. Additionally, during low traffic times (e.g. nighttime) dataingest could be scaled down to reduce cloud hosting costs.

Hosting dataingest on a cloud service would also enable possibility to distribute the servers geographically across continents, enabling faster communication between geographically distributed tenants and the servers. Cloud services also enable monitoring and automation of components with less work.

Cons of deploying in the cloud contain the high costs of hosting the ingesting component and if the servers are not geographically distributed enough, there may be communication latencies with some tenants. There can also be issues with data regulation when storing data in different continents. Also energy efficiency of the hosting hardware infrastructure may vary.

## Part 2 - Implementation

1. Design, implement and explain one example of the data schema/structure for a tenant whose data will be stored into mysimbdp-coredms.

An example source data of tenant will be taxi trip data from Chicago, USA. The tenant owns multiple taxi service companys. The taxis will produce the raw data with following attributes and data types:

`Trip ID,Taxi ID, Trip Start Timestamp, Trip End Timestamp, Trip Seconds, Trip Miles, Pickup Census Tract, Dropoff Census Tract, Pickup Community Area, Dropoff Community Area, Fare, Tips, Tolls, Extras, Trip Total, Payment Type,  Company, Pickup Centroid Latitude, Pickup Centroid Longitude, Pickup Centroid Location, Dropoff Centroid Latitude, Dropoff Centroid Longitude, Dropoff Centroid Location`

The platform will ingest the raw data and do some simple data wrangling and drop some attributes. The attributes to skip are Pickup Census Tract, Dropoff Census Tract, Pickup Centroid Location,
Dropoff Centroid  Location. These values are produced by the IoT machines but important enough to store in the platform.

The final data unit to be stored in the platforms data storage is the following:

* Trip ID: text
* Taxi ID: text
* Trip Start Timestamp: timestamp
* Trip End Timestamp: timestamp
* Trip Seconds: int
* Trip Miles: float
* Pickup Community Area: float
* Dropoff Community Area: float
* Fare: float
* Tips: float
* Tolls: float
* Extras: float
* Trip Total: float
* Payment Type: text
* Company: text
* Pickup Centroid Latitude: double
* Pickup Centroid Longitude: double
* Dropoff Centroid Latitude: double
* Dropoff Centroid Longitude: double

The schema contains 19 attributes. The schema contains basic trip information: identifying of the trip and taxi, the time statistics of the trip, the trip length and pickup/dropoff points and cost information. Additionally, the schema contains payment type and taxi company providing the trip. The schema also contains exact coordinates of pickup and dropoff points for precise demand analysis.

Given the data schema/structure of the tenant (Part 2, Point 1), design a strategy for data
partitioning/sharding, explain the goal of the strategy (performance, data regulation and/or what),
and explain your implementation for data partitioning/sharding together with your design for
replication in Part 1, Point 4, in mysimbdp-coredms.

The taxi trip data will be partitioned across multiple Cassandra nodes using **Pickup Community Area** as the partitioning key.
This partitioning will distribute the data based by the geographical location of start of the taxi trip. All trips with the same staring location will
be stored in the same node, meaning more efficient queries of taxi trips based on the location. This will enable efficient analysis of in-demand areas
enabling the tenant to locate taxis nearby.

The data storage is composed of 4 Cassandra nodes with a replication factor of 3. The data is partitioned by the geographical pickup location and
each partition is replicated across 3 different Cassandra nodes in 2 data centers.

3. Assume that you play the role of the tenant, emulate the data sources with the real selected dataset
and write a mysimbdp-dataingest that takes data from your selected sources and stores the data into
mysimbdp-coredms. Explain what would be the atomic data element/unit to be stored. Explain
possible consistency options for writing data in your mysimdbp-dataingest

The tenant dataset is the [taxi trip data of Chicago](https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/about_data).
The implementation for the mysimbdp-dataingest can be found on *code/mysimbdp-dataingest/*.

The consistency level in Cassandra means the number of replicas/nodes must acknowledge a read or write operation before it is succesful.
Consistency level can bet set for both read and write operations. The consistency level for both read and write is chosen to be Quorum,
which means out of all the replication nodes, majority must respond before the operation is succesful.
This means in this platform out of the 3 data replication nodes, 2 nodes must respond. This consistency level enables balance between availability and risk of data loss, and hence
is a good option for this domain.

Alternative consistency levels would be All and and One. All would mean that all 3 replication nodes must acknowledge the operation. In case of node failures
this would mean that the operation wont go through, which decreases the platforms availability. As the data domain is transportation and not, example financial transactions, this consistency level would be unnecessarily high. Consistency level of one would enable the fastest reads, but could return stale data.

4. Given your deployment environment, measure and show the performance (e.g., response time,
throughput, and failure) of the tests for 1,5, 10, .., n of concurrent mysimbdp-dataingest writing data
into mysimbdp-coredms with dierent speeds/velocities together with the change of the number of
nodes of mysimbdp-coredms. Indicate any performance dierences due to the choice of consistency
options. (1 point)

Avg. data produce velocity for each of these test case (runtime of a single producer): finished producing input data at 1738592753.3659682, total runtime: 80.86 s
(different log mechanism)
Each consumer polls for new messages every second

**Different number of data coming in and different number of kafka consumers:**

Number of consumers	At least as many partitions as consumers for parallelism. We use as 1.5x number of consumers
Kafka broker is a server that stores and serves messages. Brokers work together in a Kafka cluster.
Kafka replication factor is 3.

partitions: 15

5 Kafka brokers, 4 Cassandra nodes (DC1: 2, DC2: 2, replication DC1: 2, DC2: 1), Consistency: Quorum, Kafka consumers poll every second

1 Kafka Producers producing 10 000 rows of data each, 1 Kafka Consumers
- log0.log

1 Kafka Producers producing 10 000 rows of data each, 5 Kafka Consumers
- log1.log

10 Kafka Producers producing 5 000 rows of data each, 10 Kafka Consumers
- log2.log

10 Kafka Producers producing 10 000 rows of data each, 15 Kafka Consumers
- log3.log

30 Kafka Producers producing 10 000 rows of data each, 30 Kafka Consumers
- log4.log


**Different poll time of consumers:**

3 Kafka brokers, 4 Cassandra nodes, Consistency: Quorum, 20 Kafka Producers producing 10 000 rows of data each (), 10 Kafka Consumers (poll 1s)
- log4.log

3 Kafka brokers, 4 Cassandra nodes, Consistency: Quorum, 20 Kafka Producers producing 10 000 rows of data each (), 10 Kafka Consumers poll every 0.1s
- log5.log
=> 10%? increase in speed

3 Kafka brokers, 4 Cassandra nodes, Consistency: Quorum, 20 Kafka Producers producing 10 000 rows of data each (), 10 Kafka Consumers poll every 0.01s
- log6.log
=> no notable speed increase anymore

3 Kafka brokers, 4 Cassandra nodes, Consistency: Quorum, 20 Kafka Producers producing 10 000 rows of data each (), 10 Kafka Consumers poll every 2s
- log7.log
=> not much difference to 1s poll time

**Different write consistencies:**

3 Kafka brokers, 4 Cassandra nodes, 5 Kafka Producers producing 10 000 rows of data each, 10 Kafka Consumers poll every 0.1s

Consistency: Any
- log9.log

Consistency: One
- log10.log
 
Consistency: Quorum
- log11.log

Consistency: All
- log12.10

**Different number of Cassandra nodes:**

3 Kafka brokers, Consistency: Quorum, 5 Kafka Producers producing 10 000 rows of data each, 10 Kafka Consumers poll every 0.1s

2 Cassandra nodes (2 in DC1)
- log13.log
- => CREATE KEYSPACE taxiServices WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

3 Cassandra nodes (3 in DC1) => CREATE KEYSPACE taxiServices WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
- log14.log

3 Cassandra nodes (3 in DC1) => CREATE KEYSPACE taxiServices WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
- log15.log
=> less replication, more fast? only a little

3 Cassandra nodes (2 in DC1, 1 in DC1), CREATE KEYSPACE taxiServices WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 2, 'DC2': 1} AND durable_writes = true;
- log16.log
=> 

3 Cassandra nodes (2 in DC1, 1 in DC1), CREATE KEYSPACE taxiServices WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1, 'DC2': 1} AND durable_writes = true;
- log17.log
=> 

4 Cassandra nodes (2 in DC1, 2 in DC2)
- log1.log


5 Cassandra nodes (3 in DC1, 2 in DC2)
=> VM cant take it anymore with the memory configurations, changed HEAP_NEWSIZE: 128M and MAX_HEAP_SIZE: 2G -- not working still

6 Cassandra nodes


1. Observing the performance and failure problems when you push a lot of data into mysimbdpcoredms (you do not need to worry about duplicated data in mysimbdp), propose the change of your
deployment to avoid such problems (or explain why you do not have any problem with your
deployment). (1 point)

Not many failures happened apart from the individual incorrect data types in source data.

## Part 3 Extension

1. Using your mysimbdp-coredms, a single tenant can run mysimbdp-dataingest to create many
dierent databases/datasets. The tenant would like to record basic lineage of the ingested data,
explain what types of metadata about data lineage you would like to support and how would you do
this. Provide one example of a lineage data. (1 point)

The platform could support multiple types of metadata for the data ingested into it. An example of a data lineage for a taxi service provider which sends their raw
data to this platform's data ingestion would following:

The tenant information would be stored, containing which tenant and which user of tenant has stored specific data.
For example, when the tenant starts producing the source data by Kafka Producer from their server, the user who started the action
would be stored. Also the source data type would be stored, with the schema of the data units. Then the processing done by the mysimbdp-dataingest
would be stored, which could include ignoring unnecessary (if predetermined) columns, changing data types (e.g. timestamps, int to float) and data filtering.
Then the ingestion result data would be stored, which would contain total numbers of input data ingested, total number of succesful data storage inserts and
errors. Additionally, the ingestion start timestamp and end timestamps would be stored. Also the used Cassandra keyspace(s) and table(s) would be stored.

The data lineage could be stored in JSON format like:

{
  "tenant_id": "tenant_a",
  "user_id": "user_1",
  "source": {
    "schema": ["trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",  
        "trip_miles", "pickup_community_area", "dropoff_community_area", "fare", "tips",  
        "tolls", "extras", "trip_total", "payment_type", "company",  
        "pickup_centroid_latitude", "pickup_centroid_longitude",  
        "dropoff_centroid_latitude", "dropoff_centroid_longitude"],
    "type": "csv"
  },
  "processing": {
    "trip_start_timestamp": "modify_to_timestamp",
    "trip_end_timestamp": "modify_to_timestamp",
    "trip_miles": "modify_to_int",
    "dropoff_centroid_latitude": "drop"
    "dropoff_centroid_longitude": "drop"
  },
  "ingestion_result": {
    "total_rows_processed": 10000,
    "total_rows_discarded": 0,
    "successful_db_inserts": 9990,
    "failed_db_inserts": 10,
    "success_rate": "99.9%",
    "rows_inserted_per_second": 555
  },
  "ingestion_start": "2025-02-04T20:00:15Z",
  "ingestion_end": "2025-02-04T22:00:00Z",
  "cassandra_keyspaces": ["taxiservices"],
  "cassandra_tables": ["trips"]
}
  
2. Assume that each of your tenants/users will need a dedicated mysimbdp-coredms. Design the data
schema of service and data discovery information for mysimbdp-coredms that can be published into
an existing registry (like ZooKeeper, consul or etcd) so that you can nd information about which
mysimbdp-coredms is for which tenants/users. (1 point)

For managing multiple mysimbdp-coredms instances, the platform would need to handle and store the schema of service and data
discovery information for each tenant in a configuration synchronization service like Apache ZooKeeper. These kind of services
enable centralized and consistent way for managing configuration and metadata about multiple services like mysimbdp-coredms'.
A tenant's mysimbdp-coredms information would contain essential information about the tenant such as
tenant name, id and users. Information about the tenant specific coredms(s) would contain: number of cassandra nodes, data centers, replication factor, keyspaces, tables
Information about the VM instance running the coredms would contain the addresss (ip:port),
firewall rules (protocols accepted, ports etc), hardware specs (CPU, RAM etc) and the cost of the machine. Also metadata such as tenant creation date, location would be stored.
Also coredms status would be stored, including active, stopped, removed.

The tenant information could be stored in JSON like:

{
    "tenant_name": "taxi_service_provider_abc",
    "tenant_id": "abc123",
    "status": "active",
    "users": [
        {
            "user_id": "id123",
            "role": "admin"
        },
        {
            "user_id": "id456",
            "role": "user"
        }
    ],
    "databases": [
        {
            "keyspace:": "taxiservices",
            "tables": ["trips", "reviews"],
        }
    ],
    "db_nodes": 4,
    "replication": 3,
    "db_data_centers: ["DC1", "DC2"],
    "virtual_machine": {
        "ip": "127.0.0.1",
        "cassandra_port": 9042,
        "ports_listened_on": ["9042", "9042", "9044"],
        "protocols": ["http", "https"],
        "vm_cost_annual_euro": 10000,
        "cpu_cores": 4,
        "memory": 16,
        "disk": 400
    },
    "creation_date": "2025-02-04T20:00:15Z",
    "location": "USA"
}

1. Explain how you would change the implementation of mysimbdp-dataingest (in Part 2) to integrate a
service and data discovery feature (no implementation is required). (1 point)

Currently, mysimbdp-dataingest contains hardcoded environmental variables to use when connecting to the specific mysimbdp-coredms instance.
This means that the coredms instanece ip and port are manually inserted, and cassandra keyspace and table are manually inserted. The use of the tenant service and data discovery
feature would enable retrieving these values from the centralized configuration management service (e.g. ZooKeeper) and automatically inserting the correspoding coredms data into
mysimbdp-dataingest configurations.
This would be implemented in a way, where the platform would also host ZooKeeper service, and the dataingest would ask for the configurations from there.
Mysimbdp-dataingest would query for the corresponding coredms data by the tenant id, and the ZooKeeper service would return it, if values are right, for example status is active.
When dataingest receives new data from tenant data sources, it would first query the data by tenant id, and then if the source data matches the configurations in the tenant data,
the data is inserted (e.g. the corresponding table exists?)

1. Assume that you have to introduce a new key component, called mysimbdp-daas, of which APIs can
be called by external data producers/consumers to store/read data into/from mysimbdp-coredms.
This component is a platform-as-a-service. Tenants can get shared or dedicated instances of
mysimbdp-daas for their usage. Assume that now only mysimbdp-daas can read and write data into
mysimbdp-coredms, how would you change your mysimbdp-dataingest (in Part 2) to work with
mysimbdp-daas, draw the updated architecture of your mysimbdp? (1 point)

The mysimbdp-daas would be implemented as a platform-as-a-service inside a platform. The source data ingestion would still be done
by the mysimdp-dataingest. The daas would provide APIs for tenants to use for ingesting data. The underlying technology would still be Kafka.
The daas would also provide APIs for reading the data. This would require a new component for querying the coredms and returning the results to the API.

![Platform architecture with daas](../architecture_daas.png)

1. Assume that the platform allows the customer to dene which types of data should be stored in a hot
space and which in a cold space in the mysimbdp-coredms. Provide one example of constraints based
on characteristics of data for data in a hot space vs in a cold space. Explain how would you support
automatically moving/extracting data from a hot space to a cold space. (1 point)