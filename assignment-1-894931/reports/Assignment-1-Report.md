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