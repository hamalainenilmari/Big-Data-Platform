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