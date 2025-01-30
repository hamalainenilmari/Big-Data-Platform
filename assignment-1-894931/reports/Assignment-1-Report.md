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
