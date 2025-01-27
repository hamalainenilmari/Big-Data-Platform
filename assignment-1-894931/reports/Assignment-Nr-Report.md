# This your assignment report

It is a free form. you can add:

* your designs
* your answers to questions in the assignment
* your test results
* etc.

The best way is to have your report written in the form of point-to-point answering the assignment.

1. Explain your choice of the application domain and generic types of data to be supported and
technologies for mysimbdp-coredms. Explain your assumption about the tenant data sources and
how one could get data from the sources. Explain under which situations/assumptions, your platform
serves for big data workloads. (1 point)

The application domain for this big data platform is transportation data. This platform is tested with City of Chicago Taxi Trip data (<https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/about_data>). The reason for this domain is the relevance of this kind of data with multiple worldwide taxi services including Uber, Yango and Bolt dealing with massive volumes of real-time data. The data type of source data this platform supports is csv. It is assumed, that the source IoT data (from the taxis) is generated in csv format. Compatibility for other data types could be implemented.  The data supported is structured and semi-structured data, meaning that there are no strict restrictions for the number of columns.

Technology for data storage mysimbdp-coredms is Apache Cassandra, a distributed NoSQL database. Cassandra is chosen for this component as it offers scalability and high availability, which are essential assets for a serving as a big data -data storage.

The platform serves for big data workloads: by the 4Vs:
Volume: the amount of data being managed is large.
Variety: the data managed is mainly structured/semi-structured so variety is not a problem.
Velocity: this is the main V: there will be lots of data coming every second (real time taxi data)
Veracity: data can be incorrect/missing values (problems with driver/IoT use, time delays etc)



    Part 3 Extension (weighted factor for grades = 1)
Address the following points:
1. Using your mysimbdp-coredms, a single tenant can run mysimbdp-dataingest to create many
dierent databases/datasets. The tenant would like to record basic lineage of the ingested data,
explain what types of metadata about data lineage you would like to support and how would you do
this. Provide one example of a lineage data. (1 point)

- info found: https://github.com/rdsea/bigdataplatforms/blob/master/lecturenotes/pdfs/module2-lecture3-bigdatastoragedatabase-v0.6.pdf

2. Assume that each of your tenants/users will need a dedicated mysimbdp-coredms. Design the data
schema of service and data discovery information for mysimbdp-coredms that can be published into
an existing registry (like ZooKeeper, consul or etcd) so that you can nd information about which
mysimbdp-coredms is for which tenants/users. (1 point)
3. Explain how you would change the implementation of mysimbdp-dataingest (in Part 2) to integrate a
service and data discovery feature (no implementation is required). (1 point)
4. Assume that you have to introduce a new key component, called mysimbdp-daas, of which APIs can
be called by external data producers/consumers to store/read data into/from mysimbdp-coredms.
This component is a platform-as-a-service. Tenants can get shared or dedicated instances of
mysimbdp-daas for their usage. Assume that now only mysimbdp-daas can read and write data into
mysimbdp-coredms, how would you change your mysimbdp-dataingest (in Part 2) to work with
mysimbdp-daas, draw the updated architecture of your mysimbdp? (1 point)
5. Assume that the platform allows the customer to dene which types of data should be stored in a hot
space and which in a cold space in the mysimbdp-coredms. Provide one example of constraints based
on characteristics of data for data in a hot space vs in a cold space. Explain how would you support
automatically moving/extracting data from a hot space to a cold space. (1 point)