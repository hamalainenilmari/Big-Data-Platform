# Assignment 3 Report - Stream and Batch Analytics

## Part 1 - Design for streaming analytics

### 1.1 Dataset for streaming analytics

We are using the same dataset as previously used in this platform, the [2024 Chicago Taxi Trip data](https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/about_data). The data contains 23 columns, including information about trip start and end times, trip distance, pickup and dropoff locations, taxi ID and total trip cost. The dataset is suitable for streaming data analytics as it can be used to simulate data streaming of taxi trips, which is a domain that generates continuous high-volume data in real time and can highly benefit from streaming analytics. The data requires immediate processing and insights to optimize the taxi business. For example, Uber handles very high volumes of data streams every day and has engineered their in-house streaming analytics platform called AthenaX to utilize the streaming data. The dataset contains potential streaming and batch (historical) analytics possibilities, which can be used to optimize the business of the taxi service provider tenant.

The component **tenantstreamapp** analyzes raw streaming data from the tenants. The component ingests raw taxi trip data from Kafka producers and produces *silver data*, which is by cleansing the data by removing invalid records e.g. without essential values. The trips are aggregated by location and time. The streaming analytics would include real-time demand analysis, where analyzing trip pickup locations would enable dynamic pricing. The component would identify high-demand areas in real-time. Other analytics could contain average number of trips per vehicle, aggregated total fates, estimated hourly revenue per vehicle, number of trips in some time window. The silver data is generated to csv files and the data sink is HDFS.

The batch streaming analytics component is **batchstreamapp** uses workflow model and analyzes the historical silver data outputted by streaming analytics to produce *gold data*. The component processes historical data periodically to generate gold data. The component analyzes larger time scale information from the silver data, with analytics for example about geographical demand areas per week days. The gold data is also stored to HDFS.

The workflow model is the following. Tenant streaming analytics component produces silver data continuously. The batch streaming analytics component schedules periodic processing of the silver data. It performs more complex aggregations and statistical analysis and generates gold data as insights and reports.

### 1.2 Messaging system and stream analytics settings

The streaming analytics component handles streaming data, which can be either keyed or non-keyed. Keyed data means that each data record is associated with a key, allowing grouping, partitioning and stateful processing. With keyeing, the streaming data can be partitioned and processed in parallel. Non-keyed data means that the data is handled as indepentent records, which enables simpler storage but less efficient partitioning and aggregations.

As keyed data allows parallel processing and grouping the data, we key the streaming data by the pickup location. This allows us to enable real-time aggregations per geographical location and generate silver data based on the analytics. Then batch analytics component can more efficiently query the silver data from HDFS to produce gold data based on the demand analytics.

Message delivery guarantee means the assurance the messaging system provides about the delivery and processing of messages. Different levels of guarantees ensure that messages are reliably delivered and not lost, in certain ways. Message guarantees are the job of the messaging system, and processing guarantees are the job of the stream processing components. Message delivery guarantees include exactly once, at least once, at most once. At most once -guarantee means that the message might be not delivered at all, but no duplicate messages is possible. It is good option when duplicate processing is costly/unnecessary, such as logging. At least once -guarantee means that the message delivery is guaranteed, but it might me delivered multiple times in case of errors meaning duplicate data. It is suitable when message loss cannot be tolerated. Exactly once -guarantee is the strictest, as it ensures no message loss and no duplicates. It is used in financial transactions.

In our case, we will use the at least once -guarantee. It means that even in case of failures during message processing, no data is lost.

### 1.3 Data times and windows

The data pipeline in the platform contains several unique times associated with data. One time element is the event time, which means the time the message is produced. This time is automatically stored in the data record with rounded value (15 minutes), as the Trip End Timestamp contains it. Other time element is the time the message is entered into the system. One time element is the time when the data is processed. Because in streaming analytics we are interested in amount of trips per geographical area per some time unit, we use the trip end timestamp in stream processing to aggregate the data.

In stream processing, windows are used to group the data for processing in time-bound chunks. As streams keep producing events indefinitely, windows allow us to divide this continuous flow of data into manageable discrete frames and then process them like batches. Window is a chuch of data.

There are different types of windows, with sliding and tumbling windows being the feasible possibilites in this context. Tumbling window defines a fixed-size, non-overlapping window of data. Once a window is complete, the system moves to the next, window slides forward by the window size. E.g. with 10 minute tumbling window, data would be grouped from 00:00 to 09:59, with next group being 10:00 to 19:59. Used for calculating aggregations of events over fixed time intervals. Sliding window is also a fixed-size window, but it slides over the stream at regural intervals, i.e. windows overlap. A sliding window with a slide of 30 seconds would capture the data from 0:00 to 0:59, 0:30 to 1:29, 1:00 to 1:59. Sliding windows are used for e.g. tracking moving averages, such as computing average temperature in the last 5 minutes, updating every minute.

As we want to calculate aggregations of taxi trips over fixed time intervals, we are using tumbling window. We are yet to determine if use time-based (window of 1, 10, 60 minute?) or count-based (window of 100, 500, 1000 trips?).

Out-of-order data records could be caused the taxi trip IoT device failures, network failures etc. This is expected, as the data is coming from distributed sources with unreliable networks.

A watermark is a progress marker for stream processing that helps the system decide when to move forward to avoid waiting indefinetely for out-of-order events. It allows the system to process a late event. As we are calculating amount of trips per area over time we will be using watermarks. The watermark acts as a threshold that marks the oldest event we will still process even if the event is late. 
TODO add watermark info

### 1.4 Performance metrics of streaming analytics

There are several important performance metris for streaming analytics for the taxi service provider tenant. An essential metric is event throughput. It measures the number of events processed per second by the streaming analytics application, indicating how efficiently the system can handle the incoming taxi trip streaming data. We can measure it with Kafka or Flink.

End-to-end latency. Time taken from when event is produced by tenant until it is processed and stored into silver data location in HDFS. It is used to ensure real-time insights are generated within acceptable delays. The real-time analytics are not useful, if they are not provided in real-time also.

Processing time per event. Time taken to process a single event withing the streaming analytics pipeline. It helps in optimizing recources. Ensures system can keep up with incoming data rates.

### 1.5 Architecture of streaming analytics service

Tenant data sources
Messaging system
streaming computing service
tenantstreamapp
tenantbatchapp
mysimbdp-coredms

The platform contains messaging system, of which technology is Apache Kafka. Tenants produce data to the platform by sending data records using Kafka Producers. The messaging system component contains Kafka cluster, to which the tenants producers send data. The streaming computing service of the platform is implemented with technology choice of Apache Flink, which is extremely efficieny for tracking running aggregations and detecting anomalies. The streaming computing service runs Flink cluster (?), and the tenantstreamapp is a Flink job, which is executed in the platforms Flink cluster. The core data management system (coredms) contains two separate data storage components; one for operational data and one for analytical data. The operational data storage is Cassandra cluster, as previously in this platform. The analytical data storage is Hadoop Distributed File System (HDFS). In real scenario a data lake would be more suitable choice for analytical data, but for simplicity we will use HDFS, which is efficient for handling large data and supporting batch analysis. The HDFS storage system is separated into two storages, silver and gold data. Tenantbatchapp is the component, which runs batch data analytics of the silver data and produces gold data. Apache Airflow is used for orchestrating the workflow, scheduling the periodic running of the tenantbatchapp.

![Platform achitecture](../images/architecture.png)

The workflow is the following. Tenants produce real time data with Kafka producers producing data into the messaging system. The tenants streaming application Flink job is running on the platform Flink cluster, and consuming the streaming data of the tenant. The tenantstreamapp processes the data by cleaning it and stores the processes data into mysimbdp-coredms Cassandra cluster. At the same time, the tenantstreamapp is producing silver data, i.e. running aggregations of taxi trips per area over time on the data. The produced silver data is sent to the tenant in real-time and stored to the analytical data storage HDFS silver data st0rage. Tenantbatchapp runs periodically and consumes the silver data, generates gold data from it and stores the gold data into analytical data HDFS gold data storage.

## Part 2 - Implementation of streaming analytics

### 2.1 Tenantstreamapp

### 2.2 Tenantbatchapp

### 2.3 Performance testing

### 2.4 Wrong data

### 2.5 Performance with tenantstreamap parallellism configurations

## Part 3 - Extension

### 3.1 API for batch data ML

### 3.2 Bounded data tenantbatchapp trigger

### 3.3 Critical condition detection architecture

### 3.4 Different schemas

### 3.5 End-to-end exactly once delivery