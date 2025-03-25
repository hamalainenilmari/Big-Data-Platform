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



### 1.4 Performance metrics of streaming analytics

### 1.5 Architecture of streaming analytics service

Tenant data sources
Messaging system
streaming computing service
tenantstreamapp
tenantbatchapp
mysimbdp-coredms

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