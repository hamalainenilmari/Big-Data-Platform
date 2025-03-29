# Assignment 3 Report - Stream and Batch Analytics

## Part 1 - Design for streaming analytics

### 1.1 Dataset for streaming analytics

We are using the same dataset as previously used in this platform, the [2024 Chicago Taxi Trip data](https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/about_data). The data contains 23 columns, including information about trip start and end times, trip distance, pickup and dropoff locations, taxi ID and total trip cost. The dataset is suitable for streaming data analytics as it can be used to simulate data streaming of taxi trips, which is a domain that generates continuous high-volume data in real time and can highly benefit from streaming analytics. The data requires immediate processing and insights to optimize the taxi business. For example, Uber handles very high volumes of data streams every day and has engineered their in-house streaming analytics platform called AthenaX to utilize the streaming data. The dataset contains potential streaming and batch (historical) analytics possibilities, which can be used to optimize the business of the taxi service provider tenant.

The component **tenantstreamapp** analyzes raw streaming data from the tenants. The component ingests raw taxi trip data from Kafka producers and produces *silver data*, which is by cleansing the data by removing invalid records e.g. without essential values. The trips are aggregated by location and time. The streaming analytics would include real-time demand analysis, where analyzing trip pickup locations would enable dynamic pricing. The component would identify high-demand areas in real-time. Other analytics could contain average number of trips per vehicle, aggregated total fates, estimated hourly revenue per vehicle, number of trips in some time window. The silver data is generated to csv files and the data sink is HDFS.

The batch streaming analytics component is **batchstreamapp** uses workflow model and analyzes the historical silver data outputted by streaming analytics to produce *gold data*. The component processes historical data periodically to generate gold data. The component analyzes larger time scale information from the silver data, with analytics for example about geographical demand areas per week days. The gold data is also stored to HDFS.

The workflow model is the following. Tenant streaming analytics component produces silver data continuously. The batch streaming analytics component schedules periodic processing of the silver data. It performs more complex aggregations and statistical analysis and generates gold data as insights and reports.

### 1.2 Messaging system and stream analytics settings

The streaming analytics component handles streaming data, which can be either keyed or non-keyed. Keyed data means that each data record is associated with a key, allowing grouping, partitioning and stateful processing. With keyeing, the streaming data can be partitioned and processed in parallel. Non-keyed data means that the data is handled as indepentent records, which enables simpler storage but less efficient partitioning and aggregations.

As keyed data allows parallel processing and grouping the data, we key the streaming data by the pickup location. This allows us to enable real-time aggregations per geographical location and generate silver data based on the analytics. Then batch analytics component can more efficiently query the silver data from HDFS to produce gold data based on the demand analytics. If the input data is missing the key, pickup community area, the data is ignored.

Message delivery guarantee means the assurance the messaging system provides about the delivery and processing of messages. Different levels of guarantees ensure that messages are reliably delivered and not lost, in certain ways. Message guarantees are the job of the messaging system, and processing guarantees are the job of the stream processing components. Message delivery guarantees include exactly once, at least once, at most once. At most once -guarantee means that the message might be not delivered at all, but no duplicate messages is possible. It is good option when duplicate processing is costly/unnecessary, such as logging. At least once -guarantee means that the message delivery is guaranteed, but it might me delivered multiple times in case of errors meaning duplicate data. It is suitable when message loss cannot be tolerated. Exactly once -guarantee is the strictest, as it ensures no message loss and no duplicates. It is used in financial transactions.

In our case, we will use the at exactly once -guarantee. It means that each message is processed exactly once, ensuring the analytical silver data correctness. It also means that in case of errors or failures, data records could be lost. We chose this guarantee, because we want to avoid duplicate data, which would corrupt the silver data and cause misleading conclusions. Missing occasional records is not so important in this case.

### 1.3 Data times and windows

The data pipeline in the platform contains several unique times associated with data. One time element is the event time, which means the time the message is produced. This time is automatically stored in the data record with as the Trip End Timestamp contains it. Other time element is the time the message is entered into the system. One time element is the time when the data is processed. Because in streaming analytics we are interested in amount of trips per geographical area per some time unit, we use the trip start timestamp as data timestamp in stream processing to aggregate the data.

In stream processing, windows are used to group the data for processing in time-bound chunks. As streams keep producing events indefinitely, windows allow us to divide this continuous flow of data into manageable discrete frames and then process them like batches. Window is a chuch of data.

There are different types of windows, with sliding and tumbling windows being the feasible possibilites in this context. Tumbling window defines a fixed-size, non-overlapping window of data. Once a window is complete, the system moves to the next, window slides forward by the window size. E.g. with 10 minute tumbling window, data would be grouped from 00:00 to 09:59, with next group being 10:00 to 19:59. Used for calculating aggregations of events over fixed time intervals. Sliding window is also a fixed-size window, but it slides over the stream at regural intervals, i.e. windows overlap. A sliding window with a slide of 30 seconds would capture the data from 0:00 to 0:59, 0:30 to 1:29, 1:00 to 1:59. Sliding windows are used for e.g. tracking moving averages, such as computing average temperature in the last 5 minutes, updating every minute.

As we want to calculate aggregations of taxi trips over fixed time intervals, we are using tumbling window. We are using tumbling windows of 30 minutes. This means that we generate analytics containing information about number of taxi trips per geographical area over 30 minutes. We use allowed lateness of 5 minutes, meaning that incoming late data is aggregated to the results, if it less than 5 minutes late. After the 30 minutes + lateness, the resulting data is stored in the HDFS silver data storage as csv, with each row corresponding to one area.

Out-of-order data records could be caused the taxi trip IoT device failures, network failures etc. This is expected, as the data is coming from distributed sources with unreliable networks. A watermark is a progress marker for stream processing that helps the system decide when to move forward to avoid waiting indefinetely for out-of-order events. It allows the system to process a late event. As we are calculating amount of trips per area over time we will be using watermarks. The watermark acts as a threshold that marks the oldest event we will still process even if the event is late. The watermark associated to data records is the taxi trip start time, as we use the window based on it. We will allow lateness of 5 minutes.

### 1.4 Performance metrics of streaming analytics

There are several important performance metris for streaming analytics for the taxi service provider tenant. An essential metric is event throughput. It is measured by the number of events processed per second per window by the streaming analytics application, indicating how efficiently the system can handle the incoming taxi trip streaming data. The tenant streaming analytics component implemented with Kafka calculates these metrics. This metric is useful for the platform, as it can indicate performance and possible slowness of the data processing.

Another important metric is data quality. It is measured by the relation of source data matching the enforced schema and data mismatching the schema. It is measured as decimal, e.g. with 1000 total records, 950 records matching schema and 50 records not matching, the data quality of this window would be logged as 0.95. This metrics is used to monitor the quality of the data and perform actions based on it. If the quality is under some agreed configuration with the tenant, we would send the silver data straight to the tenant for analysing. This metric is most useful for the tenant, as it can indicate they have some problems with producing the data.

Alongside these essential metrics, we will log amount of records ingested per window, and the pickup area and timestamp errors, which cause the data to be discarded from analytics. We also log the number of late data, and send them back to tenant for further analysis.

### 1.5 Architecture of streaming analytics service

The platform contains messaging system, of which technology is Apache Kafka. This component is reused from previous implementations. Tenants produce data to the platform by sending data records using Kafka Producers. The messaging system component contains Kafka cluster, to which the tenants producers send data for analytics. The streaming computing service of the platform is implemented with technology choice of Apache Flink, which is extremely efficient for tracking running aggregations and detecting anomalies. In real scenario, the streaming computing service would run a Flink cluster, and the tenantstreamapp would a Flink job, which is executed in the platforms Flink cluster. In this case for simplicity, the tenantstreamapp is flink job, which is executed in flink python environment. The core data management system (coredms) contains two separate data storage components; one for operational data and one for analytical data. The operational data storage is Cassandra cluster, as previously in this platform. The analytical data storage is Hadoop Distributed File System (HDFS). In real scenario a data lake would be more suitable choice for analytical data, but for simplicity we will use HDFS, which is efficient for handling large data and supporting batch analysis. The HDFS storage system is separated into two storages, silver and gold data. Tenantbatchapp is the component, which runs batch data analytics of the silver data and produces gold data. Apache Airflow is used for orchestrating the workflow, scheduling the periodic running of the tenantbatchapp.

![Platform achitecture](../images/architecture.png)

The workflow is the following. Tenants produce real time data with Kafka producers producing data into the messaging system. The tenants streaming application Flink job is running continuosly and consuming the streaming data of the tenant. The tenantstreamapp processes the data by cleaning and transforming it, and producing silver data, i.e. running aggregations of taxi trips per area over time (30 minute window) on the data. The produced silver data is sent to the tenant in real-time with Kafka and stored to the analytical data storage HDFS silver data storage. There could be specific condition under where the silver data is sent to tenant, such as specific limit for number of trips per area, so we would not sent every silver data unit. For simplicity, we assume the tenant wants all the calculated silver data, and makes decisions based on them on their own behalf. The streaming application component also calculates metrics such as data quality, and based on configurations, sends alert to the tenant about the data quality. For example, if we would have set data quality limit to 0.8 (relation of good rows to all rows), and the calculated quality would be lower, we would send alert to tenant by Kafka. The Tenantbatchapp runs periodically and consumes the silver data, generates gold data from it and stores the gold data into analytical data HDFS gold data storage.

## Part 2 - Implementation of streaming analytics

### 2.1 Tenantstreamapp

The tenantstreamapp is the component which generates the analytical silver data from the source data stream. The implementation can be found from *code/tenantstreampapp/tenantstreampp.py*.

Input streaming data has the following schema:

```json
{
  "Trip ID": string,
  "Taxi ID": string, 
  "Trip Start Timestamp": timestamp, 
  "Trip End Timestamp": timestamp, 
  "Trip Seconds": int, 
  "Trip Miles": float, 
  "Pickup Census Tract": int, 
  "Dropoff Census Tract": int, 
  "Pickup Community Area": int, 
  "Dropoff Community Area": int, 
  "Fare": float, 
  "Tips": float, 
  "Tolls": float, 
  "Extras": float, 
  "Trip Total": float, 
  "Payment Type": string, 
  "Company": string, 
  "Pickup Centroid Latitude": float, 
  "Pickup Centroid Longitude": float, 
  "Pickup Centroid Location": string, 
  "Dropoff Centroid Latitude": float, 
  "Dropoff Centroid Longitude": float, 
  "Dropoff Centroid  Location": string
}
```

The trip end timestamp is 0, as we dont yet know when does the trip end, as this is just the notification that the trip has started.

For the tenantstreamapp to be able to correctly generate the analytics from the streaming data, the input data must contain the following values. Pickup community area is essential, as it is used for the main analytics, which is the number of trips per pickup area over time. Obviously, the trip start time is also crucial. Other value used for the analytics is the trip total fare, but it is not enforced, as the main on-demand analytics can be generated without the trip costs. The times are expected to be in format "2025-03-27T09:59:22Z", from where it is assigned as the timestamp and changed to integer timestamp format.

The generated output analytics silver data is in following format:

| pickup location  | amount of trips | total fares | window start          | window end            |
|-----|----------|-----------|---------------|---------------|
| 32   | 7       | 252.50    | 1743076720000 | 1743076720000 |
| 2   | 2        | 22.15    | 1743076720000 | 1743076720000 |

The data is inserted into the silver data storage and send to the tenant without the headers.

The input data from Kafka producers is serialized into bytes. The tenantstreamapp Flink application deserealizes the data into string format, without enforcing any schema at this point. Then the tenantstreamapp validates the schema, by checking if the data contains the needed values. If trip total is missing, it is simply inserted to 0. After generating the analytics over the tumbling window, the resulting Flink Row-format data is serialized into string-format, and inserted into the HDFS silver data storage, and send to the tenant.

The logic of the streaming component is the following. After ingesting the source data from Kafka topic (in other words, after some data unit is sent), the record is assigned a timestamp for windowing. The timestamp is the trip start time, which should be really close to current time. If the data arrives late (e.g. after 10 minutes after start tiem), the data is ignored from the analytics. In the window (e.g. 30 minutes), every taxi trip started from that time is aggregated, with total fares aggregated also. The window accepts late data of 5 minutes, which it still aggregates. Then the aggregated silver data is sent to tenant. Component also calculates data quality and processing metrics. Processing metrics are logged to platform.

The generated silver data is sent to the tenant in real-time manner after each window. The data is sent using Kafka, assuming that the tenant has some component for consuming the data. The tenant would consume the analytical silver data and visualize it in some analytics component for performing business actions based on the data. The streaming analytics component also sends alert to the tenant if the data quality is under predefined configuration value. For example, if the tenant has set the data quality minimum to be 0.9, and in a window the result is lower than the limit, the platform sends Kafka message to the tenant containing information about the data quality being lower than expected. We also sent all the late data to tenant, for simplicity leaving the responsibility of making actions based on that data themselves.

### 2.2 Tenantbatchapp

The tenant implementation for batch analytics is the tenantbatchapp component, which is a Spark job. The component takes as input the generated silver data and produces gold data, which is more defined analytical data. The component sums up the fares and number of trips of the silver data. The batch analytics service provided by the platform is implemented with Apache Spark, which is a distributed computing framework for fast and large-scale data processing, especially for batch workloads. The platform hosts a Spark processing engine, and the tenantbatchapps are executed by submitting the application as Spark job to the platform engine. The tenantbatchapp is ran periodically. The component consumes all the latest generated silver data from the HDFS silver data storage. An example configuration could be that tenantstreamapp produces silver data records, which contain taxi trips statistics of an 1 hour window of different locations. Then the batch analytics component could consume the generated silver data every 24 hours, generating more detailed taxi trip statistics, enabling more in-depth, long term analysis.

The batch analytics component workflow is the following. First the platform checks for new untouchable input silver data from HDFS silver data storage. If new data is not found, the platform will stop the workflow exetution, and wait for predefined time interval, before starting the workflow again. In this small scale context, we will halt for 1 minute, but in production the interval could be for example from 10 minutes to 1 hour. If new input data is found, the workflow stores the file locations, and moves the execution to the tenantbatchapp-component, which gets the input files. Then the tenantbatchapp job is submitted to the Spark engine with the input files. After the batch analytics component has produced the gold data, the final workflow step begins. In this step the platform modifies the processed silver data file names to include a mark, that the file is processed. The file name will be transformed from "silverdata.csv" to "silverdata_processed.csv". This last step makes sure that the tenantbatchapp does not process the same silver data twice with duplicated data and produce incorrect, misleading analytics.

The underlying workflow orchestrator is implemented with Apache Airflow, which is a platform for automating and managing data pipelines. Airflow works by concepts of directed acyclic graphs with each task containing possible dependencies to others. The batch analytics workflow is defined as Airflow DAG, and it is scheduled to run every minute. Again, of course, in a real production environment this schedule would be different.

### 2.3 Performance testing

The test environment for testing the streaming analytics contains tenant data producers, tenantstreamapp, coredms analytical data storage HDFS for silver (and gold data). The streaming data is simulated by reading the Chicago Taxi Trip data and sending data row by row to the Kafka topic. As we want to simulate on demand analytics of taxi trips, we will modify the Chicago data in a way, that the trip start time is current time and end time is 0, as the taxi trip has just started.

As the main focus of this testing is the streaming analytics, we will not be storing the operational data into Cassandra data storage, and we will not be performing the batch analytics.

We have the following platform configuration:

* 1 kafka brokers
* topic partioined into 1
* replication factor of 1
* flink job parallellism of 1
* tumbling window of 1 minute

We are using limited computing power because of local testing.

For testing, we will use tumbling windows of 1 minutes, with window input data allowed lateness being 15 seconds and record timestamp lateness of 1 minute. This means that if the data arrives more than 15 seconds later than the timestamp (trip start datetime), the data is discarded from window and if it arrives later than 1 minute it is discarded totally.


test 1
We will start the testing by using 1 producer producing record of data every second. The following is a log file containing metrics of single window:

|window|records per window|records processed per second|rows discarded|data quality|pickup area errors|timestamp errors|
|-----|----------|-----------|---------------|---------------|-------------|------------|
|1743261840000|38|0.6333333333333333|2|0.9473684210526316|2|0|

CPU and memory usage both rose up approximately 10 %. The log file is 2025-03-29--15/test1.log

test2
For the next test, we will use tumbling window of 60 seconds, 1 producer producing two messages per second.

metrics:

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|--------------|--------------------|----------------|
| 1743263880000 | 114               | 1.9                         | 6             | 0.9473684210526316 | 6                  | 0              |


mem 74, cpu 20

test3
Next test will be 1 producer producing 10 messages a second (10msg total / s).

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality        | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|----------------------|--------------------|----------------|
| 1743264420000 | 545               | 9.083333333333334           | 20            | 0.963302752293578    | 20                 | 0              |

CPU and memory again 10 &.
test4
Next test will be  producers, each producing 10 messages a second (50 msg total / s)

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality        | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|----------------------|--------------------|----------------|
| 1743264720000 | 2752              | 45.86666666666667           | 72            | 0.9738372093023255   | 72                 | 0              |


CPU raised 20 %.
The data is starting to show meaningful statistics:

Top areas:

Silver data: 8,497,7125.7900390625,1743264720000,1743264780000'
Silver data: 32,411,6057.759765625,1743264720000,1743264780000'

| pickup location  | amount of trips | total fares | start          | end            |
|-----|----------|-----------|---------------|---------------|
|76|273|nan|1743090780000|1743090840000|
|32|183|nan|1743090780000|1743090840000|

Opposed to lowest:

| pickup location  | amount of trips | total fares | start          | end            |
|-----|----------|-----------|---------------|---------------|
|15|1|31.25|1743090780000|1743090840000|
|30|1|12.75|1743090780000|1743090840000|

Silver data: 14,1,7.75,1743264840000,1743264900000'

test 5
Next test 5 producers producing 20 messages a second (total 100 msg / s)

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality        | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|----------------------|--------------------|----------------|
| 1743265020000 | 4063              | 67.71666666666667           | 112           | 0.9724341619492985   | 112                | 0              |


CPU rose up to 50 %. 

Next test 5 producers producing without sleep

Aftering producing for a little over 2 minutes, the platform started to halt. As we are running locally, we cannot say which component was the first to freeze. It is Kafka or the Flink. But, because HDFS contains only 2 silver data files, we can assume that the flink job freezed as it should have started writing to the third file as the third window was started.

The CPU raised up to 100 %. Memory raised only 10 %.

In this kind of small simulated test environment where the tests were run for couple of minutes, not very detailed analytics can be made (can't take into account real time of trip, events in the city, weather etc.), but this shows that if the silver data was for example for 15 minutes of trips, the data would contain useful information for analytics.


### 2.4 Wrong data

As the analytical data needs pickup location area and trip start timestamp, we will simulate situtation where wrong data is sent to the platform in different amounts. As the streaming analytics component is made in a secure way, all the data it needs (pickupp location, timestamps) are verified to be in the input data. If the input data is missing those or they are in wrong format, the component will ignore the record. We will show it. 

First we will send data that is missing pickup location area.

Sending 1 message a second.

wrong_tests

test1.log

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality        | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|----------------------|--------------------|----------------|
| 1743266100000 | 44                | 0.7333333333333333          | 10            | 0.7727272727272727   | 10                 | 0              |


Then we will send data that is missing start timestamp.

This data error will actually not get logged in current implementation, as the data is discarded right away because it cant be assigned a timestamp.

test 2
Then we will send data that has pickup location as int instead of string.

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality        | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|----------------------|--------------------|----------------|
| 1743266520000 | 23                | 0.38333333333333336         | 2             | 0.9130434782608696   | 2                  | 0              |

test3 
We will send data where every second record has no pickup area location

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality        | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|----------------------|--------------------|----------------|
| 1743266880000 | 58                | 0.9666666666666667          | 29            | 0.5                  | 29                 | 0              |

test4
Lastly, we will send data where every row is missing (expect for start time so wont get discarded straight away), with 1 producer producing 20 messages a second

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality        | Pickup Area Errors | Timestamp Errors |
|--------------|-------------------|-----------------------------|---------------|----------------------|--------------------|----------------|
| 1743267480000 | 529               | 8.816666666666666           | 529           | 0.0                  | 529                | 0              |



### 2.5 Performance with tenantstreamap parallellism configurations

Two parallel producers, with two parallel tenantstreamapps running, one for each tenant.

20 messages a second each
Both parallellism 1

test 0
tenant 1:

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743273000000 | 1043               | 17.383333333333333           | 0              | 1.0          | 0                  | 0                |


tenant2 :

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743273000000 | 1042               | 17.366666666666667           | 0              | 1.0          | 0                  | 0                |

test2

Both parallellism 2

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743272580000 | 1022               | 17.033333333333              | 0              | 1.0          | 0                  | 0                |


| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743272580000 | 1031               | 17.183333333333334           | 0              | 1.0          | 0                  | 0                |



Both parallellism 4, with 20 messages a second for each

cpu raiset 30-40 %

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743271920000 | 961                | 16.016666666666666           | 0              | 1.0          | 0                  | 0                |


| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743271920000 | 967                | 16.116666666666667           | 0              | 1.0          | 0                  | 0                |

Both parallellism 8

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743272220000 | 707                | 11.783333333333333           | 0              | 1.0          | 0                  | 0                |

| Window        | Records per Window | Records Processed per Second | Rows Discarded | Data Quality | Pickup Area Errors | Timestamp Errors |
|---------------|--------------------|------------------------------|----------------|--------------|--------------------|------------------|
| 1743272220000 | 676                | 11.266666666666667           | 0              | 1.0          | 0                  | 0                |

Several possible reasons:

 task scheduling and coordination overhead increased
 resource usage: cpu, memory, networks


## Part 3 - Extension

### 3.1 API for batch data ML

### 3.2 Bounded data tenantbatchapp trigger

### 3.3 Critical condition detection architecture

### 3.4 Different schemas

### 3.5 End-to-end exactly once delivery