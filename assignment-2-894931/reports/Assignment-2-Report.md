# Assignment 2 report - Working on Data Ingestion and Transformation

## Part 1 - Batch data ingestion and transformation

### 1.1 Tenant service agreement

This platform has different constraints for source files defined in each tenant's service agreement.
This way each tenant has different level of the platform, defined by their customer level. The service agreement
is stored as an JSON file.

The number of different input file types supported by the platform depends on the service agreement.
Minimum service supports only 1 file-format, such as CSV. With higher level service, the platform will support
multiple source file formats, such as CSV, JSON, XML and txt. The number of different source file types supported means more freedom to the tenant and due depends on the service level.

Another service level quality constraint is ingest speed. This is implemented by defining an interval
for invoking the tenant's pipeline for ingesting new files. Higher service level means
shorter invoking interval and therefore faster ingestion speed.
Shortest interval for batch ingestion is 60 seconds and longest could be e.g. once a day.
Defining the interval based on the tenant service level is essential for the platform,
as executing the tenant pipeline uses platform's recources, which causes hosting costs.
The ingestion interval is stored as seconds.

Another critical service level constaint is the maximum storage of the platform's staging input directory.
The service uses platform's memory and the max storage amount is due correlated to the service agreement level.
Lowest service level has 1 GB of memory and highest could be e.g. 10 TB, depending on the final hardware.
The staging input directory max storage is stores as megabytes (MB).

The service agreement schema would also contain other needed values, such as tenant identification,
the tenant's batch ingestion pipeline, which the platform executes and the corresponding staging input directory in the platform. In real production context the pipeline component would be stored in other form, e.g. some API.

Example service agreement schemas of two different tenants:

TODO: add example domains and why such agreement for each

Lower service level with one input data type support, max staging dir storage of 10GB, maximum of 20 files and
ingestion interval of 1 hour.
```json
{
    "id": "tenant123",
    "pipeline": "123_pipeline.py",
    "fileStorageLocation": "/tenantChicagoTaxi",
    "fileTypesSupported": {
        "csv": true,
        "json": false,
        "txt": false,
        "xml": false
    },
    "maxStorage": 10000.0,
    "maxNumFiles": 20,
    "interval": 3600
}
```

A bit higher service level with three input data type support, max staging dir storage of 1TB, maximum of 50 files and
ingestion interval of 1 minute.

```json
{
    "id": "tenantAbc",
    "pipeline": "abc_pipeline.py",
    "fileStorageLocation": "/tenantAviationCompany",
    "fileTypesSupported": {
        "csv": true,
        "json": true,
        "txt": true,
        "xml": false
    },
    "maxStorage": 1000000.0,
    "maxNumFiles": 50,
    "interval": 60
}
```

### 1.2 Batch Ingest Pipeline

To perform batch ingestion with this platform, each tenant will put their source data files into a staging file directory hosted by this platform.
The technology for staging file directory is Hadoop Distributed File System (HDFS). Due to the distribution,
HDFS is fault-tolerant and it provides high throughput data access to application data.

Each tenant has created their own batch ingestion pipeline. The pipeline will take the tenant's source data
from the corresponding HDFS location, process the data and insert the processed data into the storage component
mysimbdp-coredms, which is provided by the platform.

Example implementation of batch ingestion pipeline can be found on *code/tenant/*. The main technology
is Apache Spark, which is an engine for executing data engineering and processing large amounts of data. Spark contains ready made drivers for ingesting data from HDFS and inserting it to Apache Cassandra, which is the
storage technology of this platform. The platform's batchingestmanager calls the tenant pipeline component,
with the file to ingest being an input parameter. In the pipeline, fist a spark session is created, with connection
to the tenant's corresponding Cassandra keyspace and table. Then the component reads the source data file and
processes it by:

*  removing unneeded values: Pickup Census Tract, Dropoff Census Tract, Pickup Centroid Location, Dropoff Centroid  Location
*  transforming column keys from format "Trip ID" to format "trip_id"
*  tranforming some column data types such as: trip_start_timestamp/end_timestamp from text to timestamp
*  filling null values in pickup_community_area to -1.0

After the batch data processing the processed data is inserted into corresponding Cassandra table.

### 1.3 Batch Ingestion Manager

The platform contains a component for managing different tenant batch ingestion pipelines, *batchingestmanager*.
The manager is responsible for invoking the tenants pipelines to start the ingestion. The pipelines are a
black box for the manager, meaning that the manager is only responsible for starting the pipelines and does not
need any other information about the pipelines.

The manager retrieves the service level agreement information about each tenant in JSON form. The agreement
contains information about each tenant's ingestion interval in seconds. The manager is running continuosly, and
scheduling the pipelines. When the tenant's ingestion interval has ran, the manager checks the tenant's staging input directory location in the platforms HDFS from the service agreement. Then the manager checks for new files in the location. The manager uses the service level agreement to make sure that the tenant is acting according to the agreement. If the tenant has inserted source data into the staging directory with data types not being accepted in the service agreement, the manager will remove them. Also the manager checks that the maximum storage amount in the staging directory is not exceeded. If the limit is exceeded, the manager will remove files until the storage amount is under the limit. The manager also checks that the number of input files is not exceeding the limit, and removes files until amount is under the limit if needed. In real production platform, the insertion of new files exceeding the limits would be prevented.

Finally the manager will call the tenant's ingestion pipeline with each accepted file. After all the files are ingested, the manager waits for the interval time amout until checking for the tenant's source data files again.

### 1.4 Design for multi-tenancy

TODO: explain multi-tenancy model

In a real production platform, each tenant would have their own coredms instance running in the platform.
With new platform users the platform would increase infrastructure and when tenants are removed,
their instances would be deleted.

We tested the platform against two different tenants with different level service agreements. Both tenants used the same pipeline to emphasize the effect of the service agreement details. Both tenants also used the same input data.

In the first test, we measure the ingestion speed of the two different tenants, when they have different level service agreements. Both tenant 1 and tenant 2 have 10 files of 5 000 rows inserted into their corresponding staging file directories provided by the platform. One file has size of approximately 2 MB. Tenants have the same amount of input data, to point out the effect of maximum ingestion interval total data size defined in the unique service level agreement to ingestion speed.

Tenant 1 has higher level service agreement, and in this test case the tenant's maximum ingestion interval size limit is not reached. Tenant 2 has lower level agreement with stricter maximum intercal size, meaning after ingestion of 3 files, the manager will halt the tenant's pipeline execution for 60 seconds before continuing the ingestion of the input files. In this case the maximum storage and number of files service agreement details are not exceeded.

Tenant 1 service agreement:

```json
{
    "id": "tenant_chicago",
    "pipeline": "batch_ingest_pipeline.py",
    "fileStorageLocation": "/tenantChicagoTaxi",
    "coredmsKeyspace": "taxiservices",
    "coredmsTable": "trips",
    "fileTypesSupported": {
        "csv": true,
        "json": false,
        "txt": false,
        "xml": false
    },
    "maxStorage": 50.0,
    "maxIngestionIntervalSize": 25.0,
    "maxNumFiles": 20,
    "interval": 10
}
```

Tenant 2 service agreement:

```json
{
    "id": "example_tenant_123",
    "pipeline": "batch_ingest_pipeline.py",
    "fileStorageLocation": "/exampleTenant",
    "coredmsKeyspace": "exampletenant",
    "coredmsTable": "trips",
    "fileTypesSupported": {
        "csv": true,
        "json": false,
        "txt": false,
        "xml": false
    },
    "maxStorage": 50.0,
    "maxIngestionIntervalSize": 5.0,
    "maxNumFiles": 20,
    "interval": 60
}
```

Results:

| Tenant          | Total time (s)  | Ingestion speed (MB/s) |
|-----------------|-----------------|------------------------|
| 1               |  384            | 0.05                   |
| 2               |  571            | 0.04                   |

We can see that for the tenant 1 with higher level service agreement, ingestion speed was 33 % faster than the lower level agreement having tenant 2. As the input data contained 50 000 rows total, the corresponding ingestion speeds measured by rows are 130 rows/s and 88 rows/s. This test environment is rather minimal, but it indicates the impact of the service level agreement to the ingestion speed difference.

The full log files are *tenant_chicago_1741351815_ingestion.log* and *example_tenant_123_1741351815_ingestion.log*.

In the second test, we will violate the service level agreement constraints. During ingestion, tenant 2 will exceed the maximum input storage of the staging input directory defined to the tenant. From the log *tenant_chicago_1741351815_ingestion.log* we can see the error message, which tells that the tenant is breaking the service agreement of maximum staging input directory storage and the ingestion is not started.

```log
2025-03-07 15:11:59,930 - INFO - Statistics:
  *  Amount of files in tenant's staging input directory: 10
  *  Combined Storage Used: 113.63 /50.0 MB
2025-03-07 15:11:59,931 - WARNING - Tenant Service Agreement limit reached: storage limit exceeded
2025-03-07 15:11:59,931 - WARNING - Storage used: 113.625373 MB
2025-03-07 15:11:59,931 - WARNING - Storage limit: 50.0 MB
2025-03-07 15:11:59,931 - WARNING - Remove exceeding files to start ingestion.
```

In the third test, tenant 2 will break the service agreement of input data types supported. Tenant 2 agreement supports only csv-format, but tenant will insert txt-format file. The error message can be seen on log *example_tenant_123_1741353469_ingestion.log*

```log
2025-03-07 15:17:49,422 - INFO - Starting tenant example_tenant_123 pipeline execution ...
2025-03-07 15:17:49,468 - INFO - Files:
2025-03-07 15:17:49,483 - INFO - * fail.txt - 0.00 MB
2025-03-07 15:17:49,483 - WARNING - Tenant Service Agreement failure: input file type 'txt' not supported
2025-03-07 15:17:49,483 - WARNING - Removing file fail.txt
```

In this final part, we measure the maximum amount of data per this platform can ingest by increasing the batch file sizes while running ingestion on the two tenants. We will keep testing with 2 tenants to make the scenario more realistic, even though we could get higher ingest speed with only 1 tenant running.

**2 files of 25000 rows each (10 MB/file):**

The runtime of both tenants stayd approximately the same for each test, so we show only one.

| Rows of data | Total time (s)  | Ingestion speed (MB/s) |
|--------------|-----------------|------------------------|
| 50000        |  106            | 0.20                   |

logs:

* example_tenant_123_1741354437_ingestion.log
* tenant_chicago_1741354437_ingestion.log

**2 files of 35 000 rows each (15 MB/file):**

| Rows of data | Total time (s)  | Ingestion speed (MB/s) |
|--------------|-----------------|------------------------|
| 70000        |  116            | 0.25                   |

logs:

* example_tenant_123_1741354734_ingestion.log
* tenant_chicago_1741354734_ingestion.log

**2 files 45 000 rows each (19 MB/file):**

| Rows of data | Total time (s)  | Ingestion speed (MB/s) |
|--------------|-----------------|------------------------|
| 90000        |  122            | 0.30                   |

logs:

* tenant_chicago_1741354983_ingestion.log
* example_tenant_123_1741354983_ingestion.log

The final test of 2 files of 50 000 rows (22 MB/file) resulted in the platform halting so we found the limit.
When running the platform locally with one machine (HDFS, Spark, Cassandra), and two parallel tenants,
the maximum amount of data per second we can ingest by batch ingestion is approximately
0.30 MB/s (738 rows/s) per tenant. We can assume that this would approximately double up when running with only one tenant. We can assume that this would also linearly decrease with the amount of tenants increasing.

### 1.5 Logging

Define metrics:
* why do we log these info, why are the data needed?
* how are the log data used to manage service quality (platform not too slow etc)
* are the logging data metrics defined in the service agreement (we promise that this is minimum ingest speed etc)

The platform logs multiple factors by the batch ingestion manager, which runs the tenants ingestion pipelines. When the manager invokes the tenant's pipeline execution, the corresponding log file is initialized with the tenant id. Then the manager checks for new files from the tenant's staging input directory. The manager logs the following general information aspects:

* info if no new input files are found
* names of files found and file sizes
* combined storage used of the staging input directory

The manager logs the following service agreement violation aspects:

* input file amount limit exceeded
* storage limit exceeded
* input file type not supported

The manager logs the following ingestion statistics aspects:

* time taken to ingest each file
* ingested file is removed from staging directory
* number of files ingested
* total ingestion time (s)
* total ingestion size (MB)
* ingestion speed (MB/s)

The manager stores the log file in the platform. In real production environment, the file would be stored to a e.g. a database.

The information logged could be used for various ways. For example, if the service level agreement would contain some level of promised ingestion speed (e.g. in MB/s), the data could be used to monitor that the platform is fulfilling the promised ingestion speed of tenant. The log information of service level agreement violations could be used to monitor tenant actions, and if the violations would continue the platform could perform some corresponding actions (e.g. suspend data ingestion, notify the tenant). The logs also contain possible platform errors, such as HDFS errors. These informations could be used to fix possible bugs and so on.

This is an example log file showing succesful data ingestion of a tenant:

```log
2025-03-07 14:50:15,257 - INFO - ######################################################
2025-03-07 14:50:15,257 - INFO - Starting tenant example_tenant_123 pipeline execution ...
2025-03-07 14:50:15,286 - INFO - Files:
2025-03-07 14:50:15,302 - INFO - * sample0.csv - 2.06 MB
2025-03-07 14:50:15,313 - INFO - * sample1.csv - 2.06 MB
2025-03-07 14:50:15,324 - INFO - * sample2.csv - 2.07 MB
2025-03-07 14:50:15,336 - INFO - * sample3.csv - 2.07 MB
2025-03-07 14:50:15,350 - INFO - * sample4.csv - 2.06 MB
2025-03-07 14:50:15,360 - INFO - * sample5.csv - 2.07 MB
2025-03-07 14:50:15,371 - INFO - * sample6.csv - 2.07 MB
2025-03-07 14:50:15,383 - INFO - * sample7.csv - 2.07 MB
2025-03-07 14:50:15,392 - INFO - * sample8.csv - 2.06 MB
2025-03-07 14:50:15,402 - INFO - * sample9.csv - 2.07 MB
2025-03-07 14:50:15,403 - INFO - Statistics:
  *  Amount of files in tenant's staging input directory: 10
  *  Combined Storage Used: 20.66 /50.0 MB
2025-03-07 14:50:15,403 - INFO - Ingesting sample0.csv
2025-03-07 14:50:55,489 - INFO - Ingested sample0.csv. Time taken: 40.06s. Deleted from staging dir.
2025-03-07 14:50:55,490 - INFO - Ingesting sample1.csv
2025-03-07 14:51:37,792 - INFO - Ingested sample1.csv. Time taken: 42.27s. Deleted from staging dir.
2025-03-07 14:51:37,795 - INFO - Ingesting sample2.csv
2025-03-07 14:52:19,463 - INFO - Ingested sample2.csv. Time taken: 41.64s. Deleted from staging dir.
2025-03-07 14:52:19,464 - INFO - Ingestion interval limit reached. Starting ingestion again after 60 seconds ...
2025-03-07 14:53:19,519 - INFO - Ingesting sample3.csv
2025-03-07 14:53:56,893 - INFO - Ingested sample3.csv. Time taken: 37.31s. Deleted from staging dir.
2025-03-07 14:53:56,894 - INFO - Ingesting sample4.csv
2025-03-07 14:54:39,813 - INFO - Ingested sample4.csv. Time taken: 42.88s. Deleted from staging dir.
2025-03-07 14:54:39,816 - INFO - Ingesting sample5.csv
2025-03-07 14:55:30,367 - INFO - Ingested sample5.csv. Time taken: 50.51s. Deleted from staging dir.
2025-03-07 14:55:30,370 - INFO - Ingestion interval limit reached. Starting ingestion again after 60 seconds ...
2025-03-07 14:56:30,431 - INFO - Ingesting sample6.csv
2025-03-07 14:57:12,387 - INFO - Ingested sample6.csv. Time taken: 41.92s. Deleted from staging dir.
2025-03-07 14:57:12,388 - INFO - Ingesting sample7.csv
2025-03-07 14:57:44,161 - INFO - Ingested sample7.csv. Time taken: 31.72s. Deleted from staging dir.
2025-03-07 14:57:44,162 - INFO - Ingesting sample8.csv
2025-03-07 14:58:15,480 - INFO - Ingested sample8.csv. Time taken: 31.29s. Deleted from staging dir.
2025-03-07 14:58:15,480 - INFO - Ingestion interval limit reached. Starting ingestion again after 60 seconds ...
2025-03-07 14:59:15,534 - INFO - Ingesting sample9.csv
2025-03-07 14:59:46,517 - INFO - Ingested sample9.csv. Time taken: 30.92s. Deleted from staging dir.
2025-03-07 14:59:46,517 - INFO - Ingestion ended, amount of files ingested: 10
2025-03-07 14:59:46,518 - INFO - --------------------------------
2025-03-07 14:59:46,518 - INFO - Total ingestion time: 571.11 s
2025-03-07 14:59:46,518 - INFO - Total ingestion size: 20.66 MB
2025-03-07 14:59:46,518 - INFO - Ingestion speed: 0.04 MB/s
```

The log file contains information about the input data and ingestion statistics.

This is an example log file of an ingestion canceled because service level agreement violation of tenant:

```log
2025-03-07 15:11:59,729 - INFO - ######################################################
2025-03-07 15:11:59,730 - INFO - Starting tenant example_tenant_123 pipeline execution ...
2025-03-07 15:11:59,800 - INFO - Files:
2025-03-07 15:11:59,817 - INFO - * sample0.csv - 20.66 MB
2025-03-07 15:11:59,830 - INFO - * sample1.csv - 20.66 MB
2025-03-07 15:11:59,846 - INFO - * sample2.csv - 20.66 MB
2025-03-07 15:11:59,861 - INFO - * sample3.csv - 20.65 MB
2025-03-07 15:11:59,874 - INFO - * sample4.csv - 20.66 MB
2025-03-07 15:11:59,887 - INFO - * sample5.csv - 2.07 MB
2025-03-07 15:11:59,898 - INFO - * sample6.csv - 2.07 MB
2025-03-07 15:11:59,908 - INFO - * sample7.csv - 2.07 MB
2025-03-07 15:11:59,919 - INFO - * sample8.csv - 2.06 MB
2025-03-07 15:11:59,930 - INFO - * sample9.csv - 2.07 MB
2025-03-07 15:11:59,930 - INFO - Statistics:
  *  Amount of files in tenant's staging input directory: 10
  *  Combined Storage Used: 113.63 /50.0 MB
2025-03-07 15:11:59,931 - WARNING - Tenant Service Agreement limit reached: storage limit exceeded
2025-03-07 15:11:59,931 - WARNING - Storage used: 113.625373 MB
2025-03-07 15:11:59,931 - WARNING - Storage limit: 50.0 MB
2025-03-07 15:11:59,931 - WARNING - Remove exceeding files to start ingestion.
2025-03-07 15:11:59,931 - INFO - --------------------------------
2025-03-07 15:11:59,931 - INFO - Total ingestion time: 0.00 s
2025-03-07 15:11:59,931 - INFO - Total ingestion size: 0.00 MB
2025-03-07 15:11:59,931 - INFO - Checking for new files after 60 seconds ...
2025-03-07 15:12:59,981 - INFO - ######################################################
2025-03-07 15:12:59,981 - INFO - Starting tenant example_tenant_123 pipeline execution ...
2025-03-07 15:13:00,041 - INFO - Files:
2025-03-07 15:13:00,057 - INFO - * sample0.csv - 20.66 MB
2025-03-07 15:13:00,057 - INFO - Statistics:
  *  Amount of files in tenant's staging input directory: 1
  *  Combined Storage Used: 20.66 /50.0 MB
2025-03-07 15:13:00,057 - INFO - Ingesting sample0.csv
2025-03-07 15:13:58,377 - INFO - Ingested sample0.csv. Time taken: 58.29s. Deleted from staging dir.
2025-03-07 15:13:58,378 - INFO - Ingestion ended, amount of files ingested: 1
2025-03-07 15:13:58,378 - INFO - --------------------------------
2025-03-07 15:13:58,378 - INFO - Total ingestion time: 58.32 s
2025-03-07 15:13:58,378 - INFO - Total ingestion size: 20.66 MB
2025-03-07 15:13:58,379 - INFO - Ingestion speed: 0.35 MB/s
2025-03-07 15:13:58,379 - INFO - Ingestion ended, amount of files ingested: 1
```

This log shows the agreement violation of staging input directory maximum storage exceeded. The tenant has then removed files, and when the manager has invoked the ingestion again, the limit is not exceeded anymore and the ingestion is started.

## Part 2 - Near real-time data ingestion and transformation
