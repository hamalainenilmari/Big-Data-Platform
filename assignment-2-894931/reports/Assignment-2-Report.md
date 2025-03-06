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

### 1.5 Logging

Define metrics:
* why do we log these info, why are the data needed?
* how are the log data used to manage service quality (platform not too slow etc)
* are the logging data metrics defined in the service agreement (we promise that this is minimum ingest speed etc)
