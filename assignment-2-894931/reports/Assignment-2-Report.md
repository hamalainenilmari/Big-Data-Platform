# Assignment 2 report - Working on Data Ingestion and Transformation

## Part 1 - Batch data ingestion and transformation

### 1.1 Constraints of data files supported

This platform has different constraints for source files defined by each tenant service agreement.
The number of different input file types supported depends on the service agreement.
Minimum service supports only 1 file-format, such as CSV. With higher level service, the platform will support
multiple source file formats, such as CSV, JSON, XML. The number of different source file types supported means more technical implementation
in the platform and due is dependent on the service agreement.

Another service level quality constraint is ingest speed. Higher service level means faster ingest speed.
This means how often we check for new files.

Lowest level will ingest 100 rows in a second, and highest 10000 rows in a second???

You must describe constraints on the files that can be ingested into your platform, and constraints on the quality of service you offer to each tenant, such as number of files, storage limits, ingest rate/speed, etc.

Some YAML/JSON format constraints
* how big file max?
* how many files max?
* how many different file format? only 1 supported?

Example JSON:

```json
{
    "tenantId": "tenant123",
    "inputFileTypes: ["csv", "json"],

}
```

### 1.2 Batch Ingest Pipeline

To perform batch ingestion with this platform, each tenant will put their source data files into a staging file directory hosted by this platform.
The technology for staging file directory is Hadoop Distributed File System (HDFS).