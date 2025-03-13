# Batch ingestion

This part contains the batch ingestion components of the platform.

* service agreements: example tenants service agreements of the platform
* tenant-staging-input-dir: contains information about how to set up HDFS
* batch_ingest_manager: python script for batch ingest manager which invokes pipeline
* batch_ingest_pipeline: python script for batch ingest pipeline which consumes the data from HDFS, transforms it and inserts into coredms
