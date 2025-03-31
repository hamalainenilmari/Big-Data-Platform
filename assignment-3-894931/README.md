# Assignment 3 894931

This repository contains the implementations and reports of Big Data Platforms assignment 3 -  Stream and Batch Analytics.

From *reports/* you can find:

* Assignment-3-Report: the answers to the assignment questions
* Assignment-3-Deployment: instructions for running the platform locally

From *logs/* you can find the log files of the performance testing of the platform. Each log file is explained more in the assignment report.

From *data/* you can find sample of the Chicago taxi trip data set.

From *code/* you can find all the implementations of the platform:

* *cassandra*: contains cassandra cluster set up (from assignment 1,2, is not necessart for streaming analytics)
* *hdfs*: instructions for configurating HDFS silver and gold data storage
* *messaging_system*: contains messaging system (kafka cluster set up)
* *orchestrator*: contains Apache Airflow batch analytics workflow orchestrator
* *tenant*: contains example tenant data producer, and data consumer (for consuming silver data)
* *tenantbatchapp*: contains batch analytics processing component
* *tenantstreamapp*: contains stream analytics processing component

Each folder contains more information about itself.
