# Assignment 1 894931

> Guide line: This is the file where you will explain the structure of your assignment delivery. Remember to replace **Assignment_NR** with the number of the assignment (e.g., 1, 2, 3, or 4) and **Your_STUDENTID** with your student number. Remove all guidelines from the template.

This repository contains the implementation and report of Big Data Platforms assignment 1.

From *reports/* you can find:

* Assignment-1-Report: the answers to the assignment questions
* Assignment-1-Deployment: instructions for running the platform locally.

From *logs/* you can find the log files of the performance testing of the platform.
Each log file is explained more in the assignment report.

from *data/* you can find sample of the Chicago taxi trip data set and a python script for
generating multiple samples from the original set.

From *code/* you can find all the implementations of the platform:

* *mysimbdp-dataingest*: contains kafka server set up and consumer python application
* *mysimbdp-coredms*: contains cassandra cluster set up
* *tenant*: contains example tenant implementation, with kafka producer

Each folder contains more information about itself.
