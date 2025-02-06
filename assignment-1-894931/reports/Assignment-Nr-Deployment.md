# This is a deployment/installation guide

It is a free form. you can use it to explain how to deploy/install and run  your code. Note that this deployment/installation guide ONLY helps to run your assignment. **It is not where you answer your solution for the assignment questions**


The platform can be run locally by the following steps:

**coredms**:

go to code/mysimbdp-coredms and run docker compose up. This deploys the cassandra cluster with 4 nodes in containers locally.
The folder contains instructions how to create the keyspace and table.

**dataingest**:

Start the Kafka server by going to code/mysimbp/kafka_server. Create the .env file and add the variables to it according to the instructions in the folder.
Run docker compose up. Create Kafka topic using the instructions.

After the Cassandra and Kafka containers are up and running, you can start simulating using the platform.

Go to code/tenant. Create .env file and add values from .env_example. 