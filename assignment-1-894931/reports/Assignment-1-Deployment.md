# This is a deployment/installation guide for running this platform locally

To run the platform, Docker and Docker compose must be installed. Python and pip must also be installed.
The platform can be run locally by the following steps (use separate terminal for each):

##### Note that if the component configurations are too much for your device, you can reduce the number of cassandra nodes and kafka brokers in the docker-compose files. 3 nodes and 3 brokers works just fine also.

**Coredms**:

Go to *code/mysimbdp-coredms* and run docker compose up. This deploys the cassandra cluster with 4 nodes in containers locally.
The folder contains instructions on how to create the keyspace and table.

**Dataingest**:

Start the Kafka server by going to *code/mysimbp-dataingest/kafka_server*. Create the .env file and add the variables to it according to the instructions in the folder.
Run docker compose up. Create Kafka topic using the instructions. Then start the consumer by going to *mysimbdp-dataingest/consumer*, and running the python application
kafka_consumer.py according to the instructions there.

After the Cassandra and Kafka containers and the kafka consumer are up and running, you can start simulating using the platform.

**Tenant**:

This external part is an example user of the platform.
Download the source data set according to instructions in folder *data*. Create the sample data sets from the source data following the instructions.

Go to *code/tenant*. Then run the data generators according to the instructions on the folder.

After completing these steps succesfully, you should see:

* producer(s) generating the data in the console where you ran ./start_producing.sh
* consumer(s) reading the data in the console where you ran ./star_consuming.sh

The producers will send data to the kafka server until they have reached the end of the sample data sets.
The consumers will keep on listening to data until the shell script is stopped by control+C.
You can check that the data is succesfully inserted by querying for it in one of the Cassandra nodes according to the instructions in *mysimbdp-coredms*.
