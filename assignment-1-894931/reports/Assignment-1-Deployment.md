# This is a deployment/installation guide

It is a free form. you can use it to explain how to deploy/install and run  your code. Note that this deployment/installation guide ONLY helps to run your assignment. **It is not where you answer your solution for the assignment questions**

To run the platform, Docker and Docker compose must be installed. Python and pip must also be installed.
The platform can be run locally by the following steps (use separate terminal for each):

**coredms**:

Go to *code/mysimbdp-coredms* and run docker compose up. This deploys the cassandra cluster with 4 nodes in containers locally.
The folder contains instructions on how to create the keyspace and table.

**dataingest**:

Start the Kafka server by going to *code/mysimbp-dataingest/kafka_server*. Create the .env file and add the variables to it according to the instructions in the folder.
Run docker compose up. Create Kafka topic using the instructions. Then start the consumer by going to *mysimbdp-dataingest/consumer*, and running the python application
kafka_consumer according to the instructions there.

After the Cassandra and Kafka containers and the kafka consumer are up and running, you can start simulating using the platform.

**tenant**:

Download the source data set according to instructions in folder *data*. Create the sample data sets from the source data by the instructions.

Go to *code/tenant*. Create .env file and add values from .env_example. Then run the data generators according to the instructions on the folder.

After completing these steps, you succesfully see:

* producers generating the data in the console where you ran ./start_producing.sh
* consumers reading the data in the console where you ran ./star_consuming.sh

The consumers will keep on listening to data until the shell script is stopped by control+C.
You can check that the data is succesfully inserted by querying for it in one of the Cassandra nodes according to the instructions in *mysimbdp-coredms*.

