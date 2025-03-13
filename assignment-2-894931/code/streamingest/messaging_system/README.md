# Messaging System

This component is from assignment 1. This component contains kafka cluster.

Starting Kafka cluster:

First create .env file in the kafka_server folder from the example_env.

Before running the docker compose file, run:

``$ docker run -it  bitnami/kafka:latest kafka-storage.sh random-uuid``

and add the output into KAFKA_KRAFT_CLUSTER_ID in .env.

Add KAFKA_CFG_ADVERTISED_LISTENERS=localhost to .env if needed.

After these two variables are in the .env, you can run ``$ docker compose up`` to start the kafka server.
After the kafka brokers in the cluster are up and healthy, add a Kafka topic by running:

docker exec -it <container_name> kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic <your_topic_name>

The topics needed to run the platform are written in the deployment info.

After the topic is created it is time to start the kafka consumers. Go to folder consumer and install the requirements:

``$ pip install -r requirements.txt``

Then add .env file, which should be the same as the example env if you are running this platform according to the instructions.

Then you can start running the kafka consumer using the shell script (takes one argument: number of concurrent kafka consumers):

``$ ./start_consuming.sh 5``

This starts 5 kafka consumers.
