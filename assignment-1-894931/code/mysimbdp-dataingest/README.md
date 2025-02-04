* This component contains kafka server and consumer

Before running the docker compose file, run:
$docker run -it  bitnami/kafka:latest kafka-storage.sh random-uuid
and add the output into KAFKA_KRAFT_CLUSTER_ID

to run kafka server, first add the following to the .env file:
* KAFKA_KRAFT_CLUSTER_ID (run: $docker run -it  bitnami/kafka:latest kafka-storage.sh random-uuid)
* KAFKA_CFG_ADVERTISED_LISTENERS (use host.docker.internal when running locally)

To add a Kafka topic, run:

docker exec -it <container_name> kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic <your_topic_name>