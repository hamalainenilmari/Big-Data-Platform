Before running the docker compose file, run:
$docker run -it  bitnami/kafka:latest kafka-storage.sh random-uuid
and add the output into KAFKA_KRAFT_CLUSTER_ID