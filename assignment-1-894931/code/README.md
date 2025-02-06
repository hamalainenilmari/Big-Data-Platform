# This directory is about the code.
>Note: we must be able to compile and/or run the code. No BINARY files are within the code. External libraries should be automatically downloaded (e.g., via Maven, npm, pip, docker pull)

# This directory contains the components of this data platform.

Mysimbdp-coredms: docker compose of Cassandra cluster
Mysimbdp-dataingest: docker compose of Kafka server and consumer application
Tenant: example tenant kafka producer to simulate data generation

Each of these components and tenant contain instructions on how to run.
