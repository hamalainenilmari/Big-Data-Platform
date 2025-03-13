# Tenant simulating instructions

This component is the example tenants using the platform to ingest data into the coredms.

chicago/kafka_producer.py and ny/kafka_producer.py contains kafka producers which sends data to the platforms kafka server running on the dataingest component.

To simulate producing data into the platform first run:

``$ pip install -r requirements.txt``

If you have problems with installing Pandas-package from requirements.txt, try to install Numpy first (pip install "numpy>=1.21.0")

If you have not created the sample data sets, go to folder *data* and create sample data set according to the instructions.

Then using the shell script you can start the producers.

``$ ./start_producing.sh 5 localhost:9092``

The first argument is the number of concurrent producers (use max as many as you have sample data sets created in **data** folder).
The second argument is the Kafka broker address.

The producers run until they have sent their sample files to the kafka server.
