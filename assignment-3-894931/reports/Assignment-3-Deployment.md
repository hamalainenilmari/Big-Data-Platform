# This is a deployment guide for running this platform locally

To run the platform, the following technologies must be installed:

* Docker and Docker compose
* Python and pip
* HDFS - Hadoop (version 3.4.1), download from https://www.apache.org/dyn/closer.cgi/hadoop/common/
* Apache Spark (version 3.5.5), download from https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
* Java (e.g. openjdk version 1.8.0_392)
* Flink Kafka jar file: https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka/3.4.0-1.20
* Flink Cassandra jar file: https://mvnrepository.com/artifact/org.apache.flink/flink-connector-cassandra_2.12/3.2.0-1.19
* Flink python jar: https://mvnrepository.com/artifact/org.apache.flink/flink-python/1.20.1
* Flink Hadoop jar: https://mvnrepository.com/artifact/org.apache.flink/flink-shaded-hadoop-2-uber/2.7.5-9.0

Install the dependencies by running:

``$ pip install -r requirements.txt``

Create .env based on the example env file. Generate kafka cluster ID from messaging_system instructions and add your JAR paths.

## Coredms

**Cassandra:**

*This component is not necessary for simulating the real time analytics.*

The operational data storage of real-time data ingestion is Cassandra. Go to *code/coredms* and run docker compose up. This deploys the cassandra cluster with 3 nodes in containers locally. The folder contains instructions on how to create a keyspace and a table.

**HDFS:**

Hadoop Distributed File System (HDFS) is analytical data storage of this platform. After installing it, set the following configurations:

Modify *etc/hadoop/core-site.xml* to match:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

Create folders for namenode and datanode in the hadoop-3.4.1 root folder:

``$ mkdir datanode namenode``
``$ mkdir namenode``

Modify *etc/hadoop/hdfs-site.xml* to match (make sure the paths to previously created folders are correct):

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
      <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
      <value>/your/path/to/namenode/</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
      <value>/your/path/to/datanode/</value>
  </property>
</configuration>
```

Check that you can ssh to the localhost without a passphrase:

``$ ssh localhost``

If you cannot ssh to localhost without a passphrase, execute the following commands:

``$ ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa``
``$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys``
``$ chmod 0600 ~/.ssh/authorized_keys``

Format the HDFS filesystem by running:

``$ bin/hdfs namenode -format``

Start NameNode daemon and DataNode daemon:
``$ sbin/start-dfs.sh``

Create silver and gold data locations:

``$bin/hdfs dfs -mkdir chicagoTenant/``
``$bin/hdfs dfs -mkdir chicagoTenant/silverData/``
``$bin/hdfs dfs -mkdir chicagoTenant/goldData/``

Usual Linux syntax can be used to manipulate the HDFS (rm, cp, mkdir, ...)

## Stream analytics

Start up the kafka broker cluster:

Go to code/streamingest/messaging_system/

Create .env file based on env example file. If running locally, leave KAFKA_CFG_ADVERTISED_LISTENERS=localhost.
Generate KAFKA_KRAFT_CLUSTER_ID by by running:

``$ docker run -it  bitnami/kafka:latest kafka-storage.sh random-uuid``

Add the output to env.

Then start the cluster by running:

``$ docker compose up -d``

This platform can be run with also 1 Kafka broker, if 3 is too much for your device. Create the topic replications according to number of brokers.

Then create the following Kafka topics:

``$ docker exec -it messaging_system-kafka0-1 kafka-topics.sh --create  --bootstrap-server localhost:9092  --replication-factor 3  --partitions 15  --topic chicagotenant_trips``

``$ docker exec -it messaging_system-kafka0-1 kafka-topics.sh --create  --bootstrap-server localhost:9092  --replication-factor 1  --partitions 1  --topic chicagotenant_analytics``

Then run the stream analytics component by code/tenantstreamapp

``$ python3 tenantstreamapp.py``

Make sure you have the correct dataset downloaded. Check more information from folder data/.

Start producing data for tenant by going to */tenant/chicago* and running:

``$ python3 chicago/kafka_produrer.py``

You can also run the tenant kafka consumer, which consumes the silver data and data quality alerts.

``$ python3 chicago/analytics_consumer.py``

The tenantstreamapp produces silver data the HDFS chicagoTenant/silverData storage, and stores processing metrics to local folder logs/.

## Batch analytics

**Airflow:**

Go to folder orchestrator and nstall Apache Airflow there:

``$ pip install "apache-airflow[celery]==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt"``

Check [here](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) for more information if you get install errors.

Create folder airflow in your root directory.

Indicate airflow home dir in the bdp orchestrator folder you are currently in.

Export the airflow home like:

``$ export AIRFLOW_HOME=/home/ilmarih/airflow``

Then initialize database:

``$ airflow db init``

Then start airflow webserver:

``$ airflow webserver -p 8080``

Create user by:

``$ airflow users create --username admin --firstname firstname --lastname lastname --role Admin --email admin@email.com``

Go to another terminal, again run

``$ export AIRFLOW_HOME=/home/ilmarih/airflow``

Then run:

``$ airflow scheduler``



start Spark cluster


