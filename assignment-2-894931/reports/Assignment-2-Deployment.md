# This is a deployment guide for running this platform locally

**This assignment contains two separate parts, one for batch ingestion and another for stream ingestion.**

To run the platform, the following technologies must be installed:

* Docker and Docker compose
* Python and pip
* HDFS - Hadoop (version 3.4.1), download from https://www.apache.org/dyn/closer.cgi/hadoop/common/
* Apache Spark (version 3.5.5), download from https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
* Apache Flink (version ), download from https://dlcdn.apache.org/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz
* Flink Kafka jar file: https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka/3.4.0-1.20
* Flink Cassandra jar file: https://mvnrepository.com/artifact/org.apache.flink/flink-connector-cassandra_2.12/3.2.0-1.19

**Coredms**:

The data storage of both ingestion mechanisms is the same coredms. Go to *code/coredms* and run docker compose up. This deploys the cassandra cluster with 3 nodes in containers locally. The folder contains instructions on how to create a keyspace and a table.

**Batch ingestion**:

HDFS is the staging input directory technology. After installing it, set the following configurations:

1. Modify *etc/hadoop/core-site.xml* to match:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

2. Create folders for namenode and datanode in the hadoop-3.4.1 root folder:

``$ mkdir datanode``
``$ mkdir namenode``

3. Modify *etc/hadoop/hdfs-site.xml* to match (make sure the paths to previously created folders are correct):

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

4. Format the HDFS filesystem by running:

``$ bin/hdfs namenode -format``

5. Start NameNode daemon and DataNode daemon:
``$ sbin/start-dfs.sh``

To create sample data, use the instruction on *data/* folder.

To insert data to hdfs tenant, run:
``$bin/hdfs dfs -put -f ../../bdp_25/assignment-2-894931/data/*.csv /tenantChicagoTaxi/``

Usual Linux syntax can be used to manipulate the HDFS (rm, cp, mkdir, ...)

After you have created a folder to the local HDFS and inserted source data there, you can simulate running the platform. To start the ingestion, run:

``$ python3 code/batchingest/batch_ingest_manager.py``

This will start the manager, which invokes the batch_ingest_pipeline and ingest the processed data into the Cassandra table.

**Stream ingestion**: