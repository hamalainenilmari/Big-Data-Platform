# This is a deployment/installation guide

It is a free form. you can use it to explain how to deploy/install and run  your code. Note that this deployment/installation guide ONLY helps to run your assignment. **It is not where you answer your solution for the assignment questions**


## Hadoop

Hadoop (version 3.4.1):
download from https://www.apache.org/dyn/closer.cgi/hadoop/common/


Use the following:

etc/hadoop/core-site.xml:

<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>

create folders for namenode and datanode
mkdir -p hdfs/datanode
mkdir -p hdfs/namenode

add the paths to the hdfs-site.xml as following

etc/hadoop/hdfs-site.xml
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

Now check that you can ssh to the localhost without a passphrase:

  $ ssh localhost
If you cannot ssh to localhost without a passphrase, execute the following commands:

  $ ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  $ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  $ chmod 0600 ~/.ssh/authorized_keys

  

Format the HDFS filesystem by running:

``hdfs namenode -format``


Start NameNode daemon and DataNode daemon:

  $ sbin/start-dfs.sh


to insert data to hdfs tenant:
bin/hdfs dfs -put -f ../../bdp_25/assignment-2-894931/data/*.csv /tenantChicagoTaxi/

## Spark
version 3.5.5
download from https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz


