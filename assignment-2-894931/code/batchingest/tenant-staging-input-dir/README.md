# Staging Input Directory

Technology is HDFS. After installing it, set the following configurations:

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
