# Silver and gold data storage

Technology is HDFS. After installing it, set the following configurations:

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

``$ mkdir datanode``
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

Create silver and gold data storage locations:

``$bin/hdfs dfs -mkdir chicagoTenant/silverData``
``$bin/hdfs dfs -mkdir chicagoTenant/goldData``

Usual Linux syntax can be used to manipulate the HDFS (rm, cp, mkdir, ...)
