# Stream analytics component

Tenantstreamapp.py is the implementation of the stream analytics component. The application consumes messages from Kafka topic, aggregates number of taxi trips per geographical area over tumbling window and stores produced silver data into HDFS silver data storage. The application also sends the silver data and data quality alerts to another Kafka topic, to which the tenant listens to. The application also generates data processing metrics and stores them into log files in local logs folder.

The data quality limit for alert is hardcoded in the code, and can be changed by changing the value

```python
quality_alert_metrics = summed_metrics.filter(lambda x: (1 - x[2]/x[1]) < 0.99) # Change this if you want different limit
```
