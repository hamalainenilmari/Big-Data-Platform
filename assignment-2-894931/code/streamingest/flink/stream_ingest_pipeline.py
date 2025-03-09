from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.common import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # JARs of kafka (source) and Cassandra (sink) connectors
    env.add_jars("file:///home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-sql-connector-kafka-3.4.0-1.20.jar")
    env.add_jars("file:////home/ilmarih/bdp_25_tech/flink-1.20.1/lib/flink-connector-cassandra_2.12-3.2.0-1.19.jar")
    
    # Schema of the Kafka JSON input
    json_schema = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW_NAMED(
            ["id", "message"],  # Field names
            [Types.STRING(), Types.STRING()]  # Field types
        )) \
        .build()

    # Kafka Source setup
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("testTopic") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_schema) \
        .set_properties({
         'fetch.max.wait.ms': '10000',  # Adjust this for longer timeout
        }) \
        .build()
    
    # Input data stream
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    
    # Print the stream to the console
    stream.print()

    # Cassandra Sink
    CassandraSink.add_sink(stream) \
        .set_host("localhost") \
        .set_query("INSERT INTO flink.test (id, message) VALUES (?, ?)") \
        .build()

    
    try:
        env.execute("Kafka Flink Cassandra pipeline")
    except Exception as e:
        print(f"Error while executing the Flink job: {e}")

if __name__ == "__main__":
    main()