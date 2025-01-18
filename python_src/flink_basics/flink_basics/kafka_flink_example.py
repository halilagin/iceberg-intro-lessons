from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, DeliveryGuarantee
from pyflink.common.watermark_strategy import WatermarkStrategy

jar_dir = "/usr/local/workdir/python_src/flink_basics/flink_basics/jars"

jar_files = [
    "flink-connector-kafka-1.17.1.jar",
    "kafka-clients-3.2.3.jar",
    "flink-streaming-java-1.17.1.jar",
    "flink-shaded-guava-30.1.1-jre-15.0.jar",
    # "flink-python-1.20.0.jar",
]
def get_jar_file_paths():
    return [jar_dir + "/" + jar_file for jar_file in jar_files]


def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()    
    # Add required JAR files for Kafka connector


    jars = get_jar_file_paths()
    files_jars = ["file://"+j for j in jars]
    print(files_jars)
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print ("env.init")
    env.set_parallelism(1)
    print ("env.parallelism set")
    # Add required JAR files for Kafka connector
    env.add_jars(*files_jars)
    print ("env.jars:added") 


    # Create Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka1:9092") \
        .set_topics("sensor-data") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create Kafka sink
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka1:9092") \
        .set_record_serializer(SimpleStringSchema()) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_topic("output-topic") \
        .build()

    # Define the streaming pipeline
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    # Simple transformation: convert to uppercase
    result = ds.map(lambda x: x.upper())

    # Write to sink
    result.sink_to(sink)

    # Execute the job
    env.execute("Kafka Flink Example")


if __name__ == "__main__":
    main()
