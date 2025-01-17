from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, DeliveryGuarantee
from pyflink.common.watermark_strategy import WatermarkStrategy


def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()    
    # Add required JAR files for Kafka connector
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-1.17.1.jar",
                 "file:///opt/flink/lib/kafka-clients-3.2.3.jar")

    # Create Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("input-topic") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create Kafka sink
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
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
