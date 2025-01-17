import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types


def main():
    # 1) Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 2) Sample data: list of dictionaries
    sensor_readings = [
        {"sensor_id": "sensor1", "reading": 32.1, "timestamp": "2020-01-01T01:01:01Z"},
        {"sensor_id": "sensor2", "reading": 28.5, "timestamp": "2020-01-01T01:02:01Z"},
        {"sensor_id": "sensor3", "reading": 31.7, "timestamp": "2020-01-01T01:03:01Z"},
    ]

    # 3) Create a DataStream from the list
    data_stream = env.from_collection(
        collection=sensor_readings,
        type_info=Types.MAP(Types.STRING(), Types.STRING())
    )

    # 4) Convert each dictionary to a JSON string
    json_stream = data_stream.map(
        lambda d: json.dumps(d),
        output_type=Types.STRING()
    )

    # 5) Build a KafkaSink
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("sensor-data")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        # Choose your delivery guarantee
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # 6) Add the sink to the stream
    json_stream.sink_to(kafka_sink)

    # 7) Execute the Flink job
    env.execute("Kafka Producer Job")


if __name__ == "__main__":
    main()
