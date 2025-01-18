from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.serialization import SerializationSchema
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee, KafkaRecordSerializationSchema
from pyflink.common.typeinfo import Types
import random
from faker import Faker
import json
from typing import List, Dict, Any


def generate_data():
    """Generate sample sensor data"""
    sensor_id = random.randint(1, 5)
    temperature = round(random.uniform(20.0, 35.0), 2)
    return f"sensor_{sensor_id}:{temperature}"


class JsonSerializationSchema(SerializationSchema):
    def serialize(self, value: dict) -> bytes:
        return json.dumps(value).encode("utf-8")


def generate_fake_persons(num_records=10):
    """Generate a list of fake person data"""
    fake = Faker()
    persons = [
        {
            "id": fake.unique.random_number(digits=5),
            "name": fake.first_name(),
            "surname": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address().replace('\n', ', '),
            "birthdate": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
            "job": fake.job(),
            "company": fake.company(),
            "credit_card": fake.credit_card_number(card_type=None),
            "salary": round(random.uniform(30000, 120000), 2)
        } for i in range(num_records)
    ]
    return persons


stream_input = generate_fake_persons(20)

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
    print(stream_input)
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

    # Create a DataStream using a custom source function
    ds = env.from_collection(
        collection=stream_input,
        # type_info=List[Dict[str, Any]]
        type_info=Types.MAP(Types.STRING(), Types.STRING())

    ).map(lambda x: json.dumps(x), output_type=Types.STRING())  # Convert dict to JSON string
    print ("ds created")

    kafka_record_serializer = KafkaRecordSerializationSchema \
        .builder() \
        .set_topic("sensor-data")\
        .set_value_serialization_schema(SimpleStringSchema())\
        .build()
    print ("kafka_record_serializer created")
    # Create Kafka sink
    #.set_bootstrap_servers("kafka1:9092,kafka2:9093,kafka3:9094") \
    #.set_bootstrap_servers("kafka1:29092,kafka2:29093,kafka3:29094") \
    #.set_bootstrap_servers("172.22.0.7:9092,kafka2:9093,kafka3:9094") \
    #.set_bootstrap_servers("host.docker.internal:29092,host.docker.internal:29093,host.docker.internal:29094") \
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka1:9092") \
        .set_record_serializer(kafka_record_serializer)\
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    print ("sink created")

    # Write to Kafka
    ds.sink_to(sink)
    print ("ds sinked to sink")

    # Execute the job
    env.execute("Flink Data Producer")


if __name__ == "__main__":
    main()
