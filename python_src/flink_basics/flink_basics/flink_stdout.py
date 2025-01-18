from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
import config as local_config
import json


def process_sensor_data(data):
    """Process the user data and return analyzed results"""
    try:
        # Parse the string data as JSON
        user_data = json.loads(data)
        # Create a processed result with selected fields and analysis
        result = {
            'user_id': user_data['id'],
            'full_name': f"{user_data['name']} {user_data['surname']}",
            'salary_category': 'High' if user_data['salary'] > 80000 else 'Medium' if user_data['salary'] > 50000 else 'Low',
            'salary': user_data['salary'],
            'job': user_data['job'],
            'company': user_data['company']
        }
        return json.dumps(result)
    except Exception as e:
        return json.dumps({
            'error': str(e),
            'raw_data': data
        })





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
    env.add_jars(*files_jars)

    # Create Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka1:9092,kafka2:9093,kafka3:9094") \
        .set_topics("std-out") \
        .set_group_id("flink-consumer-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create the consumer pipeline
    ds = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    # Process the incoming data
    processed_stream = ds.map(process_sensor_data)

    # Print the results (in production, you might want to send this to another sink)
    processed_stream.print()

    # Execute the job
    env.execute("Flink Data Consumer")


if __name__ == "__main__":
    main()
