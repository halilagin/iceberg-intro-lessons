from kafka import KafkaConsumer
import json


def json_deserializer(message):
    """Deserialize JSON message"""
    return json.loads(message.decode('utf-8'))


def create_kafka_consumer(topic_name):
    """Create and return a Kafka consumer instance"""
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=json_deserializer,
        consumer_timeout_ms=60000  # Wait up to 60 seconds for message
    )


def main():
    topic_name = "test_topic"
    consumer = create_kafka_consumer(topic_name)
    print(f"Started consuming messages from topic: {topic_name}")
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
            print(f"Partition: {message.partition}, Offset: {message.offset}")
            print("---")
    except KeyboardInterrupt:
        print("Stopping the consumer...")
        consumer.close()


if __name__ == "__main__":
    main()
