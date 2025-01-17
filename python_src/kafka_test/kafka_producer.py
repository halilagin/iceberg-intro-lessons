from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random


def create_sample_message():
    """Create a sample message with timestamp and random data"""
    return {
        "timestamp": datetime.now().isoformat(),
        "value": random.randint(1, 100),
        "message": f"Test message {random.randint(1, 1000)}"
    }


def json_serializer(data):
    """Serialize data to JSON format"""
    return json.dumps(data).encode('utf-8')


def create_kafka_producer():
    """Create and return a Kafka producer instance"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )


def main():
    producer = create_kafka_producer()
    topic_name = "test_topic"
    print("Started producing messages to Kafka...")
    try:
        while True:
            # Create and send message
            message = create_sample_message()
            producer.send(topic_name, message)
            print(f"Produced message: {message}")
            # Flush and sleep for a second
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping the producer...")
        producer.close()


if __name__ == "__main__":
    main()
