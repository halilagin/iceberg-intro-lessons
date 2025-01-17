from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import sys

# Kafka broker list
BROKERS = "localhost:19092,localhost:29092,localhost:39092"
BROKERS = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
TOPIC = "test-topic"

# Function to produce messages
def produce_messages():
    producer = Producer({'bootstrap.servers': BROKERS})

    def delivery_report(err, msg):
        """Delivery report handler."""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    print("Producing messages...")
    for i in range(10):
        message = f"Message {i}"
        producer.produce(TOPIC, message.encode('utf-8'), callback=delivery_report)
        producer.flush()

# Function to consume messages
def consume_messages():
    consumer = Consumer({
        'bootstrap.servers': BROKERS,
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC])

    print("Consuming messages...")
    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("End of partition reached.")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        print("Consumer interrupted.")
    finally:
        consumer.close()

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ["produce", "consume"]:
        print("Usage: python kafka_test.py <produce|consume>")
        sys.exit(1)

    if sys.argv[1] == "produce":
        produce_messages()
    elif sys.argv[1] == "consume":
        consume_messages()
