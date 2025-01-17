# Python virtual environment




# Kafka scripts
PRODUCER = "kafka_producer.py"
CONSUMER = "kafka_consumer.py"

# Requirements file
REQUIREMENTS = requirements.txt



create_topics:
	 docker exec kafka1 kafka-topics   --create   --topic test-topic   --partitions 1   --replication-factor 1   --if-not-exists   --bootstrap-server kafka1:9092
	 docker exec kafka1 kafka-topics   --create   --topic sensor-data   --partitions 1   --replication-factor 1   --if-not-exists   --bootstrap-server kafka1:9092


list_topics:
	docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:9092


# Run Kafka producer
.PHONY: producer
run_producer: 
	python $(PRODUCER)

# Run Kafka consumer
.PHONY: consumer
run_consumer: 
	python $(CONSUMER)

# Run both producer and consumer in background
.PHONY: run
test_kafka: 
	@echo "Starting Kafka consumer in background..."
	python  kafka_producer.py > producer.log 2>&1 &
	# @echo "Starting Kafka prodm runucer in background..."
	python kafka_consumer.py > consumer.log 2>&1 &
	# @echo "Both processes started. Check consumer.log and producer.log for output."

# Stop Kafka processes
.PHONY: stop
stop:
	@echo "Stopping Kafka processes..."
	@pkill -f "$(CONSUMER)" || true
	@pkill -f "$(PRODUCER)" || true
	@echo "Kafka processes stopped"

# Show logs
.PHONY: logs
logs:
	@echo "=== Consumer Log ==="
	@cat consumer.log || echo "No consumer log found"
	@echo "\n=== Producer Log ==="
	@cat producer.log || echo "No producer log found"

# Follow logs
.PHONY: follow-logs
follow-logs:
	@tail -f consumer.log producer.log

# Clean up
.PHONY: clean
clean: stop
	rm -f consumer.log producer.log
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make install      - Install dependencies"
	@echo "  make producer     - Run producer in foreground"
	@echo "  make consumer     - Run consumer in foreground"
	@echo "  make run          - Run both producer and consumer in background"
	@echo "  make stop         - Stop all Kafka processes"
	@echo "  make logs         - Show all logs"
	@echo "  make follow-logs  - Follow logs in real-time"
	@echo "  make clean        - Clean up logs and cache files" 
