
workdir="/usr/local/workdir"


download_jars:
	rm -f flink_basics/jars/*
	mkdir -p flink_basics/jars
	wget -P flink_basics/jars https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/flink-connector-kafka-1.17.1.jar
	wget -P flink_basics/jars https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar
	wget -P flink_basics/jars https://repo1.maven.org/maven2/org/apache/flink/flink-streaming-java/1.17.1/flink-streaming-java-1.17.1.jar
	wget -P flink_basics/jars https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-guava/30.1.1-jre-15.0/flink-shaded-guava-30.1.1-jre-15.0.jar

create_topics:
	docker exec kafka kafka-topics --create --topic sensor-data --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
list_jars:
	docker exec flink-jobmanager ls /opt/flink/lib

run_producer:
	#docker exec flink-jobmanager flink run -py /usr/local/workdir/python_src/flink_basics/flink_basics/flink_producer.py
	docker exec flink-jobmanager python /usr/local/workdir/python_src/flink_basics/flink_basics/flink_producer.py

run_consumer:
	#docker exec flink-jobmanager flink run -py /usr/local/workdir/python_src/flink_basics/flink_basics/flink_consumer.py
	docker exec flink-jobmanager python /usr/local/workdir/python_src/flink_basics/flink_basics/flink_consumer.py
