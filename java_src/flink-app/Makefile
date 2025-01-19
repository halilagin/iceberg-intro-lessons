create_topics:
		docker exec kafka1 kafka-topics --create --topic std-out --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1
		docker exec kafka1 kafka-topics --create --topic test_topic --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1
		docker exec kafka1 kafka-topics --create --topic sensor-data --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1

list_topics:
	docker exec kafka1 kafka-topics   --list   --bootstrap-server kafka1:29092

f_write_to_std_out:
	docker exec jobmanager flink run -c com.spycloud.FlinkWriteToTopicStdOut -d /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar
f_write_to_sensor_data:
	docker exec jobmanager flink run -c com.spycloud.FlinkWriteToTopicSensorData -d /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar
f_fr_sensor_data_to_std_out:
	docker exec jobmanager flink run -c com.spycloud.FlinkKafkaTopicRedirector -d /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar
f_print_std_out:
	java -cp   /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar com.spycloud.FlinkPrintKafkaTopicStdOut


p:
	mvn clean package
