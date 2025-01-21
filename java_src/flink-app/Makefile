create_topics:
		docker exec kafka1 kafka-topics --create --topic std-out --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1
		docker exec kafka1 kafka-topics --create --topic test_topic --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1
		docker exec kafka1 kafka-topics --create --topic sensor-data --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1
		docker exec kafka1 kafka-topics --create --topic country --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1
		docker exec kafka1 kafka-topics --create --topic user --bootstrap-server kafka1:29092 --partitions 1 --replication-factor 1

list_topics:
	docker exec kafka1 kafka-topics   --list   --bootstrap-server kafka1:29092

f_write_to_std_out:
	docker exec jobmanager flink run -c com.spycloud.demo01.FlinkWriteToTopicStdOut -d /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar
f_write_to_sensor_data:
	docker exec jobmanager flink run -c com.spycloud.demo01.FlinkWriteToTopicSensorData -d /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar
f_fr_sensor_data_to_std_out:
	docker exec jobmanager flink run -c com.spycloud.demo01.FlinkKafkaTopicRedirector -d /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar
f_print_std_out:
	docker exec jobmanager java -cp   /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar com.spycloud.demo01.FlinkPrintKafkaTopicStdOut &


run_demo01: f_print_std_out f_fr_sensor_data_to_std_out f_write_to_sensor_data 
	@echo done

run_demo02:
	docker exec jobmanager java -cp   /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar com.spycloud.demo02.BroadcastJoinInMemory
	@echo done
run_demo03:
	docker exec jobmanager java -cp   /usr/local/workdir/java_src/flink-app/target/flink-app-1.0-SNAPSHOT-fat.jar com.spycloud.demo02.BroadcastJoinWithKafkaTopic
	@echo done

p:
	mvn clean package
