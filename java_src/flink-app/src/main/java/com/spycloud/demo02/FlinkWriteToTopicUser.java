package com.spycloud.demo02;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.spycloud.demo02.BroadcastJoinInMemory.UserEvent;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkWriteToTopicUser {

    public static void main(String[] args) throws Exception {
        // 1. Setup Flink environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Generate fake data
        List<String> records = generateFakePersons(10);

        // 3. Create DataStream from the list
        DataStream<String> sourceStream = env.fromCollection(records);

        // 4. Configure Kafka serialization
        KafkaRecordSerializationSchema<String> recordSerializer =
            KafkaRecordSerializationSchema.<String>builder()
                .setTopic("user")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build();

        // 5. Create Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("host.docker.internal:29092,host.docker.internal:29093,host.docker.internal:29094")
                .setRecordSerializer(recordSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 6. Write to Kafka
        sourceStream.sinkTo(sink);

        // 7. Execute job
        env.execute("Flink Data Producer");
    }

    // Fake data generator using Java Faker + Gson
    private static List<String> generateFakePersons(int count) {
        Faker faker = new Faker();
        Gson gson = new Gson();
        List<String> jsonList = new ArrayList<>();
        List<UserEvent> userList = new ArrayList<>();

        List<UserEvent> knownUserList = Arrays.asList(
            new UserEvent("user1", "US"),
            new UserEvent("user2", "DE"),
            new UserEvent("user3", "XX") // Unknown country code
        );
        userList.addAll(knownUserList);

        for (int i = 0; i < count; i++) {
            UserEvent user = new UserEvent("user"+faker.number().digit(), faker.country().countryCode2());
            userList.add(user);
        }

        userList.forEach(user -> {
            String jsonString = gson.toJson(user);
            jsonList.add(jsonString);
        });
        
        return jsonList;
    }
}
