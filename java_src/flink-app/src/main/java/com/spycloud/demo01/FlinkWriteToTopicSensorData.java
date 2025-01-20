package com.spycloud.demo01;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkWriteToTopicSensorData {

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
                .setTopic("sensor-data")
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

        for (int i = 0; i < count; i++) {
            Map<String, Object> person = new HashMap<>();
            person.put("id", faker.number().randomNumber(5, true));
            person.put("name", faker.name().firstName());
            person.put("surname", faker.name().lastName());
            person.put("email", faker.internet().emailAddress());
            person.put("phone", faker.phoneNumber().phoneNumber());
            person.put("address", faker.address().fullAddress());
            person.put("birthdate", faker.date().birthday(18, 80).toString());
            person.put("job", faker.job().title());
            person.put("company", faker.company().name());
            person.put("credit_card", faker.finance().creditCard());
            person.put("salary", faker.number().randomDouble(2, 30000, 120000));

            String jsonString = gson.toJson(person);
            jsonList.add(jsonString);
        }
        return jsonList;
    }
}
