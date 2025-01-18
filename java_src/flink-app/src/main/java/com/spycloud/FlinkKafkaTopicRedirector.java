package com.spycloud;



import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlinkKafkaTopicRedirector {

    public static void main(String[] args) throws Exception {
        // 1. Create Stream Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Create Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("kafka1:29092,kafka2:29093,kafka3:29094")
            .setTopics("sensor-data")
            .setGroupId("flink-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // 3. Create DataStream from the Kafka source
        DataStream<String> inputStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // 4. Process the incoming data (similar to process_sensor_data in PyFlink)
        DataStream<String> processedStream = inputStream
            .map(new ProcessUserDataMapFunction());

         // 4. Define a Kafka Sink
        KafkaSink<String> stdoutSink = KafkaSink.<String>builder()
        .setBootstrapServers("kafka1:29092,kafka2:29093,kafka3:29094")
        .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("std-out")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 5. Print the results
        // processedStream.print();
        processedStream.sinkTo(stdoutSink);
        // 6. Execute the job
        env.execute("Flink From SensorData to StdOut");
    }

    // Equivalent of your process_sensor_data function
    public static class ProcessUserDataMapFunction implements MapFunction<String, String> {
        private static final Gson gson = new Gson();

        @Override
        public String map(String value) {
            try {
                JsonObject userData = gson.fromJson(value, JsonObject.class);

                // Prepare result
                JsonObject result = new JsonObject();
                result.addProperty("user_id", userData.get("id").getAsString());

                String fullName = userData.get("name").getAsString() 
                                  + " " 
                                  + userData.get("surname").getAsString();
                result.addProperty("full_name", fullName);

                double salary = userData.get("salary").getAsDouble();
                String salaryCategory;
                if (salary > 80000) {
                    salaryCategory = "High";
                } else if (salary > 50000) {
                    salaryCategory = "Medium";
                } else {
                    salaryCategory = "Low";
                }
                result.addProperty("salary_category", salaryCategory);
                result.addProperty("salary", salary);

                result.addProperty("job", userData.get("job").getAsString());
                result.addProperty("company", userData.get("company").getAsString());

                return gson.toJson(result);

            } catch (JsonSyntaxException | IllegalStateException e) {
                // Return error JSON if parsing fails or fields are missing
                JsonObject errorObj = new JsonObject();
                errorObj.addProperty("error", e.getMessage());
                errorObj.addProperty("raw_data", value);
                return gson.toJson(errorObj);
            }
        }
    }
}
