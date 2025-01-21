package com.spycloud.demo02;


import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.spycloud.demo02.BroadcastJoinInMemory.UserEvent;
import com.spycloud.demo02.model.Country;
import com.spycloud.demo02.model.EnrichedUserEvent;

/**
 * Demonstrates a join of a main DataStream (UserEvent) with
 * a dimension DataStream (CountryInfo) using a broadcast state.
 */
public class BroadcastJoinWithKafkaTopic {

    public static void main(String[] args) throws Exception {
        // 1. Create Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Define dimension data (small, changes infrequently)
        DataStream<Country> countryStream = env.fromElements(
                new Country("US", "United States", 331_002_651L),
                new Country("DE", "Germany", 83_783_942L),
                new Country("FR", "France", 67_084_000L)
        );

        // 3. Broadcast state descriptor (key=countryCode, value=CountryInfo)
        MapStateDescriptor<String, Country> broadcastStateDesc =
                new MapStateDescriptor<>("country-info-broadcast", String.class, Country.class);

        // 4. Broadcast the dimension stream
        BroadcastStream<Country> broadcastCountryStream = countryStream.broadcast(broadcastStateDesc);

        // 5. Define main event stream (e.g., user events referencing a country)
        DataStream<UserEvent> userEventStream = env.fromElements(
                new UserEvent("user1", "US"),
                new UserEvent("user2", "DE"),
                new UserEvent("user3", "XX") // Unknown country code
        );

        // 6. Connect the main stream with the broadcast dimension stream
        DataStream<EnrichedUserEvent> enrichedStream = userEventStream
            .connect(broadcastCountryStream)
            .process(new EnrichmentFunction(broadcastStateDesc));

        // 7. Print enriched results
        enrichedStream.print();

        // 8. Execute the job
        env.execute("Broadcast Join Example");
    }

    /**
     * BroadcastProcessFunction to update the broadcast state with dimension data
     * and enrich the main event stream using that broadcast data.
     */
    public static class EnrichmentFunction
            extends BroadcastProcessFunction<UserEvent, Country, EnrichedUserEvent> {

        private final MapStateDescriptor<String, Country> broadcastStateDesc;

        public EnrichmentFunction(MapStateDescriptor<String, Country> broadcastStateDesc) {
            this.broadcastStateDesc = broadcastStateDesc;
        }

        @Override
        public void processBroadcastElement(Country countryInfo,
                                            Context ctx,
                                            Collector<EnrichedUserEvent> out) throws Exception {
            // Update the broadcast state so all parallel instances have access
            BroadcastState<String, Country> broadcastState =
                    ctx.getBroadcastState(broadcastStateDesc);
            broadcastState.put(countryInfo.countryCode, countryInfo);
        }

        @Override
        public void processElement(UserEvent userEvent,
                                   ReadOnlyContext ctx,
                                   Collector<EnrichedUserEvent> out) throws Exception {
            // Read-only access to the broadcast state
            ReadOnlyBroadcastState<String, Country> broadcastState =
                    ctx.getBroadcastState(broadcastStateDesc);

                    Country countryInfo = broadcastState.get(userEvent.countryCode);

            // If countryCode is not found, handle accordingly
            if (countryInfo != null) {
                EnrichedUserEvent enriched = new EnrichedUserEvent(
                        userEvent.userId,
                        userEvent.countryCode,
                        countryInfo.countryName,
                        countryInfo.population
                );
                out.collect(enriched);
            } else {
                // Handle unknown country codes
                out.collect(new EnrichedUserEvent(userEvent.userId, userEvent.countryCode,
                        "Unknown", -1));
            }
        }
    }





    
}