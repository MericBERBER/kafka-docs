package com.mrcbrbr.kafka.streams;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class BasicStreamsApp {

    public static void main(String[] args) {

        Properties streamingProperties = new Properties();
        streamingProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        streamingProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(streamingProperties);

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> simpleFirstStream = builder.stream("src-topic", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(value -> value.toUpperCase());

        upperCasedStream
                .peek((key,value) -> System.out.println(key + ":" + value))
                .to("out-topic", Produced.with(stringSerde, stringSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        kafkaStreams.start();
    }
}
