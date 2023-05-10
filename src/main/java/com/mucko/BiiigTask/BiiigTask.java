package com.mucko.BiiigTask;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class BiiigTask {
    private static final String INPUT_TOPIC = "gamer-topic";
    private static final String APP_ID = "zookeeper";
    private static final Properties streamsConfiguration = new Properties();
    private static final String bootstrapServers = "localhost:9092";

    public static void main(String[] args) {
        //Configuring Kafka Streams input
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        //streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "huuuge-task");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, createDirectory("kafka-streams"));
        streamsConfiguration.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Building a streaming topology - definition of how events should be handled and transformed
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> gamerData = streamsBuilder.stream(INPUT_TOPIC);
        Pattern pattern = Pattern.compile(",", Pattern.UNICODE_CHARACTER_CLASS);

        KTable<String, Long> wordCounts = (KTable<String, Long>) gamerData
                .peek((k,v)-> System.out.println(k + "  ->  " + v))

                //.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .peek((k,v)-> System.out.println(k + "   " + v))
                .groupBy((key, word) -> word);
                //.count();


        //handling results
        System.out.println("start handling result");
        wordCounts.toStream().foreach((word, count) -> System.out.println("word: " + word + " -> " + count));
        System.out.println("finish handling result");
        String outputTopic = "outputTopic";
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));


        //starting kafa streams job
        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        try {
            Thread.sleep(600000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        streams.close();

    }

    private static String createDirectory(String folderName) {
        String stateDirectory = "Directory not created";
        try {
            stateDirectory = Files.createTempDirectory(folderName).toAbsolutePath().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stateDirectory;
    }
}
