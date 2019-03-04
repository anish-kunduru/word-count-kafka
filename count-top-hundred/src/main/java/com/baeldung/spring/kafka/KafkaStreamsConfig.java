package com.baeldung.spring.kafka;

import avro.wordcount.TopCounts;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    private static Set<String> STOP_WORDS = new HashSet<>();

    static {
        try {
            InputStream is = new ClassPathResource("stop-words.txt").getInputStream();
            BufferedInputStream bis = new BufferedInputStream(is);
            Scanner scanner = new Scanner(bis);

            while (scanner.hasNext()) {
                String word = scanner.next().toLowerCase().replaceAll("[\\W]", "");
                if (!word.isEmpty()) {
                    STOP_WORDS.add(word);
                }
            }

            scanner.close();
            bis.close();
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Value(value = "${input.topic.name}")
    private String inputTopic;

    @Value(value = "${output.topic.name}")
    private String outputTopic;

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${kafka.schemaReg}")
    String schemaReg;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "count100");
        props.put("schema.registry.url", schemaReg);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 30000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    KStream<String, Integer> rekeyedInputStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> rawInputStream = streamsBuilder.stream(inputTopic);
        KStream<String, String> notStopWordsStream = rawInputStream.filterNot((k, v) -> STOP_WORDS.contains(v));

        return notStopWordsStream.map((k, v) -> new KeyValue<>(v, 1));
    }

    @Bean
    // NOTE: Since the input topic has a partition of 1, we know that we don't need to have an ending stateless operation.
    // This would be stateless in-memory operation, since we would only have to hold the top 100 current records at that point.
    // In order to do this, you'd have to have a final input stream partitioned by one and would likely hold internal state for the current min value (to know if one should be evicted).
    public KTable<String, TopCounts> top100Results(StreamsBuilder streamBuilder) {
        KTable<String, TopCounts> topCountsKTable = rekeyedInputStream(streamBuilder).groupByKey().aggregate(
                () -> TopCounts.newBuilder().build(),
                (key, newValue, oldValue) -> {
                    Map<String, Long> data = oldValue.getData() == null ? new HashMap<>() : oldValue.getData();
                    Long count = data.get(key);
                    data.put(key, count == null ? newValue : count + newValue);
                    return oldValue;
                });

        topCountsKTable.toStream().through(outputTopic);
        return topCountsKTable;
    }

}