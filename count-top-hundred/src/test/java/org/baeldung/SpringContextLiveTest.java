package org.baeldung;

import avro.wordcount.TopCounts;
import com.baeldung.spring.kafka.KafkaStreamsConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@Configuration
@EmbeddedKafka(topics = {"${input.topic.name}", "${output.topic.name}"})
public class SpringContextLiveTest {

    @Value(value = "${input.topic.name}")
    String INPUT_TOPIC;

    @Value(value = "${output.topic.name}")
    String OUTPUT_TOPIC;

    @Value(value = "${kafka.bootstrapAddress}")
    String bootstrapAddress;

    @Value(value = "${kafka.schemaReg}")
    String schemaReg;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private static final int COUNTS = 12;
    private static final int RANDOM_COUNTS = 10;
    private static final String RANDOM_STRING = "WORD";

    @Configuration
    @EnableKafkaStreams
    public static class KafkaStreamsConfiguration {

        @Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
        private String brokerAddresses;

        @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
        public org.springframework.kafka.config.KafkaStreamsConfiguration kStreamsConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "embeddedTest");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 30000);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
            return new org.springframework.kafka.config.KafkaStreamsConfiguration(props);
        }
    }

    @Before
    public void setupInputTopic(){
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
        Producer<String, String> producer = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer()).createProducer();

        for (int i =0; i < COUNTS; i++){
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test-moby-dick", RANDOM_STRING));
        }

        for (int i = 0; i < RANDOM_COUNTS; i++){
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test-moby-dick", UUID.randomUUID().toString()));
        }

        producer.flush();
    }

    @Test
    public void makeSureDataIsInTopic() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dataInTopic", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
        Consumer<String, String> consumer = cf.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, INPUT_TOPIC);
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
        assertThat(records).isNotNull();
    }

    @Test
    public void testKafkaStreamsConfig() throws Exception {
        this.streamsBuilderFactoryBean.start();
        KafkaStreamsConfig kafkaStreamsConfig = new KafkaStreamsConfig();
        kafkaStreamsConfig.top100Results(streamsBuilderFactoryBean.getObject());

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dataInTopic", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put("schema.registry.url", schemaReg);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        DefaultKafkaConsumerFactory<String, TopCounts> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<String, TopCounts> consumer = cf.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, OUTPUT_TOPIC);

        ConsumerRecords<String, TopCounts> records = KafkaTestUtils.getRecords(consumer);
        ConsumerRecord<String, TopCounts> latestRecord = null;
        int recordCount = 0;
        for (ConsumerRecord r : records){
            latestRecord = r;
            recordCount++;
        }

        assertThat(recordCount).isEqualTo(COUNTS + RANDOM_COUNTS);
        assertThat(latestRecord).isNotNull();
        assertThat(latestRecord.value().getData().get(RANDOM_STRING)).isEqualTo(COUNTS);
    }

    @After
    public void tearDown() {
       this.streamsBuilderFactoryBean.stop();
    }
}