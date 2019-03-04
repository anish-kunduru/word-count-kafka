package com.baeldung.spring.kafka;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
        
        MessageProducer producer = context.getBean(MessageProducer.class);

        InputStream is = new ClassPathResource("mobydick.txt").getInputStream();
        BufferedInputStream bis = new BufferedInputStream(is);
        Scanner scanner = new Scanner(bis);

        long sentWordCount = 0L;
        while (scanner.hasNext()){
            String word = scanner.next().toLowerCase().replaceAll("[\\W]", "");
            if (!word.isEmpty()) {
                producer.sendMessage();
                sentWordCount++;
            }

            if (sentWordCount % 1000 == 0){
                System.out.println("Sent: " + sentWordCount + " messages.");
            }
        }
        System.out.println("Sent: " + sentWordCount + " messages to kafka topic.");

        scanner.close();
        bis.close();
        is.close();
        context.close();
    }

    @Bean
    public MessageProducer messageProducer() {
        return new MessageProducer();
    }

    public static class MessageProducer {

        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        @Value(value = "${message.topic.name}")
        private String topicName;

        public void sendMessage(String message) {
            kafkaTemplate.send(topicName, "moby-dick", message);
        }
    }
}
