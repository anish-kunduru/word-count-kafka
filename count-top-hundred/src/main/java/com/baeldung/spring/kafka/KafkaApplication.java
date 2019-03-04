package com.baeldung.spring.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(KafkaApplication.class, args);
    }
}
