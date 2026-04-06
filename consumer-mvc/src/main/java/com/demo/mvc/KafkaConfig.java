package com.demo.mvc;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);

        // BATCH MODE: the listener receives a List<ConsumerRecord> per poll.
        // Batch size is controlled by max.poll.records in application.properties.
        // Compare with reactive: .bufferTimeout(10, Duration.ofSeconds(2))
        factory.setBatchListener(true);

        // MANUAL_IMMEDIATE: calling ack.acknowledge() commits offsets synchronously
        // on the listener thread — it BLOCKS until Kafka confirms the commit.
        // Compare with reactive: receiverOffset.commit() — non-blocking Mono
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}
