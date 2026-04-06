package com.demo.reactive;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * reactor-kafka does NOT use Spring Boot's auto-configuration (spring.kafka.*).
 * We configure KafkaReceiver manually here.
 */
@Configuration
public class KafkaReceiverConfig {

    @Bean
    public ReceiverOptions<String, String> receiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reactive-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // CRITICAL: disable auto-commit so we can commit offsets manually after each batch.
        // This gives us at-least-once delivery semantics (no data loss).
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return ReceiverOptions.<String, String>create(props)
            .subscription(Collections.singleton("user-events"));
    }

    @Bean
    public KafkaReceiver<String, String> kafkaReceiver(ReceiverOptions<String, String> receiverOptions) {
        return KafkaReceiver.create(receiverOptions);
    }
}
