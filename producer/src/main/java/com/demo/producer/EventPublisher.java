package com.demo.producer;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    private static final String TOPIC = "user-events";
    private static final int TOTAL_EVENTS = 500;
    private static final long DELAY_MS = 20; // 1000ms / 50 msg/s = 20ms per message

    private final KafkaTemplate<String, UserEvent> kafkaTemplate;
    private final ConfigurableApplicationContext context;

    public EventPublisher(KafkaTemplate<String, UserEvent> kafkaTemplate,
                          ConfigurableApplicationContext context) {
        this.kafkaTemplate = kafkaTemplate;
        this.context = context;
    }

    @PostConstruct
    public void publishEvents() throws InterruptedException {
        log.info("Starting publisher: sending {} events to topic '{}' at ~50 msg/sec",
            TOTAL_EVENTS, TOPIC);

        for (int i = 1; i <= TOTAL_EVENTS; i++) {
            var event = new UserEvent(
                UUID.randomUUID().toString(),
                "User-" + i,
                "user" + i + "@example.com"
            );

            kafkaTemplate.send(TOPIC, event.id(), event);
            log.info("[{}] Published event #{}: {}", Instant.now(), i, event);

            Thread.sleep(DELAY_MS); // throttle to ~50 msg/sec
        }

        // Flush ensures all buffered records are delivered before we shut down
        kafkaTemplate.flush();
        log.info("Done. All {} events published to topic '{}'", TOTAL_EVENTS, TOPIC);

        // Close the Spring context — triggers graceful shutdown
        context.close();
    }
}
