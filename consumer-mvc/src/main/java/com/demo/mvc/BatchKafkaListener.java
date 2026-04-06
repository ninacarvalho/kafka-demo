package com.demo.mvc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Spring MVC-style Kafka consumer.
 *
 * Execution model (compare with ReactiveKafkaConsumer):
 *
 *   ┌─ Listener thread (blocked for the full batch lifecycle) ────────────┐
 *   │                                                                      │
 *   │  1. Kafka poll() → Spring delivers List<ConsumerRecord> (blocking)  │
 *   │  2. for each record: parse JSON + Thread.sleep(100)  (blocking)     │
 *   │  3. ack.acknowledge() → commitSync()                 (blocking)     │
 *   │  4. return → Spring polls again                                     │
 *   │                                                                      │
 *   └──────────────────────────────────────────────────────────────────────┘
 *
 * Threading: one listener thread per partition, permanently occupied during processing.
 */
@Component
public class BatchKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(BatchKafkaListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Spring Kafka batch listener:
    //   - Receives up to max.poll.records records per invocation (configured in application.properties)
    //   - Waits up to fetch.max.wait.ms if fewer records are available (the "2s timeout" trigger)
    //   - This is the MVC equivalent of reactive's: .bufferTimeout(10, Duration.ofSeconds(2))
    @KafkaListener(topics = "user-events", groupId = "mvc-consumer")
    public void processBatch(List<ConsumerRecord<String, String>> batch, Acknowledgment ack) {
        log.info("[BATCH-START] Received {} records — listener thread is now occupied", batch.size());

        for (var record : batch) {
            processRecord(record);
        }

        // SYNCHRONOUS OFFSET COMMIT
        // ack.acknowledge() with AckMode.MANUAL_IMMEDIATE calls commitSync() under the hood.
        // The listener thread BLOCKS here until the broker confirms the offset.
        //
        // Safety: we commit AFTER processing completes.
        // If the app crashes before this line, records are reprocessed (at-least-once).
        //
        // Compare with reactive: lastRecord.receiverOffset().commit()
        //   → returns a Mono<Void> immediately, does NOT block the scheduler thread
        ack.acknowledge();
        log.info("[BATCH-DONE] Committed offsets. Listener thread is now free to poll again.");
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        int attempts = 0;
        while (attempts <= 1) {
            try {
                var event = objectMapper.readValue(record.value(), UserEvent.class);

                // Simulate work (replaces real I/O like an Iceberg write in production)
                // This blocks the listener thread for 100ms per record.
                Thread.sleep(100);

                log.info("[PROCESSED] partition={}, offset={}, event={}",
                    record.partition(), record.offset(), event);
                return; // success

            } catch (JsonProcessingException e) {
                // PERMANENT error: malformed JSON is never going to succeed — skip it
                log.warn("[SKIP] Unparseable JSON at partition={}, offset={}: {}",
                    record.partition(), record.offset(), e.getMessage());
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                attempts++;
                if (attempts > 1) {
                    log.error("[RETRY-FAIL] Giving up on offset={} after 1 retry: {}",
                        record.offset(), e.getMessage());
                } else {
                    // TRANSIENT error: retry once inline (blocking)
                    log.warn("[RETRY] Transient error at offset={}, retrying: {}",
                        record.offset(), e.getMessage());
                }
            }
        }
    }

    @PreDestroy
    public void stop() {
        // Spring's @KafkaListener container handles graceful shutdown automatically:
        // it waits for the current batch to finish before stopping the listener thread.
        log.info("[SHUTDOWN] MVC consumer shutting down — current batch will complete first");
    }
}
