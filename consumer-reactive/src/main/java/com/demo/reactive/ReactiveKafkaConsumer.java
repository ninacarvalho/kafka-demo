package com.demo.reactive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reactive Kafka consumer using Project Reactor + reactor-kafka.
 *
 * Execution model (compare with BatchKafkaListener):
 *
 *   ┌─ Kafka poll thread ──────────────────────────────────────────────────┐
 *   │  Pushes incoming records into the Flux (non-blocking)               │
 *   └──────────────────────────────────────────────────────────────────────┘
 *                              │
 *                    .bufferTimeout(10, 2s)
 *                              │
 *                         .concatMap(...)
 *                              │
 *   ┌─ batchScheduler (single thread) ────────────────────────────────────┐
 *   │  processBatch() → commitOffset()   (one batch at a time)           │
 *   └──────────────────────────────────────────────────────────────────────┘
 *
 * Threading: only 2 threads total (poll + scheduler), regardless of partition count.
 * The scheduler thread is NEVER blocked waiting for a commit — commit is async.
 */
@Service
public class ReactiveKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReactiveKafkaConsumer.class);
    public static final AtomicLong messagesProcessed = new AtomicLong(0);

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Single-threaded scheduler: ensures batches are processed one at a time (sequentially).
    // Using a dedicated thread keeps blocking work (Thread.sleep) off the Kafka poll thread.
    private final Scheduler batchScheduler = Schedulers.newSingle("kafka-batch");

    // Tracks whether a batch is in flight — used to delay shutdown until the batch completes.
    private final AtomicBoolean processingBatch = new AtomicBoolean(false);

    private Disposable subscription;

    public ReactiveKafkaConsumer(KafkaReceiver<String, String> kafkaReceiver) {
        this.kafkaReceiver = kafkaReceiver;
    }

    @PostConstruct
    public void start() {
        log.info("Starting reactive Kafka consumer on topic 'user-events'");

        subscription = kafkaReceiver

            // Kafka pushes records into this Flux as they arrive (push model).
            // Compare with MVC: Spring calls our method when it polls (pull model).
            .receive()

            // ── BATCHING ────────────────────────────────────────────────────────
            // Collect records into a List until we have 10 OR 2 seconds pass.
            // This is a first-class operator — no loops, no timers, no state.
            // Compare with MVC: max.poll.records=10 + fetch.max.wait.ms=2000 in config
            .bufferTimeout(10, Duration.ofSeconds(2))

            .filter(batch -> !batch.isEmpty())

            // ── SEQUENTIAL PROCESSING ───────────────────────────────────────────
            // concatMap: subscribes to the inner Mono only AFTER the previous one completes.
            // This guarantees one batch at a time — no concurrent processing.
            // (flatMap would run batches concurrently, which we don't want here)
            .concatMap(batch ->
                processBatch(batch)
                    // ── CHAINED OFFSET COMMIT ───────────────────────────────────
                    // .then() means: "after processBatch succeeds, run commitOffset".
                    // If processBatch throws, we never reach commitOffset → no commit →
                    // the broker will redeliver on restart (at-least-once delivery).
                    // Compare with MVC: ack.acknowledge() is called explicitly after the loop
                    .then(commitOffset(batch))
                    // Run on the single-threaded scheduler (keeps poll thread free)
                    .subscribeOn(batchScheduler)
            )

            // ── RETRY ───────────────────────────────────────────────────────────
            // On error, wait 1 second and retry the ENTIRE stream once.
            // The filter ensures we only retry transient errors (not bad JSON).
            .retryWhen(
                Retry.fixedDelay(1, Duration.ofSeconds(1))
                    .filter(this::isTransient)
                    .doBeforeRetry(s ->
                        log.warn("[RETRY] Retrying stream after: {}", s.failure().getMessage()))
            )

            .doOnError(e -> log.error("[FATAL] Stream failed after retry: {}", e.getMessage()))
            .doOnTerminate(() -> log.warn("[STREAM] Kafka stream terminated"))
            .subscribe();

        log.info("Reactive Kafka consumer started");
    }

    // ── BATCH PROCESSING ────────────────────────────────────────────────────────
    // Returns Mono<Void>: signals completion (or error) without emitting a value.
    // Using fromRunnable keeps the code simple; the work runs on batchScheduler.
    private Mono<Void> processBatch(List<ReceiverRecord<String, String>> batch) {
        return Mono.<Void>fromRunnable(() -> {
            processingBatch.set(true);
            log.info("[BATCH-START] Processing {} records", batch.size());

            for (var record : batch) {
                processRecord(record);
            }

            log.info("[BATCH-DONE] Finished processing {} records", batch.size());
        }).doFinally(signal -> processingBatch.set(false));
    }

    private void processRecord(ReceiverRecord<String, String> record) {
        try {
            var event = objectMapper.readValue(record.value(), UserEvent.class);

            // Simulate work (replaces real I/O like an Iceberg write in production).
            // This blocks the batchScheduler thread, but NOT the Kafka poll thread.
            Thread.sleep(100);

            messagesProcessed.incrementAndGet();
            log.info("[PROCESSED] partition={}, offset={}, event={}",
                record.partition(), record.offset(), event);

        } catch (JsonProcessingException e) {
            // Permanent error: bad JSON will never succeed — log and skip
            log.warn("[SKIP] Bad JSON at offset={}: {}", record.offset(), e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── OFFSET COMMIT ───────────────────────────────────────────────────────────
    // Commits the offset of the LAST record, which implicitly covers all prior offsets.
    // Returns Mono<Void>: NON-BLOCKING — does not pause the scheduler thread.
    // Compare with MVC: ack.acknowledge() → commitSync() → blocks the listener thread.
    private Mono<Void> commitOffset(List<ReceiverRecord<String, String>> batch) {
        var lastRecord = batch.getLast();
        return lastRecord.receiverOffset().commit()
            .doOnSuccess(v ->
                log.info("[COMMIT] Committed offset {} on partition {}",
                    lastRecord.offset(), lastRecord.partition()))
            .doOnError(e ->
                log.error("[COMMIT-FAIL] offset={}: {}", lastRecord.offset(), e.getMessage()));
    }

    private boolean isTransient(Throwable e) {
        // JSON errors are permanent — retrying won't fix malformed data
        if (e instanceof JsonProcessingException) return false;
        if (e.getCause() instanceof JsonProcessingException) return false;
        return true;
    }

    @PreDestroy
    public void stop() {
        log.info("[SHUTDOWN] Stopping reactive Kafka consumer...");

        // Stop the stream: no new batches will be started after dispose()
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }

        // Wait briefly for any in-flight batch to complete before releasing resources
        if (processingBatch.get()) {
            log.info("[SHUTDOWN] In-flight batch detected — waiting up to 500ms...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        batchScheduler.dispose();
        log.info("[SHUTDOWN] Reactive Kafka consumer stopped");
    }
}
