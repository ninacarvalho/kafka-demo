# Kafka Consumer: Reactive vs Spring MVC

## Architecture

```
                         KAFKA BROKER
                       ┌─────────────┐
                       │ user-events │
                       └──────┬──────┘
                              │
              ┌───────────────┴───────────────┐
              │                               │
   ┌──────────▼────────────┐    ┌─────────────▼──────────────┐
   │    SPRING MVC         │    │       REACTIVE              │
   │                       │    │                             │
   │  ┌─ Listener Thread ─┐│    │  ┌─ Kafka Poll Thread ───┐ │
   │  │ poll()            ││    │  │ pushes to Flux        │ │
   │  │ process (blocks)  ││    │  └───────────────────────┘ │
   │  │ commitSync()      ││    │              │              │
   │  │ (repeat)          ││    │    bufferTimeout(10, 2s)   │
   │  └───────────────────┘│    │              │              │
   │                       │    │  ┌─ Batch Scheduler ─────┐ │
   │  One thread per        │    │  │ processBatch()        │ │
   │  partition, always     │    │  │ commitOffset() (async)│ │
   │  occupied              │    │  └───────────────────────┘ │
   └───────────────────────┘    │                             │
                                │  2 threads total,           │
                                │  regardless of partitions   │
                                └─────────────────────────────┘
```

---

## Side-by-Side: The Core Loop

The most important structural difference is in how the main processing loop is expressed.

### Spring MVC

```java
// BatchKafkaListener.java

// Spring calls this method each time it polls a batch from Kafka.
// The listener thread is BLOCKED for the entire duration of each batch.
@KafkaListener(topics = "user-events", groupId = "mvc-consumer")
public void processBatch(List<ConsumerRecord<String, String>> batch, Acknowledgment ack) {

    for (var record : batch) {
        processRecord(record);    // ← blocks for ~100ms per record
    }

    ack.acknowledge();            // ← blocks until Kafka confirms the commit
}
```

Batching is configured, not coded:
```properties
# application.properties
spring.kafka.consumer.max-poll-records=10         # size trigger
spring.kafka.consumer.properties.fetch.max.wait.ms=2000  # time trigger
```

---

### Reactive

```java
// ReactiveKafkaConsumer.java

subscription = kafkaReceiver
    .receive()                                      // push model: Kafka feeds the Flux

    .bufferTimeout(10, Duration.ofSeconds(2))       // batching: 10 records OR 2s — in code, not config

    .filter(batch -> !batch.isEmpty())

    .concatMap(batch ->                             // sequential: one batch at a time
        processBatch(batch)                         // Mono<Void> — describes work, doesn't run it yet
            .then(commitOffset(batch))              // chained: commit only if processing succeeded
            .subscribeOn(batchScheduler)            // run on dedicated thread, not the poll thread
    )

    .retryWhen(Retry.fixedDelay(1, Duration.ofSeconds(1)).filter(this::isTransient))
    .subscribe();
```

Commit is non-blocking:
```java
// Async commit — returns a Mono, does NOT block the scheduler thread
private Mono<Void> commitOffset(List<ReceiverRecord<String, String>> batch) {
    return batch.getLast().receiverOffset().commit();
}
```

---

## Key Differences

| Aspect              | Spring MVC                                    | Reactive                                        |
|---------------------|-----------------------------------------------|-------------------------------------------------|
| **Pull vs Push**    | Spring polls Kafka, calls your method         | Kafka pushes records into a `Flux`              |
| **Threading**       | 1 listener thread per partition (blocked)     | 2 threads total: poll + scheduler               |
| **Batching**        | Configured via properties (`max.poll.records`)| Declared in pipeline: `.bufferTimeout(10, 2s)`  |
| **Offset commit**   | `ack.acknowledge()` → `commitSync()` (blocks) | `.commit()` returns a `Mono` (non-blocking)     |
| **Backpressure**    | Manual: Kafka slows down when thread is busy  | Built-in: operators propagate demand upstream   |
| **Error handling**  | Try/catch inline, retry logic in the method   | `.retryWhen(...)` operator in the pipeline      |
| **Shutdown**        | Spring waits for current batch to finish      | `subscription.dispose()` + wait for in-flight   |
| **Concurrency**     | Implicit (one thread per partition)           | Explicit (`.subscribeOn`, `concatMap` vs `flatMap`) |

---

## Threading Deep Dive

This is the most important difference for I/O-bound workloads.

### Python analogy

If you're familiar with Python async:

```
MVC  ≈  threading.Thread  — one thread blocks per task
Reactive ≈  asyncio        — one event loop, tasks yield on I/O
```

### MVC: thread-per-partition

```
Partition 0 → Thread A:  [poll][process 100ms][commit][poll][process 100ms][commit]...
Partition 1 → Thread B:  [poll][process 100ms][commit][poll][process 100ms][commit]...
Partition 2 → Thread C:  [poll][process 100ms][commit][poll][process 100ms][commit]...
```

Each thread is **blocked** while processing. With 50 partitions and 100ms processing, you need 50 threads — all sitting idle most of the time, each consuming ~1MB of stack.

### Reactive: event loop

```
Poll Thread:   [push batch 1]──[push batch 2]──[push batch 3]──...
                     │               │               │
Scheduler:     [process + commit]──[process + commit]──[process + commit]──...
               (sequential, one at a time)
```

The scheduler thread does real work. The poll thread is never blocked. Total: 2 threads, regardless of partition count.

---

## When to Use Each

### Use Spring MVC (`@KafkaListener`) when:

- **Simplicity is the priority** — less infrastructure to understand
- **Low throughput** — processing takes < 10ms per record
- **CPU-bound work** — no async benefit if you're computing, not waiting
- **Small team** — reactive has a steeper learning curve
- **Existing Spring Boot app** — minimal integration overhead

### Use Reactive (`KafkaReceiver`) when:

- **I/O-bound processing** — writes to databases, HTTP calls, file I/O
- **High throughput + low latency** — need to keep Kafka poll thread free
- **Thread pool exhaustion is a risk** — many partitions or slow downstream
- **You already use Project Reactor** — e.g., Spring WebFlux in the same app
- **Fine-grained pipeline control** — backpressure, branching, merge strategies

---

## Why Reactive Was Chosen for the Real Use Case

The production system this demo is based on writes Kafka events to **Apache Iceberg** (a columnar table format on object storage like S3). Iceberg writes are:

- **Slow** (~200–500ms per batch commit)
- **I/O-bound** (network + storage)
- **Sequential by design** (atomic commits, no concurrent writes)

In the MVC model, this would mean a listener thread blocked for 500ms per batch. With 20 partitions, that's 20 permanently-occupied threads waiting on I/O — the classic thread pool exhaustion scenario.

In the reactive model, the scheduler thread submits the Iceberg write and the commit is non-blocking from the pipeline's perspective. The poll thread remains free to receive the next batch. Total resource usage is flat regardless of partition count.

```
MVC with 20 partitions + 500ms Iceberg write:
  20 threads × 500ms blocked = 10 thread-seconds wasted per second

Reactive with 20 partitions + 500ms Iceberg write:
  1 scheduler thread, 1 poll thread = constant overhead
```

The reactive model also made graceful shutdown simpler: `subscription.dispose()` stops new batches from starting, and `processingBatch` tracks whether an atomic write is in progress — no need to drain a thread pool.
