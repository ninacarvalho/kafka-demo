# Kafka Consumer: Reactive vs Spring MVC — Demo

A runnable comparison of two approaches to Kafka batch consumption in Java 21 + Spring Boot 3.3.

See [COMPARISON.md](COMPARISON.md) for the full architectural breakdown and when to use each.

---

## Prerequisites

- Java 21
- Maven 3.9+ (or use the included `./mvnw`)
- Docker + Docker Compose

---

## Setup

### 1. Start Kafka

```bash
docker-compose up -d
```

Verify it's running:

```bash
docker ps
# should show: zookeeper, kafka
```

### 2. Build all three projects

```bash
cd producer          && ./mvnw package -q && cd ..
cd consumer-mvc      && ./mvnw package -q && cd ..
cd consumer-reactive && ./mvnw package -q && cd ..
```

---

## Running the Demo

Open three terminal windows.

### Terminal 1 — MVC Consumer

```bash
cd consumer-mvc
./mvnw spring-boot:run
```

Waits for messages. You'll see:
```
[BATCH-START] Received 10 records — listener thread is now occupied
[PROCESSED] partition=0, offset=0, event=UserEvent[id=..., name=User-1, email=...]
...
[BATCH-DONE] Committed offsets. Listener thread is now free to poll again.
```

### Terminal 2 — Reactive Consumer

```bash
cd consumer-reactive
./mvnw spring-boot:run
```

Waits for messages. You'll see:
```
[BATCH-START] Processing 10 records
[PROCESSED] partition=0, offset=0, event=UserEvent[id=..., name=User-1, email=...]
...
[COMMIT] Committed offset 9 on partition 0
```

### Terminal 3 — Producer

```bash
cd producer
./mvnw spring-boot:run
```

Publishes 500 events at ~50 msg/sec and exits:
```
[12:00:00.020] Published event #1: UserEvent[id=..., name=User-1, email=user1@example.com]
[12:00:00.040] Published event #2: ...
...
Done. All 500 events published to topic 'user-events'
```

> Both consumers use different `group-id` values (`mvc-consumer` and `reactive-consumer`),
> so each receives the full set of 500 events independently.

---

## Running consumers one at a time

If you want to compare outputs in isolation, start only one consumer before running the producer. The other consumer can be started afterwards — it will replay from the beginning because `auto-offset-reset=earliest`.

---

## Teardown

```bash
docker-compose down
```

To also wipe the Kafka topic data:

```bash
docker-compose down -v
```

---

## Project Structure

```
kafka-demo/
├── docker-compose.yml         Kafka + Zookeeper (single broker)
├── COMPARISON.md              Architecture diagrams, side-by-side code, decision guide
├── producer/                  Publishes 500 UserEvents to 'user-events'
├── consumer-mvc/              Spring @KafkaListener, batch mode, commitSync
└── consumer-reactive/         KafkaReceiver + Project Reactor pipeline
```

Each project is a standalone Spring Boot app with its own `pom.xml`. No shared parent module.
