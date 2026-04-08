# Kafka Consumer Dashboard

Real-time visualization of Spring MVC vs Reactive Spring thread behavior.

## Modes

### Simulation (no apps needed)
Open `index.html` directly in your browser. Use the sliders to adjust
partition count and I/O latency and hit **Start** to animate the thread model.

### Live (real JVM data)
Start both consumer apps:

```bash
# Terminal 1 — MVC consumer (port 8081)
cd consumer-mvc && ./mvnw spring-boot:run

# Terminal 2 — Reactive consumer (port 8082)
cd consumer-reactive && ./mvnw spring-boot:run

# Terminal 3 — Producer (sends events to Kafka)
cd producer && ./mvnw spring-boot:run
```

Then open `dashboard/index.html`. The dashboard auto-detects both apps and
switches from **Simulation** to **Live** mode, showing real JVM thread data
from your running consumers.

## What the dashboard needs from the apps

Each app must expose `GET /metrics` returning JSON:

```json
{
  "threadCount": 42,
  "kafkaThreadCount": 4,
  "blockedCount": 3,
  "messagesProcessed": 1024,
  "threads": [
    { "name": "kafka-listener-0", "state": "TIMED_WAITING", "isKafkaThread": true },
    { "name": "main", "state": "RUNNABLE", "isKafkaThread": false }
  ]
}
```

See the root README for how to add this endpoint using Spring Boot Actuator
and a custom `MetricsController`.
