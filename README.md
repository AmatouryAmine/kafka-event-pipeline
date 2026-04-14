# Kafka Event Pipeline

Event-driven pipeline with Kafka, dead letter topics, and idempotent consumers.

![Java](https://img.shields.io/badge/Java-17-blue?logo=java)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.2.5-brightgreen?logo=spring)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-black?logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Architecture

```
REST Client
    │
    ▼
POST /api/orders
    │
    ▼
ProducerController
    │
    ▼
OrderProducer ──────────► Kafka Topic: orders
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ▼                       ▼
            (success path)          (failure after retries)
                    │                       │
                    ▼                       ▼
            OrderConsumer          Kafka Topic: orders.DLT
                    │
            ┌───────┴────────┐
            │                │
            ▼                ▼
      (duplicate)        (new event)
            │                │
            ▼                ▼
         skip         PostgreSQL
                    processed_events
```

---

## Features

- **Idempotent consumer** — each `orderId` is processed exactly once; duplicates are detected via a PostgreSQL unique constraint and silently skipped
- **Dead letter topic (DLT)** — messages that fail all retry attempts are automatically routed to `orders.DLT` for inspection and replay
- **Retry with backoff** — Spring Kafka `DefaultErrorHandler` retries failed messages 3 times with 1-second intervals before sending to DLT
- **Kafka topic auto-provisioning** — `orders` and `orders.DLT` topics are created on startup via `NewTopic` beans
- **Monitoring endpoint** — `GET /api/monitor/status` reports app health and total processed event count
- **Spring Actuator** — `/actuator/health`, `/actuator/metrics` exposed out of the box

---

## Getting Started

### Prerequisites

- Docker and Docker Compose

### Run

```bash
docker-compose up --build
```

This starts PostgreSQL, Zookeeper, Kafka, and the Spring Boot app. On first boot, Flyway creates the `processed_events` table automatically.

---

## API

### Publish an order event

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-001",
    "product": "Laptop",
    "amount": 1299.99
  }'
```

Response:
```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "published"
}
```

### Check pipeline status

```bash
curl http://localhost:8080/api/monitor/status
```

Response:
```json
{
  "status": "UP",
  "processedEvents": 42,
  "timestamp": "2024-06-01T10:30:00Z"
}
```

### Test DLT routing

Send an order with `"product": "FAIL"` — the consumer throws a `RuntimeException`, Spring Kafka retries 3 times (3 seconds total), then routes the message to `orders.DLT`.

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-002", "product": "FAIL", "amount": 0}'
```

---

## How Dead Letter Topics Work

1. `OrderConsumer` throws an exception during processing.
2. `DefaultErrorHandler` catches it and retries up to 3 times with a 1-second `FixedBackOff`.
3. After all retries are exhausted, `DeadLetterPublishingRecoverer` publishes the original message to `orders.DLT`.
4. The DLT message retains the original headers plus additional headers (`kafka_dlt-exception-*`) describing the failure.
5. A separate consumer or operations team can inspect and replay messages from the DLT after the root cause is fixed.

---

## Project Structure

```
kafka-event-pipeline/
├── src/main/java/com/pipeline/
│   ├── KafkaEventPipelineApplication.java
│   ├── config/
│   │   └── KafkaConfig.java          # Error handler, DLT recoverer, topic beans
│   ├── producer/
│   │   ├── OrderEvent.java           # Event POJO (orderId, customerId, product, amount, timestamp)
│   │   ├── OrderRequest.java         # REST request DTO
│   │   ├── OrderProducer.java        # KafkaTemplate wrapper
│   │   └── ProducerController.java   # POST /api/orders
│   ├── consumer/
│   │   ├── OrderConsumer.java        # @KafkaListener with idempotency check
│   │   ├── ProcessedEvent.java       # JPA entity (idempotency store)
│   │   └── ProcessedEventRepository.java
│   └── monitoring/
│       └── KafkaMonitorController.java  # GET /api/monitor/status
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/
│       └── V1__create_processed_events.sql
├── Dockerfile
├── docker-compose.yml
├── build.gradle
└── settings.gradle
```

---

## License

[MIT](LICENSE)
