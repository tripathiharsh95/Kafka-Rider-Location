# Rider Location Tracker — Spring Cloud Stream + Kafka

A real-time rider location/status streaming system built using **Spring Cloud Stream** with the **Kafka binder**, demonstrating event-driven communication between a producer and consumer service using functional programming style (`Supplier`/`Consumer` beans) instead of traditional `@KafkaListener` annotations.

---

## Overview

This project simulates a ride-hailing style event stream — a **Producer service** periodically generates random rider status updates (e.g., "ride started", "ride completed") and publishes them to a Kafka topic. A **Consumer service** listens to that topic and processes incoming rider location events in real time.

The project also demonstrates **Kafka consumer group load balancing** by running multiple consumer instances under the same group ID and observing how Kafka distributes topic partitions across them.

---

## Architecture

```
   Producer Service                    Consumer Service
 (Supplier<Message<String>>)                  │
          │                                    │
          ▼                                    ▼
   ┌─────────────────────────────────────────────────┐
   │              Kafka Topic: my-topic               │
   │              (multiple partitions)                │
   └─────────────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                              ▼
   Consumer Instance 1           Consumer Instance 2
   (group: rider-location-grp)   (group: rider-location-grp)
   → handles a subset of              → handles a subset of
     partitions                          partitions
```

---

## Modules

| Module | Role | Port |
|---|---|---|
| **producer** | Publishes random rider status events to Kafka using a `Supplier` bean | 8080 |
| **consumer** | Consumes rider location events using a `Consumer` bean | 8081 (and a second instance on 9091 for load-balancing demo) |

---

## Tech Stack

- Java 21
- Spring Boot 3.5
- Spring Cloud Stream (functional style: `Supplier` / `Consumer` beans)
- Spring Cloud Stream Kafka Binder
- Apache Kafka
- Maven

---

## How It Works

### Producer

A `Supplier<Message<String>>` bean is automatically polled by Spring Cloud Stream (default every second). On each invocation it:
- Generates a random rider ID and status (`ride started` / `ride completed`)
- Builds a Kafka message with the rider ID as the **partition key**, ensuring all events for the same rider land in the same partition (preserving per-rider ordering)
- Publishes the message to `my-topic`

### Consumer

A `Consumer<RiderLocation>` bean is automatically invoked whenever a new message arrives on `my-topic`. It deserializes the incoming JSON payload into a `RiderLocation` object and logs the received update.

### Binding Configuration (`application.yml`)

```yaml
spring:
  cloud:
    function:
      definition: sendRiderStatus     # producer
      # definition: processRiderLocation   # consumer
    stream:
      bindings:
        sendRiderStatus-out-0:
          destination: my-topic
        processRiderLocation-in-0:
          destination: my-topic
          group: rider-location-grp
          content-type: application/json
  kafka:
    bootstrap-servers: localhost:9092
```

---

## Load Balancing Demo

Kafka consumer groups allow multiple instances of the same service to share the work of processing a topic's partitions — each partition is processed by exactly one consumer within a group at a time.

To observe this:

1. Increase the topic's partition count (recommended: 4+, so it can be split across multiple consumers):
   ```bash
   kafka-topics.bat --alter --topic my-topic --partitions 4 --bootstrap-server localhost:9092
   ```
2. Run two instances of the consumer service (e.g., one on port 8080, a copy on port 9091), both configured with the **same** `group.id` (`rider-location-grp`).
3. Start the producer to generate a stream of events.
4. Both consumer instances will receive a share of the messages, since Kafka distributes the topic's partitions between them.
5. Verify partition assignment:
   ```bash
   kafka-consumer-groups.bat --describe --group rider-location-grp --bootstrap-server localhost:9092
   ```

---

## Running Locally

### Prerequisites
- Java 21
- Maven
- Apache Kafka running locally (via manual install or Docker — see below)

### Start Kafka (Docker Compose — recommended)

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

```bash
docker-compose up -d
```

### Run the services

```bash
# Terminal 1 - Producer
cd producer
mvn spring-boot:run

# Terminal 2 - Consumer
cd consumer
mvn spring-boot:run
```

---

## Error Handling (In Progress)

Kafka message deserialization failures (e.g., a malformed or unexpected payload) can otherwise crash the consumer's listener thread. This project is being extended to include:

- **`ErrorHandlingDeserializer`** — wraps the actual deserializer so failures are caught gracefully instead of crashing the consumer
- **Retry with backoff** — configurable retry attempts before giving up on a message
- **Dead Letter Topic (DLT)** — failed messages are routed to a separate topic instead of blocking the partition indefinitely

---

## Learning Outcomes

This project was built to practice:
- Event-driven communication using Spring Cloud Stream's functional programming model
- Kafka producer/consumer fundamentals — topics, partitions, keys, consumer groups
- Consumer group load balancing and partition assignment
- Handling deserialization and processing failures in a streaming pipeline

---

## Author

**Harsh Tripathi**
GitHub: [github.com/tripathiharsh95](https://github.com/tripathiharsh95)
LinkedIn: [linkedin.com/in/tripathiharsh95](https://linkedin.com/in/tripathiharsh95)
