# Transactional Outbox Pattern

Companion code for the tutorial [The Transactional Outbox Pattern in Spring Boot](https://umurinan.com/pages/tutorials/transactional-outbox.html).

The transactional outbox pattern solves a specific problem: you need to save something to the database *and* publish an event to Kafka, and you need both to succeed or neither to succeed. You can't do that with two separate operations - one might fail after the other succeeds.

The solution: write both the business row and an "outbox" row in the same database transaction. Then a separate relay process reads those outbox rows and publishes them to Kafka.

---

## How it works

When an order is placed, `OrderService` writes two rows in a single transaction:

```
orders table           outbox table
-----------            ------------
id: 42                 id: 1
userId: user-123       topic: orders
total: 59.99           aggregateId: 42
status: PLACED         payload: {"event":"OrderPlaced",...}
                       published: false
```

`OutboxRelay` runs every 500ms, picks up rows where `published = false`, sends them to Kafka, and marks them `published = true`.

If the relay crashes between sending and marking, it sends again on the next run. That's at-least-once delivery - consumers need to be idempotent.

---

## Project structure

```
src/main/java/com/umurinan/outbox/
├── entity/
│   ├── Order.java              # Business entity
│   └── OutboxMessage.java      # Outbox table row
├── repository/
│   ├── OrderRepository.java
│   └── OutboxMessageRepository.java
├── service/
│   ├── OrderService.java       # Atomic write: Order + OutboxMessage
│   ├── PlaceOrderRequest.java
│   └── OrderResponse.java
├── relay/
│   └── OutboxRelay.java        # Polls outbox, publishes to Kafka
├── controller/
│   └── OrderController.java
├── config/
│   └── KafkaConfig.java
└── TransactionalOutboxApplication.java
```

---

## Running the tests

Unit tests only (no Docker required):

```bash
mvn test
```

Full suite including integration tests (requires Docker):

```bash
mvn verify
```

The integration tests use Testcontainers to spin up real PostgreSQL and Kafka instances. `OrderServiceIT` verifies that the order and outbox row are written atomically. `OutboxRelayIT` verifies end-to-end publishing - it places orders, triggers the relay manually, and consumes from a real Kafka topic to confirm the messages arrived.

---

## Running locally

Start Postgres and Kafka, then:

```bash
mvn spring-boot:run
```

Place an order:

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","total":49.99}'
```

The relay picks it up within 500ms and publishes to the `orders` Kafka topic.

---

## Tech stack

- Java 21
- Spring Boot 4.0.5
- Spring Kafka 4.0.4
- Spring Data JPA / Hibernate
- PostgreSQL
- Testcontainers 1.21.3
