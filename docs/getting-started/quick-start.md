# Quick Start

[English](quick-start.md) | [한국어](quick-start.ko.md)

This guide uses the Spring Boot starter. For framework-independent integration, depend on the required core, inbox, outbox, storage, and transport modules and wire their ports directly.

## 1. Requirements

- Java 21
- Spring Boot 4.1
- PostgreSQL
- Kafka

Add Maven Central and the starter to the consuming application:

```gradle
repositories {
    mavenCentral()
}

dependencies {
    implementation 'io.github.oxxultus:eventdock-spring-boot-starter:0.4.0'
}
```

To test an unreleased source checkout, run `./gradlew publishToMavenLocal` in EventDock and temporarily add `mavenLocal()` before `mavenCentral()`.

## 2. Configure PostgreSQL and Kafka

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/app
    username: app
    password: app
  kafka:
    bootstrap-servers: localhost:9092

eventdock:
  initialize-schema: true
  outbox:
    enabled: true
    batch-size: 100
    poll-interval: 1s
  inbox:
    enabled: true
    consumer-id: billing-service
    topics: order.created
    batch-size: 100
    poll-interval: 1s
```

`initialize-schema: true` is convenient for local development. In production, apply `META-INF/eventdock/postgresql/V1__eventdock_schema.sql` through Flyway or the deployment pipeline and set it to `false`.

The event type is the default Kafka topic. Create each configured topic before starting the service when Kafka topic auto-creation is disabled.

This is the legacy single-listener form. Applications with multiple listeners should use [multiple consumer bindings](../guides/multiple-consumers.md), which separates Kafka `group-id` from EventDock `consumer-id` and supports event-type routes.

## 3. Publish with the domain transaction

Create the domain data and append its event in one Spring transaction. A rollback then removes both changes.

```java
import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventEnvelope;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.EventWriter;
import java.time.Instant;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

record OrderCreated(long orderId, long memberId) {}

@Service
class OrderService {
  private final OrderRepository orders;
  private final EventWriter events;

  OrderService(OrderRepository orders, EventWriter events) {
    this.orders = orders;
    this.events = events;
  }

  @Transactional
  void create(long orderId, long memberId) {
    orders.save(new Order(orderId, memberId));

    events.write(new EventEnvelope<>(
        EventId.random(),
        "order.created",
        1,
        new AggregateRef("order", Long.toString(orderId), 1),
        Instant.now(),
        new OrderCreated(orderId, memberId),
        Map.of("producer", "order-service")));
  }
}
```

With the default `OUTBOX` producer mode, EventDock stores the event in the Outbox table, publishes it asynchronously, and records completion or retry state. Changing `eventdock.producer.mode` to `DIRECT` keeps this application code unchanged.

## 4. Register a handler

Declare an `EventHandler` bean annotated with `@EventDockHandler`. EventDock registers it by consumer ID and event type.

```java
import io.github.oxxultus.eventdock.core.EventCodec;
import io.github.oxxultus.eventdock.autoconfigure.EventDockHandler;
import io.github.oxxultus.eventdock.core.EventHandler;
import io.github.oxxultus.eventdock.core.SerializedEvent;

@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.created")
final class OrderCreatedHandler implements EventHandler {
  private final EventCodec codec;
  private final BillingService billing;

  OrderCreatedHandler(EventCodec codec, BillingService billing) {
    this.codec = codec;
    this.billing = billing;
  }

  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, OrderCreated.class);
    billing.openInvoice(envelope.payload().orderId());
  }
}
```

Inbox processing and the handler's database changes run in one transaction. The same `(consumer-id, event-id)` is handled once. Missing handlers and application failures follow the configured retry policy and eventually remain as `FAILED` rows.

## 5. Verify locally

1. Start PostgreSQL and Kafka.
2. Start the producer and consumer applications.
3. Execute one domain command and confirm a row appears in `eventdock.outbox_events`.
4. Confirm the Outbox row becomes `PUBLISHED` and the consumer creates `eventdock.inbox_events`.
5. Confirm the Inbox row becomes `PROCESSED` and the expected domain change exists.
6. Stop Kafka temporarily and confirm retry state advances without losing the Outbox row.

Before production rollout, review the [operations runbook](../operations/runbook.md). For a LastDish replacement, follow the [migration guide](../migration/lastdish.md); the legacy full-JSON Kafka envelope is not wire-compatible with EventDock.

## More configuration

- [Four reliability-mode implementation guides](../concepts/reliability.md)
- [Spring Boot starter](../reference/spring-boot.md)
- [Multiple consumer bindings](../guides/multiple-consumers.md)
- [Automatic handler registration](../guides/consuming.md)
- [PostgreSQL storage](../reference/postgresql.md)
- [Kafka transport](../reference/kafka.md)
- [Architecture](../development/architecture.md)
