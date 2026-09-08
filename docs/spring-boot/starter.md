# Spring Boot Starter

[English](starter.md) | [한국어](starter.ko.md)

Add one dependency:

```gradle
implementation 'io.github.oxxultus:eventdock-spring-boot-starter:0.1.0-SNAPSHOT'
```

For local development, run `./gradlew publishToMavenLocal` in EventDock and add `mavenLocal()` to the consuming build.

The starter uses the application's `DataSource`, `PlatformTransactionManager`, `ObjectMapper`, and `KafkaTemplate`. It configures PostgreSQL repositories, a transaction-aware unit of work, JSON codec, Kafka publisher, inbox receiver, and scheduled processors. Every bean can be replaced by declaring an application bean of the same contract.

```yaml
eventdock:
  initialize-schema: true
  publish-timeout: 10s
  retry:
    max-attempts: 5
    initial-delay: 1s
    max-delay: 1m
    lock-timeout: 1m
  outbox:
    enabled: true
    batch-size: 100
    poll-interval: 1s
  inbox:
    enabled: true
    consumer-id: core-service
    topics: order.created,order.status-changed
    batch-size: 100
    poll-interval: 1s

```

Declare an `InboxHandlerRegistry` bean to enable inbox listening and processing. Append an `EventEnvelope` with `OutboxWriter` inside the same application transaction as the domain change. The transaction-aware data source makes both writes commit or roll back together.

The starter creates dedicated byte-array Kafka producer and consumer factories. Existing application `KafkaTemplate` and JSON listener factories remain unchanged.

Schema initialization defaults to enabled and executes idempotent DDL. Set `eventdock.initialize-schema=false` when Flyway or another deployment process applies `META-INF/eventdock/postgresql/V1__eventdock_schema.sql`.
