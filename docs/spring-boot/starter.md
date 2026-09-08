# Spring Boot Starter

[English](starter.md) | [한국어](starter.ko.md)

Add one dependency:

```gradle
implementation 'io.github.oxxultus:eventdock-spring-boot-starter:0.2.0'
```

The artifact is available from Maven Central. For an unreleased source checkout, run `./gradlew publishToMavenLocal` in EventDock and temporarily add `mavenLocal()` before `mavenCentral()` in the consuming build.

The starter uses the application's `DataSource`, `PlatformTransactionManager`, `ObjectMapper`, and `KafkaTemplate`. It configures PostgreSQL repositories, a transaction-aware unit of work, JSON codec, Kafka publisher, inbox receiver, and scheduled processors. Every bean can be replaced by declaring an application bean of the same contract.

```yaml
eventdock:
  initialize-schema: true
  publish-timeout: 10s
  producer:
    mode: outbox
  consumer:
    mode: inbox
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
  cleanup:
    enabled: true
    interval: 1h
    outbox-retention: 7d
    inbox-retention: 30d
    batch-size: 1000

```

Declare an `InboxHandlerRegistry` bean to enable inbox listening and processing. Inject `EventWriter` to select Outbox or Direct publication through configuration. The default Outbox writer joins the domain transaction, so both changes commit or roll back together. See [reliability modes](../reliability/modes.md).

The `inbox.consumer-id` form above creates one legacy listener. For multiple groups, mixed modes, or event-type-specific Inbox identities, use [multiple consumer bindings](multi-consumers.md).

The starter creates dedicated byte-array Kafka producer and consumer factories. Existing application `KafkaTemplate` and JSON listener factories remain unchanged.

Schema initialization defaults to enabled and executes idempotent DDL. Set `eventdock.initialize-schema=false` when Flyway or another deployment process applies `META-INF/eventdock/postgresql/V1__eventdock_schema.sql`.

Invalid durations, batch sizes, retry ranges, or enabled Inbox settings fail application startup. If Micrometer is present, EventDock records outcome counters. If Spring Boot Health is present, `eventDockHealthIndicator` reports pending and failed Outbox/Inbox counts.

Declare custom `OutboxExhaustionHandler` and `InboxExhaustionHandler` beans to publish DLQ records or page operators. The persisted `FAILED` row remains the source of truth even if the operational callback fails.
