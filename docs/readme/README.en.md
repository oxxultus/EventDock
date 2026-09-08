# EventDock

[English](README.en.md) | [한국어](README.ko.md)

Framework-independent event, transactional outbox, and idempotent inbox building blocks.

## Quick start

Use the [quick start](../getting-started/quick-start.md) to install the starter, configure PostgreSQL and Kafka, publish an event transactionally, and register a handler.

## Modules

- `eventdock-core`: event envelope and transport-neutral contracts
- `eventdock-outbox`: outbox processing rules and ports
- `eventdock-inbox`: inbox processing, idempotency, and ordering ports
- `eventdock-storage-postgresql`: PostgreSQL persistence adapters
- `eventdock-transport-kafka`: Kafka transport adapter
- `eventdock-spring-boot-autoconfigure`: conditional Spring Boot wiring
- `eventdock-spring-boot-starter`: recommended Spring Boot dependency bundle

Dependencies point inward: framework and technology adapters depend on core modules; core modules never depend on Spring, Kafka, Jackson, JPA, or a database.

## Initial support target

- Java 21
- PostgreSQL
- Kafka
- Spring Boot 4.1

Framework-independent processing, PostgreSQL storage, Kafka transport, and Spring Boot starter integration are implemented. Production hardening remains under active development.

## Documentation

- Start: [Quick start](../getting-started/quick-start.md)
- Build: [Consuming events](../guides/consuming.md), [Multiple consumers](../guides/multiple-consumers.md)
- Understand: [Reliability](../concepts/reliability.md), [Consumer identity and idempotency](../concepts/consumer-identity.md)
- Configure: [Spring Boot](../reference/spring-boot.md), [Kafka](../reference/kafka.md), [PostgreSQL](../reference/postgresql.md)
- Operate: [Runbook](../operations/runbook.md), [LastDish migration](../migration/lastdish.md)
- Develop: [Architecture](../development/architecture.md), [System design](../development/system-design.md), [Maven Central release](../development/maven-central.md)
- Project: [Contribution conventions](../../CONTRIBUTING.md), [Changelog](../project/CHANGELOG.md)
