# EventDock

[English](README.en.md) | [한국어](README.ko.md)

Framework-independent event, transactional outbox, and idempotent inbox building blocks.

## Quick start

Use the [usage guide](../getting-started/usage.md) to install the starter, configure PostgreSQL and Kafka, publish an event transactionally, and register an Inbox handler.

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

- [Usage guide](../getting-started/usage.md)
- [Reliability modes](../reliability/modes.md)
  - [Outbox + Inbox](../reliability/outbox-inbox.md)
  - [Outbox + Direct](../reliability/outbox-direct.md)
  - [Direct + Inbox](../reliability/direct-inbox.md)
  - [Direct + Direct](../reliability/direct-direct.md)
- [Maven Central release](../releasing/maven-central.md)
- [System design](../design/system-design.md)
- [Architecture](../architecture/architecture.md)
- [PostgreSQL storage](../storage/postgresql.md)
- [Kafka transport](../transport/kafka.md)
- [Spring Boot starter](../spring-boot/starter.md)
- [Operations runbook](../operations/runbook.md)
- [LastDish migration](../migration/lastdish.md)
- [Contribution conventions](../../CONTRIBUTING.md)
- [Changelog](../project/CHANGELOG.md)
