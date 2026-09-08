# EventDock

[English](docs/readme/README.en.md) | [한국어](docs/readme/README.ko.md)

Framework-independent event, transactional outbox, and idempotent inbox building blocks.

## Quick start

Add `io.github.oxxultus:eventdock-spring-boot-starter:0.1.0` from Maven Central, configure PostgreSQL and Kafka, then append an `EventEnvelope` through `OutboxWriter` inside the same transaction as the domain change.

See the complete [usage guide](docs/getting-started/usage.md) or [한국어 사용 가이드](docs/getting-started/usage.ko.md).

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

- [Usage guide](docs/getting-started/usage.md)
- [Maven Central release](docs/releasing/maven-central.md)
- [System design](docs/design/system-design.md)
- [Architecture](docs/architecture/architecture.md)
- [PostgreSQL storage](docs/storage/postgresql.md)
- [Kafka transport](docs/transport/kafka.md)
- [Spring Boot starter](docs/spring-boot/starter.md)
- [Operations runbook](docs/operations/runbook.md)
- [LastDish migration](docs/migration/lastdish.md)
- [Contribution conventions](CONTRIBUTING.md)
- [Changelog](docs/project/CHANGELOG.md)
