# EventDock

[English](docs/readme/README.en.md) | [한국어](docs/readme/README.ko.md)

Framework-independent event, transactional outbox, and idempotent inbox building blocks.

## Quick start

Add `io.github.oxxultus:eventdock-spring-boot-starter:0.3.0` from Maven Central, configure PostgreSQL and Kafka, then write an `EventEnvelope` through `EventWriter`. Producer and consumer reliability modes are independently configurable.

See the [quick start](docs/getting-started/quick-start.md) or [한국어 빠른 시작](docs/getting-started/quick-start.ko.md).

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

- Start: [Quick start](docs/getting-started/quick-start.md)
- Build: [Consuming events](docs/guides/consuming.md), [Multiple consumers](docs/guides/multiple-consumers.md)
- Understand: [Reliability](docs/concepts/reliability.md), [Consumer identity and idempotency](docs/concepts/consumer-identity.md)
- Configure: [Spring Boot](docs/reference/spring-boot.md), [Kafka](docs/reference/kafka.md), [PostgreSQL](docs/reference/postgresql.md)
- Operate: [Runbook](docs/operations/runbook.md), [LastDish migration](docs/migration/lastdish.md)
- Develop: [Architecture](docs/development/architecture.md), [System design](docs/development/system-design.md), [Maven Central release](docs/development/maven-central.md)
- Project: [Contribution conventions](CONTRIBUTING.md), [Changelog](docs/project/CHANGELOG.md)
