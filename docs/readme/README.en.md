# EventDock

[English](README.en.md) | [한국어](README.ko.md)

Framework-independent event, transactional outbox, and idempotent inbox building blocks.

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

Framework-independent outbox and inbox processing is implemented. PostgreSQL storage, Kafka mapping, auto-configuration, migrations, and production integration tests remain under active development.

## Documentation

- [System design](../design/system-design.md)
- [Architecture](../architecture/architecture.md)
- [Contribution conventions](../../CONTRIBUTING.md)
- [Changelog](../project/CHANGELOG.md)
