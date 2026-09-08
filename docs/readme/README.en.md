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

Framework-independent processing, PostgreSQL storage, Kafka transport, and Spring Boot starter integration are implemented. Production hardening remains under active development.

## Documentation

- [System design](../design/system-design.md)
- [Architecture](../architecture/architecture.md)
- [PostgreSQL storage](../storage/postgresql.md)
- [Kafka transport](../transport/kafka.md)
- [Spring Boot starter](../spring-boot/starter.md)
- [LastDish migration](../migration/lastdish.md)
- [Contribution conventions](../../CONTRIBUTING.md)
- [Changelog](../project/CHANGELOG.md)
