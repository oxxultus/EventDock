# PostgreSQL Storage

[English](postgresql.md) | [한국어](postgresql.ko.md)

`eventdock-storage-postgresql` implements the EventDock outbox, inbox, aggregate-version, and unit-of-work ports with JDBC. It has no Spring or ORM dependency.

## Setup

1. Add the `eventdock-storage-postgresql` dependency.
2. Apply `META-INF/eventdock/postgresql/V1__eventdock_schema.sql` with the application's migration tool.
3. Create `PostgresqlStorage` with a `DataSource`.
4. Pass its repositories and unit of work to the core processors.

The migration creates the `eventdock` schema. It includes partial indexes for claiming work and deleting completed records.

## Processing guarantees

- Outbox and inbox claims use `FOR UPDATE SKIP LOCKED` through an atomic update-and-return query.
- Expired processing locks can be reclaimed.
- Inbox uniqueness is scoped by `(consumer_id, event_id)`.
- Aggregate versions are locked per consumer and aggregate inside a unit of work.
- Metadata is encoded deterministically without requiring a JSON library.

## Transactions

Use `PostgresqlUnitOfWork` when an EventDock operation alone owns the transaction. To commit domain changes and EventDock records atomically, provide a transaction-aware `DataSource` and let the host framework own the transaction. The Spring Boot adapter will supply this integration; core and PostgreSQL modules remain framework-independent.

## Verification

Repository integration tests use PostgreSQL 17 through Testcontainers. They are skipped when Docker is unavailable.
