# Changelog

[English](CHANGELOG.md) | [한국어](CHANGELOG.ko.md)

All notable changes to EventDock will be documented in this file.

The format follows Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Changed

- Reorganized documentation into getting-started, guides, concepts, reference, operations, migration, development, and project sections.
- Consolidated reliability guidance and documented Kafka group, EventDock consumer identity, shared-database, and Inbox idempotency boundaries.

## [0.3.0] - 2026-09-08

### Added

- Multiple Kafka consumer bindings with independent group IDs, modes, topics, and event-type consumer routes.
- Complete English and Korean implementation guides for all four reliability-mode combinations.
- Automatic Spring Boot handler registration through repeatable `@EventDockHandler` annotations, with duplicate-route startup validation and explicit Registry compatibility.

## [0.2.0] - 2026-09-08

### Added

- Independent `OUTBOX`/`DIRECT` producer and `INBOX`/`DIRECT` consumer reliability modes.

## [0.1.0] - 2026-09-08

### Added

- Initial multi-module architecture and framework-independent event contracts.
- English and Korean system design documents with architecture, processing, and lifecycle diagrams.
- Framework-independent outbox and inbox processors with retry, exhaustion, idempotency, and latest-wins ordering policies.
- A unit-of-work port that keeps transaction control outside core modules.
- JDBC-based PostgreSQL outbox, inbox, aggregate-version, migration, and cleanup support.
- Kafka header-based wire mapping, publisher, and durable inbox receiver.
- Spring Boot auto-configuration, scheduled processors, JSON codec, and Maven-local publication.
- Production cleanup, validation, exhaustion hooks, Micrometer metrics, health details, and failure-path integration coverage.
- Tag-driven, signed Maven Central publication through the Central Publisher Portal.
