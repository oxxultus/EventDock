# Changelog

[English](CHANGELOG.md) | [한국어](CHANGELOG.ko.md)

All notable changes to EventDock will be documented in this file.

The format follows Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Added

- Initial multi-module architecture and framework-independent event contracts.
- English and Korean system design documents with architecture, processing, and lifecycle diagrams.
- Framework-independent outbox and inbox processors with retry, exhaustion, idempotency, and latest-wins ordering policies.
- A unit-of-work port that keeps transaction control outside core modules.
