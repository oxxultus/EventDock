# Architecture

## Dependency rule

Core modules contain policy and ports. Technology modules implement those ports. Framework modules only assemble and manage implementations.

```text
eventdock-spring-boot-starter
          |
eventdock-spring-boot-autoconfigure
          |
eventdock-storage-postgresql + eventdock-transport-kafka
          |
eventdock-outbox + eventdock-inbox
          |
eventdock-core
```

## Extension rules

- New database: add `eventdock-storage-<database>`.
- New broker: add `eventdock-transport-<broker>`.
- New serialization format: add `eventdock-codec-<format>`.
- New framework: add its integration or starter module.
- Change core only when the event contract or processing policy changes.

Outbox and inbox share the event envelope but remain independently usable features.
