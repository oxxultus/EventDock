# LastDish Migration

[English](lastdish.md) | [한국어](lastdish.ko.md)

Migrate one event flow at a time. Do not switch an existing topic between the legacy JSON `EventMessage` format and the EventDock header format while old producers or consumers remain attached.

## Field mapping

| LastDish `EventMessage` | EventDock |
|---|---|
| `eventId` | `EventId.value` |
| `eventType` | `EventEnvelope.type` |
| `aggregateType` | `AggregateRef.type` |
| `aggregateId` | `AggregateRef.id` as string |
| `aggregateVersion` | `AggregateRef.version` |
| `schemaVersion` | `EventEnvelope.schemaVersion` |
| `payload` | payload object encoded by `EventCodec` |
| `occurredAt` | `EventEnvelope.occurredAt` |

## Staged replacement

1. Add EventDock `0.1.0` from Maven Central and apply the starter to one service.
2. Apply the EventDock schema and disable the legacy Outbox/Inbox auto-configurations only in that service.
3. Select a low-risk event and use a new topic during the transition.
4. Replace its writer with `OutboxWriter.append(EventEnvelope<?>)` inside the existing `@Transactional` use case.
5. Register an `InboxHandlerRegistry` adapter for the existing message handler.
6. Configure Kafka byte-array serializers and the service-specific `consumer-id` and topics.
7. Verify domain rollback, Outbox retry, duplicate delivery, stale aggregate version, and service restart recovery.
8. Repeat per event, then remove LastDish `event-common`, `outbox`, and `inbox` module dependencies.

The legacy tables are not reused because their columns and ownership differ. Keep both schemas during migration, drain legacy pending records, then archive the legacy tables after an agreed retention period.

## Readiness boundary

EventDock now covers the shared library responsibilities required by LastDish: serialization, transactional append, PostgreSQL persistence, Kafka publication, durable ingestion, idempotent handling, ordering, retries, lock recovery, scheduling, and Spring Boot wiring. Service-specific event classes and handler registration remain application code by design.
