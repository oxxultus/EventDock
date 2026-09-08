# Operations Runbook

[English](runbook.md) | [한국어](runbook.ko.md)

## Delivery model

EventDock provides at-least-once delivery. A broker acknowledgement followed by a database commit failure can publish a duplicate. Inbox uniqueness and handlers designed for repeated execution are mandatory. EventDock does not claim exactly-once delivery across PostgreSQL and Kafka.

Kafka offsets are committed only after the listener returns. The listener returns after the record is inserted into Inbox or recognized as a duplicate. Business handling occurs later in the Inbox scheduler and is committed with its status/version update.

## Alerts

Alert on increases in these counters and health details:

- `eventdock.outbox.events{outcome="failed"}`
- `eventdock.inbox.events{outcome="failed"}`
- repeated `retry_scheduled` growth
- `outboxPending`, `inboxPending`, `outboxFailed`, or `inboxFailed`

Backlog alone is not an outage. Alert when it grows continuously beyond the service's normal processing window.

## Failed records and DLQ

Rows become `FAILED` after `eventdock.retry.max-attempts`. Register exhaustion handlers to send a sanitized operational message to a DLQ or incident system. Never include payloads or secrets by default. Reprocessing is a manual, audited database or future administration API action; changing `FAILED` rows directly during an active incident risks duplicates.

## Restart and scaling

Multiple instances may run schedulers concurrently. Claims use `FOR UPDATE SKIP LOCKED`. A terminated worker leaves a `PROCESSING` row that becomes claimable after `eventdock.retry.lock-timeout`. Set the lock timeout above the expected maximum handler or publish duration.

During shutdown, stop traffic and Kafka listener containers before the application context closes. In-flight database transactions roll back; broker sends already acknowledged may be redelivered and are handled by Inbox idempotency.

## Retention

Cleanup deletes only `PUBLISHED`, `PROCESSED`, and `SKIPPED` rows older than their configured retention. It never deletes pending, processing, or failed records. Choose retention longer than the incident investigation and replay window, and back up tables according to the application's data policy.

## Release gate

Before each production rollout:

1. Run `./gradlew clean build` with Docker available so PostgreSQL, Kafka, transaction, concurrency, and cleanup integration tests execute.
2. Verify the generated POM, sources JAR, Javadoc JAR, and signatures.
3. Deploy one producer and consumer flow on isolated topics.
4. Exercise duplicate delivery, broker outage, database outage, process termination, and rollback.
5. Confirm dashboards, alerts, DLQ callback, retention, and rollback procedures.
