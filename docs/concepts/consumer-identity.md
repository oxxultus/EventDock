# Consumer Identity and Idempotency

[English](consumer-identity.md) | [한국어](consumer-identity.ko.md)

Three identifiers have separate responsibilities.

| Value | Owner | Responsibility |
| --- | --- | --- |
| `group-id` | Kafka | delivery group and load sharing among instances |
| `consumerId` | EventDock | handler routing and Inbox idempotency scope |
| `eventId` | producer | individual event identity |

The Inbox unique key is `(consumerId, eventId)`. It does not include `group-id`.

## Deployment rules

- Multiple instances of one service use the same `group-id` and `consumerId`.
- Services that must each receive an event use different `group-id` and `consumerId` values.
- Independent consumers sharing one database must use different `consumerId` values.
- Different groups using the same `consumerId` and database can suppress one another as duplicates.
- With a database per service, each database applies Inbox idempotency independently.

```text
Kafka group-id → binding → eventType route → consumerId → Handler
                                              ↓
                                  Inbox (consumerId, eventId)
```

Listeners and instances in one group share partitions rather than each receiving every event. Independent consumers that must all receive an event need separate groups. See [multiple consumers](../guides/multiple-consumers.md) for configuration.
