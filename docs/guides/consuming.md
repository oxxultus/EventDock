# Consuming Events with Spring Boot

[English](consuming.md) | [한국어](consuming.ko.md)

The Spring Boot Starter automatically collects `EventHandler` beans annotated with `@EventDockHandler`. Applications do not need to create Kafka listeners or an `InboxHandlerRegistry`.

## Minimal implementation

```java
@Component
@EventDockHandler(
    consumerId = "billing-service",
    eventType = "order.created"
)
final class OrderCreatedHandler implements EventHandler {
  private final EventCodec codec;
  private final BillingService billing;

  OrderCreatedHandler(EventCodec codec, BillingService billing) {
    this.codec = codec;
    this.billing = billing;
  }

  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, OrderCreated.class);
    billing.openInvoice(envelope.payload().orderId());
  }
}
```

```yaml
eventdock:
  consumers:
    - id: billing-events
      mode: INBOX
      topics: [order-events]
      kafka:
        group-id: billing-service
      routes:
        - event-type: order.created
          consumer-id: billing-service
```

Without `routes`, the binding `id` becomes the consumer ID. The annotation `consumerId` must exactly match the resulting consumer ID. `eventType` must exactly match the case-sensitive EventDock event type header; it is not the Kafka topic name.

Resolution follows `Kafka group → binding → eventType route → consumerId → annotated handler`. Kafka `group-id` defines load-sharing scope. EventDock `consumerId` selects the handler and forms the Inbox deduplication key `(consumer-id, event-id)`. EventDock does not infer either value.

## Multiple events

Prefer one handler class per event type.

```java
@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.created")
final class OrderCreatedHandler implements EventHandler { /* ... */ }

@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.cancelled")
final class OrderCancelledHandler implements EventHandler { /* ... */ }
```

Repeat the annotation when one handler intentionally handles multiple event types.

```java
@Component
@EventDockHandler(consumerId = "audit-service", eventType = "order.created")
@EventDockHandler(consumerId = "audit-service", eventType = "order.cancelled")
final class OrderAuditHandler implements EventHandler { /* ... */ }
```

## INBOX and DIRECT

Both consumer modes use the same annotation API.

- `INBOX`: EventDock persists receipt, performs durable deduplication and retries, and runs the handler transactionally.
- `DIRECT`: EventDock invokes the handler immediately after Kafka receipt. There is no Inbox persistence, durable deduplication, or durable retry.

`eventdock.consumers[].mode`, not the handler, selects the mode. A handler can remain unchanged when the mode changes.

## Inbox processing policy

Omitting `policy` selects `IDEMPOTENT`.

```java
@EventDockHandler(
    consumerId = "order-member",
    eventType = "MEMBER_UPDATED",
    policy = OrderingPolicy.LATEST_WINS
)
final class MemberUpdatedHandler implements EventHandler { /* ... */ }
```

- `IDEMPOTENT`: removes duplicate `(consumerId, eventId)` deliveries and processes every distinct event.
- `LATEST_WINS`: also skips events at or below the last processed version for the same aggregate. Use it when only the newest synchronized state matters.

`LATEST_WINS` requires stable `aggregate.type` and `aggregate.id` values plus a monotonically increasing `aggregate.version` in the envelope. Event types that share one aggregate version cursor must use the same `consumerId` and policy. `DIRECT` stores no Inbox or aggregate version, so it ignores `policy` and invokes the handler immediately.

## Startup validation and legacy API

Two handlers mapped to the same `(consumerId, eventType)` fail application startup. Blank annotation attributes are rejected. An unmapped message follows the processor's existing handler-not-found failure policy.

Applications that explicitly provide an `InboxHandlerRegistry` or `EventHandlerRegistry` remain supported. An explicit registry makes the automatic registry back off; annotation handlers and an explicit registry are not merged automatically.

Declare `@EventDockHandler` on the Spring bean implementation class. With an `@Bean` factory method, the returned implementation class must carry the annotation.
