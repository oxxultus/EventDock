# Multiple consumer bindings

[English](multiple-consumers.md) | [한국어](multiple-consumers.ko.md)

Use `eventdock.consumers` when one application owns multiple Kafka listeners. Each binding independently selects its Kafka group, topics, and `INBOX` or `DIRECT` processing mode.

```yaml
eventdock:
  inbox:
    batch-size: 100
    poll-interval: 1s
  consumers:
    - id: point-order-picked-up
      mode: inbox
      topics: ORDER_PICKED_UP
      kafka:
        group-id: core-point-order-picked-up

    - id: cart-dish-state
      mode: direct
      topics: DISH_STATE_CHANGED
      kafka:
        group-id: core-cart-dish-state

    - id: order-member-events
      mode: inbox
      topics: MEMBER_CREATED,MEMBER_UPDATED,MEMBER_DELETED
      kafka:
        group-id: core-order-member-events
      routes:
        - event-type: MEMBER_CREATED
          consumer-id: core-order-member-created
        - event-type: MEMBER_UPDATED
          consumer-id: core-order-member-updated
        - event-type: MEMBER_DELETED
          consumer-id: core-order-member-deleted
```

## Identity rules

- `id`: binding identity and default EventDock consumer ID.
- `kafka.group-id`: Kafka partition assignment and offset identity; defaults to `id` when omitted.
- `topics`: topics owned by this listener container.
- `mode`: processing path for this binding.
- `routes[].event-type`: matches the EventDock event type header, not necessarily the Kafka topic.
- `routes[].consumer-id`: overrides the default consumer ID for Inbox deduplication, retry claims, ordering, and handler lookup.

Declare the resolved consumer ID in `@EventDockHandler`. EventDock collects handler beans automatically:

```java
@Component
@EventDockHandler(consumerId = "core-order-member-created", eventType = "MEMBER_CREATED")
final class MemberCreatedHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    members.created(codec.decode(event, MemberCreated.class).payload());
  }
}
```

Add a handler class for each remaining event type. See [automatic handler registration](consuming.md) for complete rules.

Do not combine `eventdock.consumers` with `eventdock.inbox.enabled=true`. The legacy single-listener configuration remains supported for compatibility. Setting `spring.kafka.listener.auto-startup=false` also prevents dynamic EventDock containers from starting.
