# Outbox + Inbox

[English](outbox-inbox.md) | [한국어](outbox-inbox.ko.md)

Use for critical state changes. The producer commits its domain change and Outbox row together; the consumer persists before handling.

## Configuration

```yaml
eventdock:
  producer.mode: outbox
  consumer.mode: inbox
  outbox.enabled: true
  inbox:
    enabled: true
    consumer-id: billing-service
    topics: order.created
```

## Producer

Inject the mode-neutral `EventWriter`. In `OUTBOX` mode, `write` appends the event in the current database transaction.

```java
@Service
class OrderService {
  private final OrderRepository orders;
  private final EventWriter events;

  OrderService(OrderRepository orders, EventWriter events) {
    this.orders = orders;
    this.events = events;
  }

  @Transactional
  void create(long orderId, long memberId) {
    orders.save(new Order(orderId, memberId));
    events.write(new EventEnvelope<>(
        EventId.random(),
        "order.created",
        1,
        new AggregateRef("order", Long.toString(orderId), 1),
        Instant.now(),
        new OrderCreated(orderId, memberId),
        Map.of("producer", "order-service")));
  }
}
```

If the method rolls back, both the order and Outbox row roll back. The Outbox scheduler publishes after commit; do not call Kafka directly here.

## Consumer

```java
@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.created")
final class OrderCreatedHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, OrderCreated.class);
    billing.openInvoice(envelope.payload().orderId());
  }
}
```

Delivery can repeat, but `(consumer-id, event-id)` is durably deduplicated. Verify atomic domain/Outbox rollback, `PUBLISHED` and `PROCESSED` states, duplicate delivery, and recovery after Kafka or consumer downtime.
