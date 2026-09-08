# Outbox + Direct

[English](outbox-direct.md) | [한국어](outbox-direct.ko.md)

Use when publication must survive producer failure but the latency-sensitive consumer is already idempotent.

## Configuration

```yaml
eventdock:
  producer.mode: outbox
  consumer.mode: direct
  outbox.enabled: true
  inbox:
    enabled: true
    consumer-id: notification-service
    topics: order.created
```

## Producer

The producer implementation is identical across consumer modes. In `OUTBOX` mode, the selected `EventWriter` persists inside the transaction.

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
        EventId.random(), "order.created", 1,
        new AggregateRef("order", Long.toString(orderId), 1),
        Instant.now(), new OrderCreated(orderId, memberId),
        Map.of("producer", "order-service")));
  }
}
```

Rollback removes both writes; the scheduler publishes later. Consumer `DIRECT` does not change producer code.

## Consumer

Register a transport-neutral direct handler:

```java
@Component
@EventDockHandler(consumerId = "notification-service", eventType = "order.created")
final class OrderCreatedNotificationHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, OrderCreated.class);
    notifications.sendCreated(envelope.payload().orderId());
  }
}
```

The producer retries from Outbox. No Inbox row or durable consumer deduplication exists; make the handler naturally idempotent or use a business idempotency key. Verify producer rollback/recovery, duplicate redelivery, and absence of Inbox rows.
