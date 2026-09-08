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

Publish through `EventWriter` in the domain transaction. Register a transport-neutral direct handler:

```java
@Bean
EventHandlerRegistry directHandlers(EventCodec codec, NotificationService notifications) {
  EventHandler handler = event -> {
    var envelope = codec.decode(event, OrderCreated.class);
    notifications.sendCreated(envelope.payload().orderId());
  };
  return (consumerId, eventType) ->
      consumerId.equals("notification-service") && eventType.equals("order.created") ? handler : null;
}
```

The producer retries from Outbox. No Inbox row or durable consumer deduplication exists; make the handler naturally idempotent or use a business idempotency key. Verify producer rollback/recovery, duplicate redelivery, and absence of Inbox rows.
