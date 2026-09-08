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

Call `EventWriter.write(...)` in the domain `@Transactional` method, using the [producer example](../getting-started/usage.md#3-publish-with-the-domain-transaction).

```java
@Bean
InboxHandlerRegistry inboxHandlers(EventCodec codec, BillingService billing) {
  InboxHandler handler = event -> {
    var envelope = codec.decode(event, OrderCreated.class);
    billing.openInvoice(envelope.payload().orderId());
  };
  return (consumerId, eventType) ->
      consumerId.equals("billing-service") && eventType.equals("order.created") ? handler : null;
}
```

Delivery can repeat, but `(consumer-id, event-id)` is durably deduplicated. Verify atomic domain/Outbox rollback, `PUBLISHED` and `PROCESSED` states, duplicate delivery, and recovery after Kafka or consumer downtime.
