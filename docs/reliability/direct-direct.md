# Direct + Direct

[English](direct-direct.md) | [한국어](direct-direct.ko.md)

Use only for non-critical, rebuildable, or naturally idempotent work where minimum persistence overhead matters.

## Configuration

```yaml
eventdock:
  producer.mode: direct
  consumer.mode: direct
  outbox.enabled: false
  inbox:
    enabled: true
    consumer-id: analytics-service
    topics: page.viewed
```

`inbox.enabled` enables the direct listener; it does not create Inbox rows.

## Producer

```java
@Service
class AnalyticsProducer {
  private final EventWriter events;

  AnalyticsProducer(EventWriter events) {
    this.events = events;
  }

  void pageViewed(long memberId, String path) {
    events.write(new EventEnvelope<>(
        EventId.random(), "page.viewed", 1,
        new AggregateRef("member", Long.toString(memberId), 0),
        Instant.now(), new PageViewed(memberId, path),
        Map.of("producer", "web-service")));
  }
}
```

This example has no domain transaction. `write` publishes immediately and throws on publication failure; EventDock stores no retry state.

## Consumer

Register a direct handler:

```java
@Bean
EventHandlerRegistry directHandlers(EventCodec codec, AnalyticsService analytics) {
  EventHandler handler = event -> {
    var envelope = codec.decode(event, PageViewed.class);
    analytics.record(envelope.payload());
  };
  return (consumerId, eventType) ->
      consumerId.equals("analytics-service") && eventType.equals("page.viewed") ? handler : null;
}
```

Neither side persists EventDock state. Publication can be lost, rollback can leave a published event, and redelivery can repeat handling. Verify no Outbox/Inbox rows, immediate error propagation, harmless duplication, and that lost events are acceptable or rebuildable.
