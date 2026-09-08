# Direct + Inbox

[English](direct-inbox.md) | [한국어](direct-inbox.ko.md)

Use for low-latency publication when durable consumer deduplication matters more than producer-side atomicity.

## Configuration

```yaml
eventdock:
  producer.mode: direct
  consumer.mode: inbox
  outbox.enabled: false
  inbox:
    enabled: true
    consumer-id: search-service
    topics: store.changed
```

## Producer

The same `EventWriter` API resolves to `DirectEventWriter` and publishes synchronously.

```java
@Service
class StoreService {
  private final StoreRepository stores;
  private final EventWriter events;

  StoreService(StoreRepository stores, EventWriter events) {
    this.stores = stores;
    this.events = events;
  }

  @Transactional
  void change(long storeId, long version) {
    stores.change(storeId);
    events.write(new EventEnvelope<>(
        EventId.random(), "store.changed", 1,
        new AggregateRef("store", Long.toString(storeId), version),
        Instant.now(), new StoreChanged(storeId),
        Map.of("producer", "store-service")));
  }
}
```

`write` returning means transport publication completed, not that the database transaction committed. A publish failure throws to the caller; a later rollback cannot retract an already-published event.

## Consumer

```java
@Bean
InboxHandlerRegistry inboxHandlers(EventCodec codec, SearchService search) {
  InboxHandler handler = event -> {
    var envelope = codec.decode(event, StoreChanged.class);
    search.reindex(envelope.payload().storeId());
  };
  return (consumerId, eventType) ->
      consumerId.equals("search-service") && eventType.equals("store.changed") ? handler : null;
}
```

Inbox durably deduplicates and retries received messages, but cannot recover an event never published or retract one published before rollback. Verify no Outbox row, duplicate delivery, Inbox retry, publication failure, and domain rollback.
