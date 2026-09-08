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

Inject `EventWriter`; it resolves to `DirectEventWriter`. Publication can occur before the domain transaction commits.

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
