# Direct + Inbox

[English](direct-inbox.md) | [한국어](direct-inbox.ko.md)

낮은 발행 지연이 필요하고 생산자 원자성보다 소비자의 영속 중복 제거가 중요할 때 사용합니다.

## 설정

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

`EventWriter`를 주입하면 `DirectEventWriter`가 선택됩니다. 도메인 트랜잭션 commit 전에 발행될 수 있습니다.

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

Inbox는 수신 메시지를 영속 중복 제거하고 재시도하지만 미발행 이벤트를 복구하거나 rollback 전 발행 이벤트를 회수할 수 없습니다. Outbox row 미생성, 중복 전달, Inbox 재시도, 발행 실패, 도메인 rollback을 검증합니다.
