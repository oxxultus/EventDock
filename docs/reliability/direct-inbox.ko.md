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

## 생산자

동일한 `EventWriter` API에 `DirectEventWriter`가 선택되어 동기 발행합니다.

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

`write` 반환은 전송 완료를 뜻하며 DB 트랜잭션 commit을 뜻하지 않습니다. 발행 실패는 호출자에게 예외로 전달되지만 이후 rollback이 이미 발행된 이벤트를 회수하지는 못합니다.

## 소비자

```java
@Component
@EventDockHandler(consumerId = "search-service", eventType = "store.changed")
final class StoreChangedHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, StoreChanged.class);
    search.reindex(envelope.payload().storeId());
  }
}
```

Inbox는 수신 메시지를 영속 중복 제거하고 재시도하지만 미발행 이벤트를 복구하거나 rollback 전 발행 이벤트를 회수할 수 없습니다. Outbox row 미생성, 중복 전달, Inbox 재시도, 발행 실패, 도메인 rollback을 검증합니다.
