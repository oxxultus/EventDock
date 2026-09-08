# Direct + Direct

[English](direct-direct.md) | [한국어](direct-direct.ko.md)

최소 영속화 비용이 중요하고 비핵심·재생성 가능·자연 멱등 작업일 때만 사용합니다.

## 설정

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

`inbox.enabled`는 direct listener를 활성화하지만 Inbox row를 만들지는 않습니다.

## 생산자

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

이 예제에는 도메인 트랜잭션이 없습니다. `write`는 즉시 발행하고 발행 실패 시 예외를 던지며 EventDock은 재시도 상태를 저장하지 않습니다.

## 소비자

Direct handler를 등록합니다.

```java
@Component
@EventDockHandler(consumerId = "analytics-service", eventType = "page.viewed")
final class PageViewedHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, PageViewed.class);
    analytics.record(envelope.payload());
  }
}
```

양쪽 모두 EventDock 상태를 저장하지 않습니다. 발행 누락, rollback 후 유령 이벤트, 중복 처리가 가능합니다. Outbox/Inbox row 미생성, 즉시 오류 전파, 무해한 중복, 이벤트 유실 허용 또는 재생성 가능 여부를 검증합니다.
