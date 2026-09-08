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

`inbox.enabled`는 direct listener를 활성화하지만 Inbox row를 만들지는 않습니다. `EventWriter`로 발행하고 direct handler를 등록합니다.

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

양쪽 모두 EventDock 상태를 저장하지 않습니다. 발행 누락, rollback 후 유령 이벤트, 중복 처리가 가능합니다. Outbox/Inbox row 미생성, 즉시 오류 전파, 무해한 중복, 이벤트 유실 허용 또는 재생성 가능 여부를 검증합니다.
