# Outbox + Direct

[English](outbox-direct.md) | [한국어](outbox-direct.ko.md)

생산자 장애에도 발행을 보존해야 하지만 지연에 민감한 소비자가 이미 멱등성을 보장할 때 사용합니다.

## 설정

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

도메인 트랜잭션에서 `EventWriter`로 발행하고 전송 중립적인 direct handler를 등록합니다.

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

생산자는 Outbox에서 재시도합니다. Inbox row와 영속 중복 제거는 없으므로 handler를 자연 멱등하게 만들거나 비즈니스 멱등성 키를 사용합니다. 생산자 rollback·복구, 중복 재전달, Inbox row 미생성을 검증합니다.
