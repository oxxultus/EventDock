# Outbox + Inbox

[English](outbox-inbox.md) | [한국어](outbox-inbox.ko.md)

중요한 상태 변경에 사용합니다. 생산자는 도메인 변경과 Outbox row를 함께 commit하고 소비자는 저장 후 처리합니다.

## 설정

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

도메인 `@Transactional` 메서드에서 [생산자 예제](../getting-started/usage.ko.md#3-도메인-트랜잭션에서-이벤트-발행)처럼 `EventWriter.write(...)`를 호출합니다.

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

전달은 반복될 수 있지만 `(consumer-id, event-id)`로 영속 중복 제거합니다. 도메인/Outbox 동시 rollback, `PUBLISHED`·`PROCESSED` 상태, 중복 전달, Kafka·소비자 중단 후 복구를 검증합니다.
