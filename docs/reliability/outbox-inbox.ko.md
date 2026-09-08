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

## 생산자

모드 중립적인 `EventWriter`를 주입합니다. `OUTBOX` 모드의 `write`는 현재 데이터베이스 트랜잭션에 이벤트를 추가합니다.

```java
@Service
class OrderService {
  private final OrderRepository orders;
  private final EventWriter events;

  OrderService(OrderRepository orders, EventWriter events) {
    this.orders = orders;
    this.events = events;
  }

  @Transactional
  void create(long orderId, long memberId) {
    orders.save(new Order(orderId, memberId));
    events.write(new EventEnvelope<>(
        EventId.random(),
        "order.created",
        1,
        new AggregateRef("order", Long.toString(orderId), 1),
        Instant.now(),
        new OrderCreated(orderId, memberId),
        Map.of("producer", "order-service")));
  }
}
```

메서드가 rollback되면 주문과 Outbox row가 함께 rollback됩니다. Outbox scheduler가 commit 후 발행하므로 여기서 Kafka를 직접 호출하지 않습니다.

## 소비자

```java
@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.created")
final class OrderCreatedHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, OrderCreated.class);
    billing.openInvoice(envelope.payload().orderId());
  }
}
```

전달은 반복될 수 있지만 `(consumer-id, event-id)`로 영속 중복 제거합니다. 도메인/Outbox 동시 rollback, `PUBLISHED`·`PROCESSED` 상태, 중복 전달, Kafka·소비자 중단 후 복구를 검증합니다.
