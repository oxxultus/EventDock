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

## 생산자

소비자 모드와 관계없이 생산자 구현은 같습니다. `OUTBOX` 모드에서 선택된 `EventWriter`가 트랜잭션 안에 저장합니다.

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
        EventId.random(), "order.created", 1,
        new AggregateRef("order", Long.toString(orderId), 1),
        Instant.now(), new OrderCreated(orderId, memberId),
        Map.of("producer", "order-service")));
  }
}
```

Rollback되면 두 저장이 함께 취소되고 scheduler가 나중에 발행합니다. 소비자 `DIRECT`는 생산자 코드를 바꾸지 않습니다.

## 소비자

전송 중립적인 direct handler를 등록합니다.

```java
@Component
@EventDockHandler(consumerId = "notification-service", eventType = "order.created")
final class OrderCreatedNotificationHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, OrderCreated.class);
    notifications.sendCreated(envelope.payload().orderId());
  }
}
```

생산자는 Outbox에서 재시도합니다. Inbox row와 영속 중복 제거는 없으므로 handler를 자연 멱등하게 만들거나 비즈니스 멱등성 키를 사용합니다. 생산자 rollback·복구, 중복 재전달, Inbox row 미생성을 검증합니다.
