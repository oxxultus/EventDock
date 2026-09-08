# Spring Boot 이벤트 소비

[English](consuming.md) | [한국어](consuming.ko.md)

Spring Boot Starter는 `@EventDockHandler`가 붙은 `EventHandler` bean을 자동 수집합니다. 애플리케이션은 Kafka listener나 `InboxHandlerRegistry`를 직접 만들 필요가 없습니다.

## 최소 구현

```java
@Component
@EventDockHandler(
    consumerId = "billing-service",
    eventType = "order.created"
)
final class OrderCreatedHandler implements EventHandler {
  private final EventCodec codec;
  private final BillingService billing;

  OrderCreatedHandler(EventCodec codec, BillingService billing) {
    this.codec = codec;
    this.billing = billing;
  }

  @Override
  public void handle(SerializedEvent event) {
    var envelope = codec.decode(event, OrderCreated.class);
    billing.openInvoice(envelope.payload().orderId());
  }
}
```

```yaml
eventdock:
  consumers:
    - id: billing-events
      mode: INBOX
      topics: [order-events]
      kafka:
        group-id: billing-service
      routes:
        - event-type: order.created
          consumer-id: billing-service
```

`routes`가 없으면 binding `id`가 consumer ID가 됩니다. 따라서 annotation의 `consumerId`는 최종 consumer ID와 정확히 같아야 합니다. `eventType`도 EventDock event type header와 대소문자까지 정확히 일치해야 하며 Kafka topic 이름을 뜻하지 않습니다.

처리 흐름은 `Kafka group → binding → eventType route → consumerId → annotated handler`입니다. Kafka `group-id`는 부하 분산 범위를 정하고, EventDock `consumerId`는 handler 선택과 Inbox 중복 제거 키 `(consumer-id, event-id)`에 사용됩니다. EventDock은 둘을 추측하지 않습니다.

## 여러 이벤트

이벤트마다 handler class를 분리하는 방식을 권장합니다.

```java
@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.created")
final class OrderCreatedHandler implements EventHandler { /* ... */ }

@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.cancelled")
final class OrderCancelledHandler implements EventHandler { /* ... */ }
```

하나의 handler가 여러 event type을 처리해야 하면 annotation을 반복할 수 있습니다.

```java
@Component
@EventDockHandler(consumerId = "audit-service", eventType = "order.created")
@EventDockHandler(consumerId = "audit-service", eventType = "order.cancelled")
final class OrderAuditHandler implements EventHandler { /* ... */ }
```

## INBOX와 DIRECT

같은 annotation API를 두 소비 모드에서 사용합니다.

- `INBOX`: 수신 저장, 영속 중복 제거, 재시도 및 handler의 트랜잭션 실행을 EventDock이 담당합니다.
- `DIRECT`: Kafka 수신 직후 handler를 실행합니다. Inbox 저장, 영속 중복 제거 및 영속 재시도는 없습니다.

모드는 handler가 아니라 `eventdock.consumers[].mode`가 결정합니다. handler를 바꾸지 않고 모드를 전환할 수 있습니다.

## 시작 시 검증과 기존 API

같은 `(consumerId, eventType)`을 handler 두 개에 등록하면 애플리케이션 시작이 실패합니다. annotation의 빈 값도 허용하지 않습니다. 매핑되지 않은 메시지는 기존 processor의 handler-not-found 실패 정책을 따릅니다.

기존 `InboxHandlerRegistry` 또는 `EventHandlerRegistry` bean을 직접 등록한 애플리케이션은 계속 동작합니다. 명시적 registry가 있으면 자동 registry가 물러나며, annotation handler와 명시적 registry를 자동 병합하지 않습니다.

`@EventDockHandler`는 Spring bean의 구현 class에 선언합니다. `@Component` 대신 `@Bean`을 사용해도 반환 객체의 구현 class에 annotation이 있으면 검색됩니다.
