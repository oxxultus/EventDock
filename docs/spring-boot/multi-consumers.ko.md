# 다중 consumer binding

[English](multi-consumers.md) | [한국어](multi-consumers.ko.md)

하나의 애플리케이션이 Kafka listener 여러 개를 가질 때 `eventdock.consumers`를 사용합니다. 각 binding은 Kafka group, topic, `INBOX` 또는 `DIRECT` 처리 모드를 독립적으로 선택합니다.

```yaml
eventdock:
  inbox:
    batch-size: 100
    poll-interval: 1s
  consumers:
    - id: point-order-picked-up
      mode: inbox
      topics: ORDER_PICKED_UP
      kafka:
        group-id: core-point-order-picked-up

    - id: cart-dish-state
      mode: direct
      topics: DISH_STATE_CHANGED
      kafka:
        group-id: core-cart-dish-state

    - id: order-member-events
      mode: inbox
      topics: MEMBER_CREATED,MEMBER_UPDATED,MEMBER_DELETED
      kafka:
        group-id: core-order-member-events
      routes:
        - event-type: MEMBER_CREATED
          consumer-id: core-order-member-created
        - event-type: MEMBER_UPDATED
          consumer-id: core-order-member-updated
        - event-type: MEMBER_DELETED
          consumer-id: core-order-member-deleted
```

## 식별 규칙

- `id`: binding 식별자이자 기본 EventDock consumer ID입니다.
- `kafka.group-id`: Kafka partition 분배와 offset 식별자이며 생략하면 `id`를 사용합니다.
- `topics`: 해당 listener container가 담당하는 topic입니다.
- `mode`: binding의 처리 경로입니다.
- `routes[].event-type`: Kafka topic이 아니라 EventDock event type header와 비교합니다.
- `routes[].consumer-id`: Inbox 중복 제거, 재시도 claim, 순서 처리 및 handler 탐색에 사용할 consumer ID를 덮어씁니다.

최종 결정되는 consumer ID를 `@EventDockHandler`에 등록합니다. EventDock이 handler bean을 자동 수집합니다.

```java
@Component
@EventDockHandler(consumerId = "core-order-member-created", eventType = "MEMBER_CREATED")
final class MemberCreatedHandler implements EventHandler {
  @Override
  public void handle(SerializedEvent event) {
    members.created(codec.decode(event, MemberCreated.class).payload());
  }
}
```

나머지 event type도 같은 방식으로 handler class를 추가합니다. 전체 규칙은 [Handler 자동 등록](handlers.ko.md)을 확인합니다.

`eventdock.consumers`와 `eventdock.inbox.enabled=true`를 함께 사용하지 않습니다. 기존 단일 listener 설정은 하위 호환을 위해 유지됩니다. `spring.kafka.listener.auto-startup=false`는 동적 EventDock container 시작도 중단합니다.
