# Consumer 식별과 멱등성

[English](consumer-identity.md) | [한국어](consumer-identity.ko.md)

세 식별자는 서로 다른 책임을 가집니다.

| 값 | 판단 주체 | 역할 |
| --- | --- | --- |
| `group-id` | Kafka | 메시지를 받을 소비자 그룹과 인스턴스 간 부하 분산 |
| `consumerId` | EventDock | Handler routing과 Inbox 멱등성 범위 |
| `eventId` | 생산자 | 개별 이벤트 식별 |

Inbox 고유 키는 `(consumerId, eventId)`입니다. `group-id`는 포함하지 않습니다.

## 배치 규칙

- 같은 서비스의 여러 인스턴스: 같은 `group-id`, 같은 `consumerId`를 사용합니다.
- 같은 이벤트를 각각 받아야 하는 서비스: 서로 다른 `group-id`, 서로 다른 `consumerId`를 사용합니다.
- 같은 DB를 공유하는 독립 consumer: 반드시 서로 다른 `consumerId`를 사용합니다.
- 서로 다른 group이 같은 DB에서 같은 `consumerId`를 사용하면 먼저 저장한 쪽 때문에 다른 쪽이 중복으로 판정될 수 있습니다.
- 서비스별 DB를 사용하면 각 DB의 Inbox에서 독립적으로 멱등성이 적용됩니다.

```text
Kafka group-id → binding → eventType route → consumerId → Handler
                                              ↓
                                  Inbox (consumerId, eventId)
```

같은 group의 listener와 인스턴스는 메시지를 각각 받지 않고 partition을 나눠 처리합니다. 같은 이벤트를 모두 받아야 하는 독립 소비자는 group을 분리합니다. 설정 예제는 [다중 Consumer](../guides/multiple-consumers.ko.md)를 확인합니다.
