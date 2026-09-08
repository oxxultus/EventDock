# LastDish 전환

[English](lastdish.md) | [한국어](lastdish.ko.md)

이벤트 흐름을 하나씩 전환합니다. 기존 producer나 consumer가 연결된 상태에서 동일 topic을 legacy JSON `EventMessage` 형식과 EventDock header 형식 사이에서 변경하지 않습니다.

## 필드 매핑

| LastDish `EventMessage` | EventDock |
|---|---|
| `eventId` | `EventId.value` |
| `eventType` | `EventEnvelope.type` |
| `aggregateType` | `AggregateRef.type` |
| `aggregateId` | 문자열 형태의 `AggregateRef.id` |
| `aggregateVersion` | `AggregateRef.version` |
| `schemaVersion` | `EventEnvelope.schemaVersion` |
| `payload` | `EventCodec`이 인코딩하는 payload 객체 |
| `occurredAt` | `EventEnvelope.occurredAt` |

## 단계적 교체

1. Maven Central의 EventDock `0.1.0`과 Starter를 한 서비스에 적용합니다.
2. EventDock schema를 적용하고 해당 서비스에서만 기존 Outbox/Inbox 자동설정을 비활성화합니다.
3. 위험이 낮은 이벤트를 선정하고 전환 중에는 새 topic을 사용합니다.
4. 기존 `@Transactional` use case 안의 writer를 `OutboxWriter.append(EventEnvelope<?>)`로 교체합니다.
5. 기존 message handler를 연결하는 `InboxHandlerRegistry` adapter를 등록합니다.
6. Kafka byte-array serializer와 서비스별 `consumer-id`, topic을 설정합니다.
7. 도메인 rollback, Outbox 재시도, 중복 전달, 이전 Aggregate Version, 서비스 재시작 후 복구를 검증합니다.
8. 이벤트별로 반복한 후 LastDish의 `event-common`, `outbox`, `inbox` 모듈 의존성을 제거합니다.

기존 table은 column과 소유권이 다르므로 재사용하지 않습니다. 전환 중 두 schema를 함께 유지하고 legacy 대기 record를 모두 처리한 뒤 합의한 보존 기간 이후 기존 table을 archive합니다.

## 준비 범위

EventDock은 LastDish 공통 라이브러리 교체에 필요한 직렬화, 트랜잭션 기반 append, PostgreSQL 저장, Kafka 발행, 내구성 있는 수신, 멱등 처리, 순서 제어, 재시도, lock 복구, scheduling 및 Spring Boot 연동을 제공합니다. 서비스별 이벤트 class와 handler 등록은 의도적으로 애플리케이션 코드에 남깁니다.
