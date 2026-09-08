# 빠른 시작

[English](quick-start.md) | [한국어](quick-start.ko.md)

이 문서는 Spring Boot Starter 기준입니다. 프레임워크 독립 방식에서는 필요한 core, inbox, outbox, storage, transport 모듈만 의존하고 각 포트를 직접 조립합니다.

## 1. 요구 사항

- Java 21
- Spring Boot 4.1
- PostgreSQL
- Kafka

사용할 애플리케이션에 Maven Central과 Starter를 추가합니다.

```gradle
repositories {
    mavenCentral()
}

dependencies {
    implementation 'io.github.oxxultus:eventdock-spring-boot-starter:0.4.0'
}
```

아직 배포하지 않은 source checkout을 시험하려면 EventDock에서 `./gradlew publishToMavenLocal`을 실행하고 `mavenCentral()` 앞에 `mavenLocal()`을 임시로 추가합니다.

## 2. PostgreSQL과 Kafka 설정

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/app
    username: app
    password: app
  kafka:
    bootstrap-servers: localhost:9092

eventdock:
  initialize-schema: true
  outbox:
    enabled: true
    batch-size: 100
    poll-interval: 1s
  inbox:
    enabled: true
    consumer-id: billing-service
    topics: order.created
    batch-size: 100
    poll-interval: 1s
```

로컬 개발에서는 `initialize-schema: true`가 편리합니다. 운영에서는 Flyway 또는 배포 파이프라인으로 `META-INF/eventdock/postgresql/V1__eventdock_schema.sql`을 적용하고 값을 `false`로 설정합니다.

기본 Kafka topic 이름은 이벤트 타입입니다. Kafka의 topic 자동 생성이 꺼져 있다면 서비스를 시작하기 전에 설정한 topic을 생성합니다.

이 설정은 기존 단일 listener 방식입니다. listener가 여러 개인 애플리케이션은 Kafka `group-id`와 EventDock `consumer-id`를 분리하고 eventType route를 지원하는 [다중 consumer binding](../guides/multiple-consumers.ko.md)을 사용합니다.

## 3. 도메인 트랜잭션에서 이벤트 발행

도메인 데이터 저장과 이벤트 추가를 하나의 Spring 트랜잭션에서 실행합니다. rollback되면 두 변경이 함께 취소됩니다.

```java
import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventEnvelope;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.core.EventWriter;
import java.time.Instant;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

record OrderCreated(long orderId, long memberId) {}

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

기본 `OUTBOX` 생산 모드에서는 EventDock이 이벤트를 Outbox 테이블에 저장하고 비동기로 발행한 뒤 완료 또는 재시도 상태를 기록합니다. `eventdock.producer.mode`를 `DIRECT`로 바꿔도 애플리케이션 코드는 그대로 유지됩니다.

## 4. Handler 등록

`@EventDockHandler`를 붙인 `EventHandler` bean을 선언합니다. EventDock이 consumer ID와 이벤트 타입으로 자동 등록합니다.

```java
import io.github.oxxultus.eventdock.core.EventCodec;
import io.github.oxxultus.eventdock.autoconfigure.EventDockHandler;
import io.github.oxxultus.eventdock.core.EventHandler;
import io.github.oxxultus.eventdock.core.SerializedEvent;

@Component
@EventDockHandler(consumerId = "billing-service", eventType = "order.created")
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

Inbox 처리와 handler의 데이터베이스 변경은 하나의 트랜잭션에서 실행됩니다. 동일한 `(consumer-id, event-id)`는 한 번만 처리합니다. Handler 누락과 애플리케이션 오류는 설정한 정책에 따라 재시도하며, 한도를 넘으면 `FAILED` row로 남습니다.

## 5. 로컬 검증

1. PostgreSQL과 Kafka를 실행합니다.
2. 생산자와 소비자 애플리케이션을 실행합니다.
3. 도메인 명령을 한 번 실행하고 `eventdock.outbox_events` row 생성을 확인합니다.
4. Outbox row가 `PUBLISHED`가 되고 소비자에 `eventdock.inbox_events`가 생성되는지 확인합니다.
5. Inbox row가 `PROCESSED`가 되고 기대한 도메인 변경이 저장됐는지 확인합니다.
6. Kafka를 잠시 중단하고 Outbox row 유실 없이 재시도 상태가 증가하는지 확인합니다.

운영 반영 전에는 [운영 Runbook](../operations/runbook.ko.md)을 확인합니다. LastDish를 교체한다면 [전환 가이드](../migration/lastdish.ko.md)를 따릅니다. 기존 전체 JSON Kafka envelope와 EventDock wire format은 직접 호환되지 않습니다.

## 상세 문서

- [4가지 신뢰성 모드 구현 가이드](../concepts/reliability.ko.md)
- [Spring Boot Starter](../reference/spring-boot.ko.md)
- [다중 consumer binding](../guides/multiple-consumers.ko.md)
- [Handler 자동 등록](../guides/consuming.ko.md)
- [PostgreSQL 저장소](../reference/postgresql.ko.md)
- [Kafka 전송](../reference/kafka.ko.md)
- [아키텍처](../development/architecture.ko.md)
