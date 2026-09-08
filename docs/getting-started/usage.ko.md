# 사용 가이드

[English](usage.md) | [한국어](usage.ko.md)

이 문서는 Spring Boot Starter 기준입니다. 프레임워크 독립 방식에서는 필요한 core, inbox, outbox, storage, transport 모듈만 의존하고 각 포트를 직접 조립합니다.

## 1. 요구 사항

- Java 21
- Spring Boot 4.1
- PostgreSQL
- Kafka

EventDock은 아직 Maven Central에 배포되지 않았습니다. 먼저 현재 checkout을 Maven Local에 배포합니다.

```shell
./gradlew publishToMavenLocal
```

사용할 애플리케이션에 Maven Local과 Starter를 추가합니다.

```gradle
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation 'io.github.oxxultus:eventdock-spring-boot-starter:0.1.0-SNAPSHOT'
}
```

정식 저장소 버전을 사용하게 되면 `mavenLocal()`을 제거합니다.

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

## 3. 도메인 트랜잭션에서 이벤트 발행

도메인 데이터 저장과 이벤트 추가를 하나의 Spring 트랜잭션에서 실행합니다. rollback되면 두 변경이 함께 취소됩니다.

```java
import io.github.oxxultus.eventdock.core.AggregateRef;
import io.github.oxxultus.eventdock.core.EventEnvelope;
import io.github.oxxultus.eventdock.core.EventId;
import io.github.oxxultus.eventdock.outbox.OutboxWriter;
import java.time.Instant;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

record OrderCreated(long orderId, long memberId) {}

@Service
class OrderService {
  private final OrderRepository orders;
  private final OutboxWriter outbox;

  OrderService(OrderRepository orders, OutboxWriter outbox) {
    this.orders = orders;
    this.outbox = outbox;
  }

  @Transactional
  void create(long orderId, long memberId) {
    orders.save(new Order(orderId, memberId));

    outbox.append(new EventEnvelope<>(
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

EventDock은 이벤트를 Outbox 테이블에 저장하고 비동기로 발행한 뒤 완료 또는 재시도 상태를 기록합니다. 도메인 트랜잭션에서 Kafka 메시지를 직접 전송하지 않습니다.

## 4. Inbox handler 등록

`InboxHandlerRegistry` bean 하나를 선언합니다. consumer ID와 이벤트 타입으로 handler를 찾고 `EventCodec`으로 payload를 역직렬화합니다.

```java
import io.github.oxxultus.eventdock.core.EventCodec;
import io.github.oxxultus.eventdock.inbox.InboxHandler;
import io.github.oxxultus.eventdock.inbox.InboxHandlerRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class EventHandlers {
  @Bean
  InboxHandlerRegistry inboxHandlers(EventCodec codec, BillingService billing) {
    InboxHandler orderCreated = event -> {
      var envelope = codec.decode(event, OrderCreated.class);
      billing.openInvoice(envelope.payload().orderId());
    };

    return (consumerId, eventType) ->
        consumerId.equals("billing-service") && eventType.equals("order.created")
            ? orderCreated
            : null;
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

- [Spring Boot Starter](../spring-boot/starter.ko.md)
- [PostgreSQL 저장소](../storage/postgresql.ko.md)
- [Kafka 전송](../transport/kafka.ko.md)
- [아키텍처](../architecture/architecture.ko.md)
