# Spring Boot 설정 명세

[English](spring-boot.md) | [한국어](spring-boot.ko.md)

다음 의존성 하나를 추가합니다.

```gradle
implementation 'io.github.oxxultus:eventdock-spring-boot-starter:0.4.0'
```

Artifact는 Maven Central에서 사용할 수 있습니다. 아직 배포하지 않은 source checkout을 시험하려면 EventDock에서 `./gradlew publishToMavenLocal`을 실행하고 사용하는 프로젝트의 `mavenCentral()` 앞에 `mavenLocal()`을 임시로 추가합니다.

Starter는 애플리케이션의 `DataSource`, `PlatformTransactionManager`, `ObjectMapper` 및 `KafkaTemplate`을 사용합니다. PostgreSQL repository, transaction-aware Unit of Work, JSON codec, Kafka publisher, Inbox receiver 및 주기 실행 processor를 자동설정합니다. 동일한 contract의 애플리케이션 bean을 선언하면 각 bean을 교체할 수 있습니다.

```yaml
eventdock:
  initialize-schema: true
  publish-timeout: 10s
  producer:
    mode: outbox
  consumer:
    mode: inbox
  retry:
    max-attempts: 5
    initial-delay: 1s
    max-delay: 1m
    lock-timeout: 1m
  outbox:
    enabled: true
    batch-size: 100
    poll-interval: 1s
  inbox:
    enabled: true
    consumer-id: core-service
    topics: order.created,order.status-changed
    batch-size: 100
    poll-interval: 1s
  cleanup:
    enabled: true
    interval: 1h
    outbox-retention: 7d
    inbox-retention: 30d
    batch-size: 1000

```

소비 처리는 `@EventDockHandler`가 붙은 `EventHandler` bean을 자동 등록합니다. 설정으로 Outbox 또는 Direct 발행을 선택하려면 `EventWriter`를 주입합니다. 기본 Outbox Writer는 도메인 트랜잭션에 참여하므로 두 변경은 함께 commit되거나 rollback됩니다. [Handler 등록](../guides/consuming.ko.md)과 [신뢰성 모드](../concepts/reliability.ko.md)를 확인합니다.

위 `inbox.consumer-id` 방식은 기존 단일 listener 하나를 생성합니다. 여러 group, 혼합 모드 또는 eventType별 Inbox 식별자가 필요하면 [다중 consumer binding](../guides/multiple-consumers.ko.md)을 사용합니다.

Starter는 byte-array 전용 Kafka producer 및 consumer factory를 생성합니다. 애플리케이션의 기존 `KafkaTemplate`과 JSON listener factory는 변경하지 않습니다.

Schema 초기화는 기본적으로 활성화되며 멱등 DDL을 실행합니다. Flyway 등 배포 과정에서 `META-INF/eventdock/postgresql/V1__eventdock_schema.sql`을 적용한다면 `eventdock.initialize-schema=false`로 설정합니다.

잘못된 duration, batch size, retry 범위 또는 활성화된 Inbox 설정은 애플리케이션 시작을 실패시킵니다. Micrometer가 있으면 EventDock이 처리 결과 counter를 기록합니다. Spring Boot Health가 있으면 `eventDockHealthIndicator`가 Outbox·Inbox 대기 및 실패 건수를 제공합니다.

DLQ 발행이나 운영자 호출이 필요하면 `OutboxExhaustionHandler`, `InboxExhaustionHandler` bean을 구현합니다. 운영 callback이 실패해도 저장된 `FAILED` row가 기준 데이터로 유지됩니다.
