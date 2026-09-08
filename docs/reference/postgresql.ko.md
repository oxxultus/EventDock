# PostgreSQL 저장소

[English](postgresql.md) | [한국어](postgresql.ko.md)

`eventdock-storage-postgresql`은 JDBC로 EventDock의 Outbox, Inbox, Aggregate Version 및 Unit of Work port를 구현합니다. Spring이나 ORM에 의존하지 않습니다.

## 설정

1. `eventdock-storage-postgresql` 의존성을 추가합니다.
2. 애플리케이션의 마이그레이션 도구로 `META-INF/eventdock/postgresql/V1__eventdock_schema.sql`을 적용합니다.
3. `DataSource`로 `PostgresqlStorage`를 생성합니다.
4. 생성된 repository와 unit of work를 core processor에 전달합니다.

마이그레이션은 `eventdock` schema를 생성합니다. 작업 claim 및 완료 데이터 삭제를 위한 partial index도 포함합니다.

## 처리 보장

- Outbox와 Inbox claim은 원자적인 update-and-return query와 `FOR UPDATE SKIP LOCKED`를 사용합니다.
- 처리 lock이 만료된 작업을 다시 claim할 수 있습니다.
- Inbox 중복 방지는 `(consumer_id, event_id)` 범위로 적용됩니다.
- Aggregate Version은 Unit of Work 안에서 consumer 및 aggregate 단위로 잠깁니다.
- 별도의 JSON 라이브러리 없이 metadata를 결정적으로 인코딩합니다.

## 트랜잭션

EventDock 작업만 트랜잭션을 소유하면 `PostgresqlUnitOfWork`를 사용합니다. 도메인 변경과 EventDock record를 원자적으로 commit하려면 transaction-aware `DataSource`를 제공하고 host framework가 트랜잭션을 소유해야 합니다. Spring Boot adapter가 이 연동을 제공하며, core와 PostgreSQL 모듈은 프레임워크 독립성을 유지합니다.

## 검증

저장소 통합 테스트는 Testcontainers로 PostgreSQL 17을 사용합니다. Docker를 사용할 수 없으면 테스트를 건너뜁니다.
