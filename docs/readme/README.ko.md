# EventDock

[English](README.en.md) | [한국어](README.ko.md)

프레임워크에 독립적인 이벤트 계약과 Transactional Outbox, 멱등성 Inbox를 제공하는 라이브러리입니다.

## 빠른 시작

[사용 가이드](../getting-started/usage.ko.md)에서 Starter 설치, PostgreSQL·Kafka 설정, 트랜잭션 이벤트 발행 및 Inbox handler 등록 방법을 확인할 수 있습니다.

## 모듈

- `eventdock-core`: 이벤트 메시지 규격과 전송 기술에 독립적인 계약
- `eventdock-outbox`: Outbox 처리 규칙과 확장 포트
- `eventdock-inbox`: Inbox 처리, 멱등성 및 순서 보장 포트
- `eventdock-storage-postgresql`: PostgreSQL 저장소 어댑터
- `eventdock-transport-kafka`: Kafka 전송 어댑터
- `eventdock-spring-boot-autoconfigure`: 조건 기반 Spring Boot 자동설정
- `eventdock-spring-boot-starter`: 권장 Spring Boot 의존성 묶음

의존성은 항상 내부를 향합니다. 프레임워크와 기술 어댑터는 core 모듈에 의존하지만, core 모듈은 Spring, Kafka, Jackson, JPA 또는 특정 데이터베이스에 의존하지 않습니다.

## 초기 지원 범위

- Java 21
- PostgreSQL
- Kafka
- Spring Boot 4.1

프레임워크 독립적인 처리, PostgreSQL 저장소, Kafka 전송 및 Spring Boot starter 연동을 구현했습니다. 운영 고도화는 계속 개발합니다.

## 문서

- [사용 가이드](../getting-started/usage.ko.md)
- [신뢰성 모드](../reliability/modes.ko.md)
- [Maven Central 배포](../releasing/maven-central.ko.md)
- [시스템 설계](../design/system-design.ko.md)
- [아키텍처](../architecture/architecture.ko.md)
- [PostgreSQL 저장소](../storage/postgresql.ko.md)
- [Kafka 전송](../transport/kafka.ko.md)
- [Spring Boot starter](../spring-boot/starter.ko.md)
- [운영 Runbook](../operations/runbook.ko.md)
- [LastDish 전환](../migration/lastdish.ko.md)
- [기여 컨벤션](../project/CONTRIBUTING.ko.md)
- [변경 이력](../project/CHANGELOG.ko.md)
