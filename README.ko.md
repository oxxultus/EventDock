# EventDock

[English](README.en.md) | [한국어](README.ko.md)

프레임워크에 독립적인 이벤트 계약과 Transactional Outbox, 멱등성 Inbox를 제공하는 라이브러리입니다.

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

현재는 API 우선 설계를 위한 초기 골격입니다. 저장소 SQL, 트랜잭션 연동, Kafka 메시지 변환, 자동설정, 마이그레이션 및 운영 환경 테스트를 단계적으로 구현합니다.

아키텍처, 개발, Git 및 릴리스 규칙은 [CONTRIBUTING.md](CONTRIBUTING.md)를 참고하세요.
