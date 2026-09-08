# 아키텍처

[English](architecture.md) | [한국어](architecture.ko.md)

## 의존성 규칙

Core 모듈에는 정책과 port를 둡니다. 기술 모듈은 해당 port를 구현합니다. 프레임워크 모듈은 구현체 조립과 관리만 담당합니다.

```text
eventdock-spring-boot-starter
          |
eventdock-spring-boot-autoconfigure
          |
eventdock-storage-postgresql + eventdock-transport-kafka
          |
eventdock-outbox + eventdock-inbox
          |
eventdock-core
```

## 확장 규칙

- 새로운 데이터베이스: `eventdock-storage-<database>`를 추가합니다.
- 새로운 브로커: `eventdock-transport-<broker>`를 추가합니다.
- 새로운 직렬화 형식: `eventdock-codec-<format>`을 추가합니다.
- 새로운 프레임워크: 해당 프레임워크의 통합 모듈 또는 Starter를 추가합니다.
- 이벤트 계약이나 처리 정책이 바뀔 때만 core를 변경합니다.

Outbox와 Inbox는 이벤트 envelope을 공유하지만 각각 독립적으로 사용할 수 있는 기능으로 유지합니다.
