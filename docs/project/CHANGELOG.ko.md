# 변경 이력

[English](CHANGELOG.md) | [한국어](CHANGELOG.ko.md)

EventDock의 주요 변경 사항을 이 파일에 기록합니다.

이 문서는 Keep a Changelog 형식을 따르며, 프로젝트는 유의적 버전 규칙을 준수합니다.

## [미배포]

### 추가

- 네 가지 신뢰성 모드 조합별 영문·한국어 전체 구현 가이드를 추가했습니다.

## [0.2.0] - 2026-09-08

### 추가

- 독립적인 생산자 `OUTBOX`/`DIRECT` 및 소비자 `INBOX`/`DIRECT` 신뢰성 모드를 추가했습니다.

## [0.1.0] - 2026-09-08

### 추가

- 최초 멀티모듈 아키텍처와 프레임워크 독립적인 이벤트 계약을 추가했습니다.
- 아키텍처, 처리 흐름 및 생명주기 다이어그램을 포함한 영문·한국어 시스템 설계 문서를 추가했습니다.
- 재시도, 재시도 소진, 멱등성 및 latest-wins 순서 정책을 지원하는 프레임워크 독립적인 Outbox·Inbox Processor를 추가했습니다.
- Core 모듈 외부에서 트랜잭션을 제어하기 위한 Unit of Work port를 추가했습니다.
- JDBC 기반 PostgreSQL Outbox, Inbox, Aggregate Version, 마이그레이션 및 정리 기능을 추가했습니다.
- Kafka header 기반 wire mapping, publisher 및 내구성 있는 Inbox receiver를 추가했습니다.
- Spring Boot 자동설정, 주기 실행 processor, JSON codec 및 Maven Local 배포 설정을 추가했습니다.
- 운영 cleanup, 설정 검증, 재시도 소진 hook, Micrometer metric, health 상세정보 및 장애 경로 통합 테스트를 추가했습니다.
- 버전 태그로 실행되는 서명된 Maven Central 자동 배포를 추가했습니다.
