# EventDock 기여 컨벤션

[English](../../CONTRIBUTING.md) | [한국어](CONTRIBUTING.ko.md)

## 아키텍처

- 의존성은 프레임워크·기술 어댑터 → 기능 core → `eventdock-core` 순서로 내부를 향해야 합니다.
- Core 모듈은 Spring, Jakarta Persistence, Kafka, Jackson, Lombok 또는 데이터베이스 드라이버에 의존하지 않습니다.
- Outbox와 Inbox는 각각 독립적으로 사용할 수 있어야 하며 `eventdock-core`의 계약만 공유합니다.
- 새로운 데이터베이스, 브로커, codec 또는 프레임워크는 새로운 어댑터 모듈로 추가합니다. 도메인 정책이나 공개 계약이 바뀔 때만 기존 core를 변경합니다.
- Starter와 자동설정 모듈은 객체 조립, 설정 바인딩 및 생명주기만 담당합니다. 비즈니스 규칙은 core 모듈에 둡니다.
- 공개 API는 `String` 같은 기술 중립적인 식별자를 사용하며, 기술별 타입은 어댑터 내부에 둡니다.

## 모듈 이름

- 핵심 계약: `eventdock-core`
- 기능: `eventdock-<feature>`
- 저장소 어댑터: `eventdock-storage-<database>`
- 전송 어댑터: `eventdock-transport-<broker>`
- Codec 어댑터: `eventdock-codec-<format>`
- 프레임워크 통합: `eventdock-<framework>-autoconfigure` 또는 `eventdock-<framework>-starter`

Java 패키지는 `io.github.oxxultus.eventdock.<area>` 형식을 사용합니다.

## 코드

- Java 21을 기준 버전으로 사용합니다.
- 불변 record와 생성자 주입을 우선합니다.
- 공개 경계의 입력을 검증하고 안정적이며 조치 가능한 오류 메시지를 제공합니다.
- Core 로직 안에서 블로킹 I/O를 숨기거나 트랜잭션을 시작하거나 스레드를 생성하지 않습니다.
- Core 로직에서 시스템 시간을 직접 호출하지 않고 `Clock`을 사용합니다.
- Major 버전 안에서는 바이너리 및 소스 호환성을 유지합니다. 가능한 경우 제거 전에 먼저 deprecated 처리합니다.

## 테스트

- Core 정책은 프레임워크 없이 단위 테스트합니다.
- 어댑터 통합 테스트는 가능한 경우 Testcontainers를 통해 실제 기술을 사용합니다.
- 동시성 테스트에서는 중복 선점, 만료된 lock 복구 및 재시도 소진을 검증해야 합니다.
- 트랜잭션 테스트에서는 비즈니스 rollback이 Outbox append도 함께 rollback하는지 증명해야 합니다.
- 모든 버그 수정에는 회귀 테스트를 포함합니다.

## Git 작업 흐름

- `main`은 항상 릴리스 가능한 상태를 유지합니다.
- 최신 `main`에서 작업 브랜치를 생성합니다.
- 브랜치는 `feature/<issue>-<short-name>`, `fix/<issue>-<short-name>`, `refactor/<issue>-<short-name>` 또는 `docs/<issue>-<short-name>` 형식을 사용합니다.
- 하나의 Issue, 브랜치 및 Pull Request는 하나의 목적만 다룹니다.
- 필수 CI 검사가 통과하면 Pull Request를 병합합니다. 관리자는 리뷰 없이 자신의 Pull Request를 병합할 수 있습니다.
- Squash merge를 우선하며 병합 후 브랜치를 삭제합니다.

커밋 형식:

```text
Type: 간결한 영어 제목

- 변경한 내용
- 변경한 이유
```

허용하는 Type은 `Feat`, `Fix`, `Refactor`, `Test`, `Docs`, `Build`, `CI`, `Chore`, `Perf`, `Revert`입니다.

## Pull Request

각 Pull Request에 다음 내용을 기록합니다.

- 변경 내용과 동기
- 영향을 받는 공개 API와 호환성
- 수행한 테스트
- 데이터베이스, 설정, 마이그레이션 및 운영 영향
- 해당하는 경우 `Closes #<number>` 형식으로 연결한 Issue

## 릴리스

- 유의적 버전 규칙을 따릅니다.
- 모든 모듈은 하나로 정렬된 버전을 사용합니다.
- 사용자에게 보이는 변경 사항은 `CHANGELOG.ko.md`에 기록합니다.
- `1.0.0` 이후 공개 API 또는 데이터베이스 스키마의 호환성을 깨는 변경은 major 릴리스가 필요합니다.
- 어댑터 추가는 minor 릴리스, 호환 가능한 수정은 patch 릴리스로 배포합니다.
- Snapshot을 안정 버전으로 배포하지 않습니다.
