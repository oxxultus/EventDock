# 운영 Runbook

[English](runbook.md) | [한국어](runbook.ko.md)

## 전달 모델

EventDock은 at-least-once 전달을 제공합니다. Broker 응답 이후 데이터베이스 commit이 실패하면 이벤트가 중복 발행될 수 있습니다. 따라서 Inbox 고유성 보장과 반복 실행에 안전한 handler가 필수입니다. PostgreSQL과 Kafka를 아우르는 exactly-once 전달을 주장하지 않습니다.

Kafka offset은 listener가 반환된 뒤 commit됩니다. Listener는 record를 Inbox에 저장하거나 중복임을 확인한 뒤 반환합니다. 비즈니스 처리는 이후 Inbox scheduler가 수행하며 상태 및 version 변경과 함께 commit됩니다.

## 알림

다음 counter와 health detail 증가를 감시합니다.

- `eventdock.outbox.events{outcome="failed"}`
- `eventdock.inbox.events{outcome="failed"}`
- 지속적인 `retry_scheduled` 증가
- `outboxPending`, `inboxPending`, `outboxFailed`, `inboxFailed`

Backlog 자체가 장애인 것은 아닙니다. 서비스의 정상 처리 시간을 넘어 계속 증가할 때 알립니다.

## 실패 record와 DLQ

`eventdock.retry.max-attempts`를 소진한 row는 `FAILED`가 됩니다. Exhaustion handler를 등록해 payload와 비밀정보를 제외한 운영 메시지를 DLQ 또는 장애 관리 시스템에 전달합니다. 재처리는 감사 가능한 수동 작업 또는 향후 관리 API로 수행합니다. 장애 중 `FAILED` row를 직접 변경하면 중복 처리 위험이 있습니다.

## 재시작과 확장

여러 instance가 scheduler를 동시에 실행할 수 있습니다. Claim은 `FOR UPDATE SKIP LOCKED`를 사용합니다. 종료된 worker가 남긴 `PROCESSING` row는 `eventdock.retry.lock-timeout` 이후 다시 claim할 수 있습니다. Lock timeout은 예상되는 handler 또는 발행 최대 시간보다 길게 설정합니다.

종료 시 애플리케이션 context보다 traffic과 Kafka listener container를 먼저 중지합니다. 진행 중인 데이터베이스 트랜잭션은 rollback됩니다. 이미 broker가 확인한 전송은 중복될 수 있으며 Inbox 멱등성으로 처리합니다.

## 보존기간

Cleanup은 설정한 보존기간을 지난 `PUBLISHED`, `PROCESSED`, `SKIPPED` row만 삭제합니다. 대기, 처리 중 또는 실패 row는 삭제하지 않습니다. 보존기간은 장애 조사 및 replay 기간보다 길게 설정하고 애플리케이션 데이터 정책에 따라 table을 백업합니다.

## 배포 Gate

운영 배포 전 다음을 확인합니다.

1. Docker를 사용할 수 있는 환경에서 `./gradlew clean build`를 실행해 PostgreSQL, Kafka, 트랜잭션, 동시성 및 cleanup 통합 테스트를 수행합니다.
2. 생성된 POM, sources JAR, Javadoc JAR 및 서명을 확인합니다.
3. 분리된 topic에서 producer와 consumer 흐름 하나를 배포합니다.
4. 중복 전달, broker 장애, 데이터베이스 장애, process 강제 종료 및 rollback을 시험합니다.
5. Dashboard, alert, DLQ callback, 보존기간 및 rollback 절차를 확인합니다.
