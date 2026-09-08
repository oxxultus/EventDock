# Maven Central 배포

[English](maven-central.md) | [한국어](maven-central.ko.md)

EventDock은 Central Publisher Portal을 통해 모듈 7개를 모두 배포합니다. 버전 태그는 변경할 수 없는 배포 단위이며 Release Workflow를 시작합니다.

## 필수 Repository Secret

GitHub Actions Secret에 다음 값을 설정합니다.

- `MAVEN_CENTRAL_USERNAME`: Central Portal 사용자 토큰의 username
- `MAVEN_CENTRAL_PASSWORD`: Central Portal 사용자 토큰의 password
- `SIGNING_KEY`: ASCII-armored PGP 비밀키 전체
- `SIGNING_PASSWORD`: PGP 비밀키 비밀번호

Central Portal에서 `io.github.oxxultus` namespace가 인증돼 있어야 합니다. 공개 PGP 키는 Maven Central이 지원하는 keyserver에 등록합니다. 인증정보와 비밀키를 Git, Workflow 입력, 로그 또는 명령행 인자로 저장하지 않습니다.

## 배포 절차

1. `main` CI 통과와 깨끗한 작업 트리를 확인합니다.
2. 영문·한국어 Changelog의 관련 내용을 `Unreleased`에서 배포 버전과 날짜 아래로 이동합니다.
3. 배포할 `main` commit에 유의적 버전 annotated tag를 생성합니다.
4. 태그를 push합니다.
5. `Release to Maven Central` Workflow와 Central Portal deployment를 확인합니다.
6. 모든 artifact가 Maven Central에서 조회된 후 설치 예제의 버전을 변경합니다.

```shell
git switch main
git pull --ff-only
git tag -a v0.1.0 -m "Release 0.1.0"
git push origin v0.1.0
```

Workflow는 `v0.1.0`에서 `0.1.0`을 구하고, 해당 버전을 build·test한 뒤 서명된 publication 7개를 staging합니다. 파일 구성을 검증한 다음 JReleaser로 deployment를 업로드합니다. 유의적 버전 형식이 아닌 태그는 배포 전에 실패합니다.

## 배포 실패

Maven Central에 공개된 버전은 재사용하지 않습니다. 문제를 수정하고 다음 버전을 배포합니다. 공개 전 검증이 실패했다면 업로드된 `jreleaser-<version>` 진단 artifact와 Central Portal deployment를 확인합니다. 잘못된 로컬·원격 태그는 해당 버전이 공개되지 않은 경우에만 삭제합니다.

Maven Central에 공개된 배포는 수정하거나 삭제할 수 없습니다.
