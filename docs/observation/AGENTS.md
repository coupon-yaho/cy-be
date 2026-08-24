# 관측·배포 작업 컨텍스트

이 디렉터리의 문서는 관측 인프라와 그 배포 상태를 기록한다. 코드·운영 설정을 바꿀 때는
현재 배포된 이미지와 소스 브랜치가 같은 기준인지 먼저 확인한다.

## OBS-28 — CY-5 이미지 재배포

2026-08-24 기준 관측·관리 API가 들어간 이미지는 `feature/CY-5` HEAD
`30cce1d8740fbc5188babd3411721eaa1ee96592` 기준으로 발행한다. 고정 배포 태그
`vobs28-cy5.1`은 원격에 push됐고 Docker 배포 워크플로가 성공했다. 다음 이미지를 배포에
사용한다.

- `seol7/coupon-yaho:api-vobs28-cy5.1`
- `seol7/coupon-yaho:batch-vobs28-cy5.1`

`compose.yml`은 이미지 태그를 직접 보관하지 않는다. 배포 환경의 비추적 `.env`에서
`COUPON_IMAGE`, `API_IMAGE_TAG`, `BATCH_IMAGE_TAG`를 설정한다. 이 배포에서는 두 태그를
각각 `api-vobs28-cy5.1`, `batch-vobs28-cy5.1`로 맞춘다.

### 2026-08-24 배포·실측 결과

새 API는 호스트 `18080`에서, 내부 API 포트는 `8080`에서 수신하도록 재생성했다. API와
batch의 관리 포트는 호스트에 공개하지 않고 Prometheus만 Docker 네트워크에서 `api:9090`과
`batch:9092`를 긁는다. 실제 Prometheus 질의 결과는 `up{job="api"}=1`,
`up{job="batch"}=1`, `app_*` 시계열 130개다.

아래 경로는 더 이상 404가 아니지만 모두 200인 것은 아니다.

| 계약 | 실측 HTTP | 상태 |
| --- | ---: | --- |
| overview | 200 | Mock 원천 응답. |
| issuance-histories | 200 | Mock 원천 응답. |
| members/issuance-inquiries | 200 | `memberId=1001`로 확인. |
| analytics | 501 | `from`·`to`를 채워도 Use Case가 미구현. |
| benchmarks | 501 | 목록 Use Case가 미구현. |
| runtime-config GET | 501 | Provider가 미연결. |
| runtime-config PUT | 501 | CAS 구현이 미연결. |

모든 실서버 요청에는 `X-User-Id: 1`, `X-User-Role: ADMIN`을 넣었다. 목 서버는 인증을
생략하므로 그 명령을 실배포에 그대로 쓰면 안 된다. `coupon-metrics`는 경로 형태가 달라
OBS-38에서 별도로 다룬다.

## main 병합 뒤에 반드시 갱신할 것

CY-5가 `main`에 병합되어 Docker 배포 워크플로가 성공하면, `api-latest`와 `batch-latest`는
그 main 커밋을 가리킨다. 그때 다음을 수행한다.

1. 배포 환경 `.env`의 `API_IMAGE_TAG`·`BATCH_IMAGE_TAG`를 `api-latest`·`batch-latest`로
   전환하거나, 추적 가능한 새 `v*` 태그로 함께 전환한다.
2. `docker compose pull` 뒤 API·batch·Prometheus를 재생성한다. batch도 반드시 교체해야
   `/actuator/prometheus`가 9092에서 살아 `up{job="batch"} == 1`이 된다.
3. 18080에서 위 7개 계약을 `X-User-Id` 헤더와 함께 재실측한다. 목 서버는 인증을 생략하므로
   그 명령을 실배포에 그대로 쓰면 안 된다.
4. Prometheus에서 `up{job="api"} == 1`, `up{job="batch"} == 1`, `app_*` 시계열 수집을
   실측한다.
5. 호스트 공개 포트와 컨테이너 API 포트가 다시 섞이지 않았는지 배포 계약 테스트와
   Prometheus 실측을 반복한다.

## 현재 알려진 보류 사항

- 신규 Git 클론에서 비추적 `.env`·`application.yml` 없이 전체 `./gradlew test`가 통과한다.
  API 통합 테스트는 `observation.datasource.enabled=true`를, batch 통합 테스트는
  `storage.jpa.auditing.enabled=false`를 직접 선언한다. 이 값들은 배포 이미지에서 읽히는
  `*.yml`의 전제였으나, 테스트가 gitignore 설정 파일에 의존하면 신규 클론에서 조건부 빈이
  사라져 실패한다.
- analytics·benchmarks·runtime-config의 501은 이미지 노후가 아니라 CY-5 코드의 명시적
  미구현이다. OBS-28 범위에서 구현하지 않으며, 각각의 Use Case·Provider/CAS 소유 티켓에서
  해결한다.
- 로컬 호스트의 3306(MySQL)과 6379(Redis)는 다른 프로세스가 점유했다. coupon-yaho 스택의
  호스트 공개 포트만 3307·6380으로 옮겼고, 컨테이너 내부 DB·Redis 포트는 계속 3306·6379다.
