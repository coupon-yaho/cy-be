# L2 측정 회차 시작 전 토폴로지 검증

공식 회차는 관리자 benchmark start API를 통해서만 연다. start 경로는 API 로컬 실측과 batch
preflight를 먼저 끝내며, 위반이 하나라도 있으면 `benchmark_runs`의 RUNNING 행을 만들지 않는다.
검증은 스케줄러가 아니라 회차 시작 요청마다 한 번 실행된다.

## 검증 항목과 값의 소유자

| 소유자 | 항목 | 확인 이유 | 값의 출처 |
|---|---|---|---|
| batch | 검증 스케줄러 스위치 | 회차 중 검증 배치가 MySQL을 변경하지 않게 한다. Gauge는 별도 스위치라 계속 돈다. | `관제-진행기록.md`의 `L2 측정 프로토콜 (AB-G3)`과 batch 런타임 Environment |
| batch | 정합성 gap 수집 주기·쿠폰 | MySQL 조회 주기를 통제하고 Gauge가 시작 요청의 쿠폰을 계속 관측하게 한다. | 같은 AB-G3 결정, start의 `couponId`, batch 런타임 Environment |
| API | Tomcat worker·`accept-count`·`max-connections` | 스파이크가 worker 전에 소켓 관문에서 잘리면 엔진별 차이를 볼 수 없다. | API에 바인딩된 `TomcatServerProperties` |
| API | Hikari 풀 총량 | v1에서 의도한 커넥션 대기 병목을 동일 조건으로 재현한다. | API의 실제 운영 `HikariDataSource`와 요청의 replica 수 |
| API | 운영/관측 풀 분리 | 관측 쿼리가 운영 풀을 점유해 Hikari 병목 증거를 흐리는 것을 막는다. | API 컨텍스트의 실제 두 `DataSource` 빈 |
| MySQL | 연결 상한·회차 쿠폰 재고 | 연결 예산과 부하 프로필의 재고가 실제 DB 상태와 같은지 확인한다. | 기존 관측 풀로 읽은 `@@max_connections`와 `coupon_stocks.total_quantity` |

프로토콜 수치는 이 문서에 복사하지 않는다. 수치의 단일 원본은
`AB-B티켓-구현상세.html#/OBS-14b`의 목표 절이다.

## start 요청과 실측값의 경계

호출자는 회차 식별값, 회차 성격, 대상 쿠폰, API replica 수, 선택적 CPU·메모리 총량, 부하
프로필과 부하 도구 메타만 보낸다. Tomcat·Hikari·DataSource·가용 프로세서·MySQL 연결 상한은
호출자 값을 받지 않고 소유 프로세스가 직접 읽는다. `requestedBy`는 요청 본문이 아니라 기존
`Caller`의 회원 식별자에서 만든다.

요청의 API replica 수는 계산 근거가 아니다. 배포가 소유하는 `benchmark.topology.app-replicas`와
일치하는지만 확인하고, 총량 계산과 회차 기록에는 배포 선언값을 사용한다. Tomcat·Hikari는 현재
API 인스턴스의 실제 빈에서 읽지만, 실행 중인 replica 생존 수를 실측하는 게이트는 아니다.

실행 순서는 다음과 같다.

```text
API 로컬 실측 → 로컬 통과 시 batch preflight → RUNNING 행 생성
```

API 로컬 단계가 실패하면 batch를 호출하지 않는다. 로컬 단계가 통과했지만 제한 시간 안에 batch가
응답하지 않으면 RUNNING 행을 만들지 않는다. 위반은
설정 키, 기대값, 실제값, 측정을 무효로 만드는 이유를 start 실패 응답에도 함께 남긴다.

## 공식 측정 배포 선행 조건

AB-G3의 공식 토폴로지는 API N=4와 MySQL `max_connections=50`을 전제로 한다. 현재 `compose.yml`은
호스트 포트를 고정 매핑한 단일 API 개발 스택이고 MySQL 연결 상한도 설정하지 않으므로 공식 회차
환경이 아니다. P-1 배포 작업에서 API N=4의 부하 분산 경로와 MySQL 연결 상한을 먼저 제공해야 하며,
그 전에는 이 스택의 start gate 실패를 우회해서 회차를 열지 않는다.

공개 API 포트에서 실제 회차 상태를 바꾸는 `start`·`finalize`·archive retry는
`BENCHMARK_ADMIN_COMMAND_SECRET`과 일치하는 `X-Benchmark-Command-Secret` 헤더를 추가로 요구한다.
secret이 비어 있으면 명령은 모두 거부된다. 일반 관리자 역할 헤더는 호출자 역할 표시에 불과하므로
이 명령 인증을 대신하지 않는다.

archive 재시도는 DB 시각의 `archive_claimed_at`으로 lease 만료를 판단하고, claim마다 새 UUID v4
fencing token을 발급한다. 시계열 교체와 DONE/FAILED 전이는 현재 token이 일치할 때만 허용하므로,
lease를 잃은 이전 작업자는 새 소유자의 결과를 덮을 수 없다. lease는
`benchmark.archive.claim-lease`에서 배포 환경에 맞게 조정한다.
DB 쓰기 트랜잭션은 원본 교체의 원자성을 위해 하나로 유지하되, 조회된 표본이
`benchmark.archive.max-samples`를 넘으면 DB 쓰기를 시작하기 전에 실패시킨다.

## batch 내부 endpoint의 위협모델

`GET /internal/v1/benchmarks/preflight`는 상태를 바꾸지 않는 읽기 전용 endpoint다. compose에서 batch
업무 포트를 호스트에 매핑하지 않는 현재 배포를 신뢰 경계로 삼고, 접속 정보나 비밀값은 응답하지
않는다. 같은 compose 네트워크의 서비스는 호출할 수 있으므로 이 전제가 바뀌거나 업무 포트를
외부에 노출할 때는 서비스 간 인증을 먼저 추가해야 한다.
