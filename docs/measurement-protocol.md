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

## 회차 중 scrape 건강도 측정 절차

L2 회차는 `scrape_interval: 1s`로 도는 Prometheus를 전제한다. 수집이 밀려도 화면에는 오류가
나지 않고 값이 성글어질 뿐이므로, 회차마다 아래를 함께 잰다. 프로토콜 수치는 이 문서에 적지
않는다 — 임계와 실측값의 단일 원본은 `infra/prometheus/prometheus.yml`의 `global:` 위 주석이다.

### 재는 대상은 둘이다

`api:9090`과 `batch:9092` 두 job을 모두 잰다. 도메인 Gauge가 통째로 batch에서 나오므로 api만
재면 화면의 절반을 안 본 것이 된다.

### 회차 전에 기록할 것

측정 조건이 문단에 남아 있지 않으면 나중에 수치가 흔들린 이유를 찾을 수 없다.

| 항목 | 기록 내용 |
|---|---|
| CPU 제한 방식 | 컨테이너 CPU 쿼터인지 전용 vCPU 머신인지. 둘은 같은 조건이 아니다 — 쿼터에는 호스트 스케줄러가 위에 하나 더 있다 |
| 실제 코어 인식값 | 컨테이너의 `nproc`이 아니라 `system_cpu_count`를 본다. CPU 쿼터는 cpuset이 아니라서 `nproc`은 호스트 코어 수를 그대로 보여준다 |
| 토폴로지 | API replica 수와 MySQL `max_connections`. 공식 AB-G3 값과 다르면 그 사실을 적는다 |
| 활성 회차 수 | 구간 시작·종료 시점의 활성 `coupons` 회차 수, 그리고 구간 중 회차가 닫힌 시각. 회차마다 미터가 `coupon_id`(=회차 id) 라벨로 붙으므로 페이로드가 회차 수에 비례해 계단을 만든다. 이 미터의 코드상 이름이 `CampaignMeterRegistry`라 로그·지표에는 '캠페인'으로 보이지만, 가리키는 개체는 회차다 |
| 부하 프로필 | 도착률·유지 시간·대상 endpoint. 회차 프로필에서 고친 것이 있으면 무엇을 왜 고쳤는지 |
| batch 가동 | 정합성 gap 수집이 MySQL을 치는 상태인지 |
| 페이로드 | `/actuator/prometheus`를 직접 긁은 줄 수·바이트·표본 수 |

### 수집값을 세는 스크립트는 가져오기 성공을 먼저 확인한다

`/actuator/prometheus`를 받아 라벨 종수를 세는 식의 보조 스크립트는, 가져오기가 실패했을 때와
"실제로 0개"일 때가 **같은 숫자로 나온다.** stderr를 버리고 빈 출력을 그대로 세면 구분할 방법이
없다. 실제로 이 때문에 없는 결함을 하나 만들어 낸 적이 있다 — 회차별 미터가 사라진 것으로
읽었으나 미터는 그대로였고 페이로드 가져오기가 실패한 것이었다.

받은 본문이 비어 있지 않은지, 종료 코드가 0인지를 먼저 보고, 실패면 그 회차를 0이 아니라
**측정 실패로 기록한다.** 부하 중에는 이 실패가 실제로 난다 — 재려는 대상이 바로 그
"응답이 늦거나 실패하는 상태"이기 때문이다.

### 관측 창을 부하 구간으로 자른다

유휴 구간이 섞이면 평균이 낙관적으로 나온다. 부하 시작·종료 시각을 기록하고 그 구간으로
질의를 자른다.

**부하가 끝난 직후는 유휴가 아니다.** `accept-count` 백로그와 Tomcat 큐가 남아 있어 CPU가
한동안 상한에 붙어 있다. 유휴 구간을 자를 때는 `process_cpu_usage`가 실제로 떨어진 뒤부터
잡는다. 이 확인을 빼면 유휴 수치에 부하 잔여가 섞인다.

### 질의

Prometheus는 호스트 포트를 열지 않으므로 compose 네트워크 안에서 친다.

```
docker exec <prometheus 컨테이너> sh -c \
  "wget -qO- 'http://localhost:9090/api/v1/query?query=<expr>&time=<창 끝 epoch>'"
```

창 길이를 `<W>s`에 넣고 창 끝 시각에 instant query로 평가한다. job마다 한 벌씩 돌린다.

```
# 분포 — 평균만으로는 판정이 안 된다. 셋이 서로 다른 방향을 가리킬 수 있다
avg_over_time(scrape_duration_seconds{job="api"}[<W>s])
quantile_over_time(0.99, scrape_duration_seconds{job="api"}[<W>s])
max_over_time(scrape_duration_seconds{job="api"}[<W>s])

# 실제 수집 실패율 = 1 - 이 값
avg_over_time(up{job="api"}[<W>s])

# 수집기가 아예 멈췄는가 (샘플 자체가 없는 고장)
min_over_time(count_over_time(up{job="api"}[1m])[<W>s:1m])

# 타임아웃 초과 건수와 전체 건수
sum_over_time((scrape_duration_seconds{job="api"} > bool 0.9)[<W>s:1s])
count_over_time(scrape_duration_seconds{job="api"}[<W>s])

# 페이로드의 시간 변화 — 활성 회차 수 변동이 여기 계단으로 보인다
scrape_samples_scraped{job="api"}
```

### 두 지표를 함께 본다

`scrape_timeout`을 넘긴 스크레이프는 실패로 기록되어 `up=0`을 **쓴다**. `up=0`도 샘플이므로
`count_over_time(up[1m])`은 60에서 안 내려간다. 즉 이 지표 하나로는 타임아웃 초과를 못 잡는다.

```
avg_over_time(up[...])              타임아웃 초과·수집 실패를 본다
count_over_time(up[1m])             수집기가 아예 멈춰 샘플이 없는 고장을 본다
```

둘은 서로 다른 고장을 보므로 어느 하나로 다른 하나를 대신할 수 없다. 두 값을 모두 기록한다.

### 원인을 가르는 방법

**부하 중 vs 유휴를 같은 스택에서 비교한다.** 다만 이 비교로 얻은 차이를 전부 코어 경합
몫으로 귀속하면 안 된다 — 부하는 CPU 경합만 바꾸지 않는다. 요청 처리 작업량, DB I/O,
할당과 GC, 큐 적체, 그리고 부하가 만드는 시계열까지 함께 달라진다. 실제로 부하 구간은
페이로드도 더 컸다.

그래서 이 비교가 말해 주는 것은 "부하 중에 scrape 가 얼마나 나빠지는가" 까지다.
그 안에서 **CPU 예산 몫만** 떼려면 대조군이 하나 더 필요하다 — 같은 부하·같은 페이로드에서
CPU 쿼터만 바꿔 한 벌 더 잰다. 그걸 안 했으면 안 갈랐다고 적는다.

같은 회차 안에 Prometheus를 두 벌 띄워 페이로드 A/B를 하지 않는다 — 코어가 빠듯한 환경에서는
두 번째 Prometheus 자신이 코어를 다투어, 재려는 그 꼬리를 측정 도구가 만든다. 페이로드 A/B가
꼭 필요하면 부하 회차가 끝난 유휴 구간에서 정적 파일로만 한다.

### 안 잰 것은 안 쟀다고 적는다

추정으로 채우면 다음 사람이 그것을 실측으로 읽는다. 배포 환경의 `.env`는 비추적이므로 거기
값을 고쳐 조건을 맞췄다면 그 사실도 함께 적는다 — 저장소에는 `.env.example`만 남는다.
