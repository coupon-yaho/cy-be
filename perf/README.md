# 측정 하네스 (CY-759)

회차를 **누구나 같은 방식으로 재현**하기 위한 스크립트와 절차다. 지금까지는 회차마다
사람이 손으로 환경을 만들었고, 그래서 두 회차의 수치를 나란히 놓을 수 없었다.

프로덕션 코드는 건드리지 않는다. `compose.yml` · `infra/prometheus/prometheus.yml` 도
고치지 않고, `perf/env/compose.perf.yml` 을 **겹쳐서만** 회차 환경이 된다.

```
perf/
  env/     ① 환경 — override · nginx · prometheus · 기동/점검/메타
  seed/    ② 시드 — 회원 · 더미 발급 · 회차 생성
  k6/      ③ 부하 — 워밍업 + 측정, 커스텀 지표
  run/     ④ 회차 실행 · 반복 · 결과 정리
  results/ 결과 (커밋하지 않는다)
```

---

## 0. 수치를 읽고 쓰는 규칙

이 절을 먼저 읽지 않으면 나머지가 위험하다.

### 설정값과 달성치를 구분한다

k6 에 넣은 도착률은 **설정값**이고, 실제로 나간 것이 **달성치**다. 둘은 늘 다르다.
이 하네스의 표는 두 열을 나란히 낸다(`설정rps` · `달성rps`).

> **`http_reqs.rate` 를 달성치로 쓰지 않는다.** 내장 `http_reqs` 는 워밍업 시나리오까지
> 합산하고, 그 `rate` 는 "워밍업 + 대기 + 측정" **전체 실행 시간**으로 나눈 값이다.
> 실측: 설정 800/s × 5s 회차에서 `http_reqs.rate` 가 **185/s** 로 나왔는데
> (워밍업 1,000건 + 측정 4,001건) ÷ 27초였다. 달성치는 결과 JSON 의
> `perf.achieved_arrival_rps` — **측정 구간에 실제로 쏜 요청 ÷ 측정 구간 길이**다.
> 못 쏜 것은 시간에 늘어지지 않고 `dropped_iterations` 로 빠지므로 이 나눗셈이 맞다.

> 돌아다니는 **"1120/s"** 라는 숫자는 **어느 결과 파일에도 없다.** k6 에 넣은 도착률
> 설정으로 보인다. 인용하려면 어느 결과의 어느 필드인지를 같이 적는다.

### 과거 회차를 인용할 때

cy-631 회차(`~/Downloads/cy-631-results/*.json`)의 통과 요청 **16,777** 은 macOS 임시 포트
범위 **16,384** 와 거의 같다. **서버 처리량이 아니라 클라이언트 한계다.** 그 회차는
"요청 2만" 이 아니라 실질 1.68만이었고, 달성 rps 는 348~557 이다. 그 회차의 수치를
용량 근거로 쓰지 않는다.

### 안 잰 것은 안 쟀다고 적는다

가져오기 실패와 "실제로 0" 은 다르다. 이 하네스의 `promq` 는 실패하면 값을 내지 않고
0 이 아닌 코드로 끝나고, k6 요약도 **한 번도 안 오른 Counter(=0)** 와 **표본이 없어
분포를 못 만든 Trend(=측정 실패)** 를 다른 칸에 찍는다. 추정으로 채우면 다음 사람이
그것을 실측으로 읽는다.

---

## 1. 3대 구성

| | 무엇 | 왜 |
|---|---|---|
| **A** 윈도우 PC 16GB | MySQL · Redis · Prometheus · api×4 · nginx | 발급 경로 전부 |
| **B** 맥북 18GB | k6 (네이티브. 도커 쓰지 마라) | 무선. 실제 사용자 환경과 같다 |
| **C** 윈도우 노트북 | 프론트(cy-fe) + 브라우저 | 화면 확인 |

**발급 1건이 api→MySQL 왕복을 3~4회 낸다.** 그 구간이 무선을 타면 재는 것이 v1/v2 차이가
아니라 와이파이 품질이 된다. 그래서 발급 경로를 전부 A 에 몬다. Prometheus 도 A 다 —
관리자 API 의 예산이 connect 100ms · read 300ms · total 500ms 라 무선을 건너면 넘긴다.

주의할 것:

- `docker compose up --scale api=4` 를 그냥 하면 포트 충돌이다. override 가 api 의
  `ports` 를 비우고 nginx 를 앞에 세운다.
- 관리 포트 9090 은 호스트에 안 열린다(의도 — actuator 에 인증이 없다). A 안에서만 접근한다.
- 윈도우 방화벽 인바운드를 열어도 **네트워크 프로필이 "공용" 이면 규칙이 막힌다.**
- 회차마다 B→A ping 200회를 재서 결과에 같이 남긴다. `preflight.sh` 가 자동으로 하고,
  **지터가 크면 그 회차는 버린다.**

### 스크립트는 어디서 도나

`perf/run/*` 은 **B(부하기)** 에서 돈다. A 의 도커를 만져야 하는 부분은 `PERF_A_SSH` 로
넘긴다(비어 있으면 로컬 도커를 본다).

```
PERF_A_SSH=user@192.168.0.20
PERF_A_REPO=/home/user/coupon-yaho
```

> 검증 상태: **A=B 인 단일 호스트(맥북) 경로만 실측으로 돌렸다.** `PERF_A_SSH` 경로와
> 윈도우 A 는 아직 안 돌려 봤다 — A 쪽에 bash·ssh 가 필요하다(WSL 또는 Git Bash).

---

## 2. 절차

### ① 환경 기동 — A

```bash
cp .env.example .env                      # 비밀값을 채운다
cp application.yml.example application.yml
cp perf/env/perf.env.example perf/env/perf.env
$EDITOR perf/env/perf.env

perf/env/up.sh
```

`up.sh` 가 하는 것:

1. `perf/env/build-images.sh` — api·batch 를 **커밋 SHA 태그**로 빌드한다.
   > **`./gradlew bootRun` 으로 재지 않는다.** bootRun 은 `-XX:TieredStopAtLevel=1` 로 떠서
   > C2 JIT 이 꺼져 있다. 같은 구간에서 **305/s vs 400/s — 약 30% 낮게** 나왔다(실측).
2. mysql · redis → 런타임 설정 시드(`config:runtime`) → api×N · batch · nginx · prometheus
3. api·batch 의 actuator 가 응답할 때까지 대기.
   > compose 의 `--wait` 은 healthcheck 가 있는 서비스에만 쓸모가 있고 api·batch 에는 없다.
   > **batch 기동에 실측 114초가 걸렸다.** 그 사이 워밍업 호출은 Connection refused 로 죽는다.
4. 관측 계정 GRANT (Flyway 뒤에만 된다 — 테이블 단위 GRANT 라 테이블이 먼저 있어야 한다)

### ② 점검 — 회차마다

```bash
perf/env/preflight.sh
```

하나라도 ✗ 면 회차를 열지 않는다. 막는 항목은 전부 실제로 밟아 회차를 버린 것들이다.

| 항목 | 왜 |
|---|---|
| Prometheus api 타깃 수 == 대수 | 저장소 기본 설정은 `static_configs: ['api:9090']` 하나라 **4대 중 한 대만 긁는다.** override 의 `dns_sd_configs` 가 안 물리면 여기서 걸린다 |
| 톰캣 수용 ≥ 요청 수 | 기본 `max-connections 4000` + `accept-count 1000` = **5,000 상한.** VU 2만을 쏘면 그 위는 연결이 아예 안 되고 **응답이 아니라 `duration 0s`** 로 찍힌다 |
| `-Xmx` 명시 | 컨테이너 메모리의 25% 가 기본이다. 3g 제한이면 힙 768MB — **이것 때문에 api 가 OOMKilled 로 세 번 죽었다** |
| ping 손실·지터 | B→A 무선 구간의 품질. 지터가 크면 그 회차는 버린다 |
| 임시 포트 수 ≥ 요청 수 | macOS 기본 49152~65535 = **16,384개.** 앞 회차의 TIME_WAIT 가 다음 회차의 가용 포트를 깎는다 |

임시 포트가 모자라면 B 에서:

```bash
sudo sysctl -w net.inet.ip.portrange.first=16384
sudo sysctl -w net.inet.tcp.msl=1000
```

### ③ 시드 — 한 번

```bash
perf/seed/seed.sh
```

멱등이다. 모자란 만큼만 채운다. 밟은 함정들이 스크립트에 박혀 있다.

- `policy_type` 은 `'PERCENT_CAPPED' | 'FIXED_AMOUNT' | 'DATA_GRANT'` 다. `'RATE'` 같은 값을
  넣으면 **발급이 500 으로 죽는다**(enum 변환 실패). 실패가 시드가 아니라 발급에서 나므로
  원인이 안 보인다.
- `coupon_templates` 에는 `min_order_amount` 가 **없다**(V2 가 지웠다). `coupons` 에는 있다.
- `issuances.code` 는 `char(16) UNIQUE` 다. 더미 30만 행을 랜덤 코드로 만들면 충돌이 나고
  충돌은 INSERT 전체를 되돌린다. `CONCAT(LPAD(id,10,'0'), LPAD(seq,6,'0'))` 로 **결정적으로** 만든다.
- 회원 수는 **한 구간의 요청 수 이상**이어야 한다. 1인 1매가 회차 단위라 요청 수만큼 서로
  다른 회원이 필요하다. `run-round.sh` 가 시작 전에 확인하고 모자라면 멈춘다.
- `issuances` 에 `members` FK 가 있다. **없는 memberId 를 주면 전부 500 이다** — 응답만 보면
  서버 결함처럼 보인다.

### ④ 회차 — 반복

```bash
perf/run/run-repeat.sh --engine V2 --profile spike
perf/run/run-repeat.sh --engine V1 --profile spike
```

`--engine` 만 다르다. **v1 회차와 v2 회차를 같은 스크립트로 잰다** — 회차의
`issuance_engine_version` 만 바뀐다.

프로필 둘:

- `spike` — 재고 1만 · 요청 2만 · 도착 1~3초. 발표 시연용
- `steps` — `PERF_STEP_RATES` 의 단계마다 **회차를 새로 만들어** 올린다.
  한 회차로 계단을 올리면 첫 단계에서 재고가 다 나가고 이후는 전부 매진 거절이라
  발급 경로를 안 탄다.

한 회차의 실행 순서:

```
워밍업 회차 생성(재고 크게) → [V2면 warmup] → 열기
측정 회차 생성            → [V2면 warmup] → 열기
k6:  워밍업 구간(300/s x 25s)  →  측정 구간
결과 + 환경 메타 + 회차 사후 상태 저장
```

> **워밍업을 계단 앞에 두는 근거는 실측이다.** 버퍼풀·JIT 가 지연 꼬리의 대부분이었다.
> 300/s × 25초를 앞에 두니 med 가 **24ms → 4ms**, p95 가 **861ms → 12ms** 로 떨어졌다.
>
> 워밍업 회차를 **따로** 두는 이유 — 워밍업 트래픽도 발급이라 같은 회차를 쓰면 재고를 먹는다.

> **같은 조건을 최소 5회 반복한다.** 같은 이미지·같은 300/s 를 세 번 쟀는데 med 가
> **3ms · 82ms · 224ms** 로 흔들렸다. 표본 하나로 두 변형을 비교하면 잡음과 차이를
> 구분할 수 없다. 비교는 **중앙값**으로 한다. `PERF_REPEATS` 기본값이 5 이고, 5회 미만이면
> 요약표가 그 사실을 그 줄에 적는다.

### ⑤ 결과 정리

`run-repeat.sh` 가 끝에 자동으로 부르고, 따로도 부른다.

```bash
python3 perf/run/summarize.py perf/results/<run-id>
python3 perf/run/summarize.py --compare perf/results/<v1-run> perf/results/<v2-run>
```

---

## 3. 결과에 무엇이 남나

```
perf/results/<run-id>/
  preflight.log
  rate-<도착률>/rep-<n>/
    k6-summary.json   k6 원본 요약
    k6.log
    round.json        회차 id · 창 시각 · DB 사후 상태 · scrape 건강도
    meta.json         환경 메타
  summary.txt
```

`meta.json` 이 없으면 회차 간 비교가 불가능하다. 들어 있는 것:

- 커밋 SHA · 브랜치 · **미커밋 파일 수**(더러운 워크트리로 빌드하면 태그의 SHA 가 이미지
  내용을 안 가리킨다)
- 이미지 태그와 image id
- 선언 대수 · 실제 컨테이너 수 · **Prometheus api 타깃 수**
- 톰캣 `max-connections`/`accept-count`/`threads`, 인스턴스당 풀 크기, `JAVA_TOOL_OPTIONS`
  (선언값 — actuator 의 `env`·`configprops` 는 비밀값 유출 방지로 노출에서 제외돼 있어 못 읽는다)
- 실측 Hikari max 합계 · JVM heap max · `system_cpu_count`
  (합계에는 운영 풀과 관측 풀이 함께 들어 있다. 관측 쿼리가 운영 풀을 점유하지 않게
  분리해 뒀기 때문에 `인스턴스당 × 대수` 와 다른 것이 정상이다)
- MySQL `max_connections` · 버전
- k6 버전 · **임시 포트 범위와 개수** · `tcp.msl` · 회차 직전 TIME_WAIT 수
- **B→A ping 200회** — 손실·min/avg/max/stddev

## 4. 지표 — 성공과 거절을 반드시 나눈다

섞은 단일 p99 는 **매진 1만 건이 분포를 끌어내려 실제보다 좋아 보인다.** 실측 한 줄:

```
성공 med 64.31ms · p99 197.08ms      매진 med 0.77ms · p99 3.14ms
```

같은 회차의 두 분포다. 섞으면 어느 쪽도 아닌 숫자가 나온다.

| 지표 | 뜻 |
|---|---|
| `issue_successes` · `issue_rejections` · `issue_errors` | 건수. 거절은 `code` 라벨로 나뉜다 |
| `issue_success_duration` | 성공 분포 |
| `issue_rejected_sold_out_duration` | 매진 거절 분포 |
| `issue_rejected_other_duration` | 매진 외 거절 분포 |
| `issue_attempts` → `perf.achieved_arrival_rps` | **달성** 도착률(측정 구간만) |
| `http_reqs.rate` | 워밍업 포함 **전체 실행 평균.** 달성치가 아니다 |
| `dropped_iterations` | 못 쏜 것 |
| `issue_connect_failures` | **`duration 0s` 인 실패 — 응답이 아니라 연결 실패다.** 따로 센다 |

회차 사후 상태(`round.json`)로 불변식도 함께 본다 — 초과 발급 0 · 1인 2매 0.

## 5. 밟은 함정

| | |
|---|---|
| `.env` 를 `source` 하면 깨진다 | `COUPON_IMAGE=<dockerhub-user>/...` 의 `<` 를 셸이 리다이렉션으로 읽는다. 이 하네스는 `while read` 로 읽는다 |
| `application.yml` 없이 `up` | Docker 가 그 이름의 **디렉터리**를 만들어 마운트한다. 설정이 통째로 비는데 에러에 원인이 안 나온다 |
| `pull_policy: always` | 회차 이미지는 로컬 빌드 태그라 레지스트리에 없다. override 가 `missing` 으로 덮는다 |
| batch 의 Redis | `compose.yml` 은 `REDIS_HOST` 를 **api 에만** 준다. batch 는 localhost 를 보고 워밍업이 500 으로 죽는다. override 가 채운다 |
| 등급 헤더 | 회차의 `eligible_grades_mask` 와 맞아야 한다. 한 회차는 **한 등급만** 쏜다(마스크가 허용하는 가장 높은 등급). 발급 경로에서 등급은 `CouponIssuePolicy` 의 비트마스크 `contains` 검사 하나에만 쓰이고 등급별 분기가 없어서 지연 편향이 없다 — **분기가 생기면 이 선택을 다시 봐야 한다.** 실제로 쏜 등급은 `round.json` 의 `member_grade` 에 남는다. k6 에 하드코딩하면 마스크를 바꾼 회차에서 전량 등급 거절이 나고 결과에는 "거절 N건" 으로만 보인다. `run-round.sh` 가 회차에서 뽑아 넘기고, 없으면 k6 가 시작 전에 죽는다 |
| `Idempotency-Key` | **UUID v4 형식을 강제한다.** 아니면 발급 경로를 타기도 전에 COUPON-300 으로 전량 거절되고, 결과에는 "거절 N건" 으로만 보인다 |
| 워밍업은 열리기 전에만 | 열린 뒤에는 `ROUND_ALREADY_OPENED`. **열린 회차의 Redis 키가 유실되면 다시 만들 통로가 없다** |
| `coupons` 의 `uk_template_open` | 회차를 여럿 만들려면 `open_at` 이 서로 달라야 한다. `new-round.sh` 가 단조 증가시킨다 |

## 6. 이 하네스가 하지 않는 것

- 프로덕션 코드 수정
- S9 경합 테스트 (CY-758)
- `config:runtime` 부트스트랩 · 게이트 재구성 · Sentinel
- 조회 부하 — 지금은 발급만 쏜다. `docs/12` §10.2 의 혼합 시나리오는 다음 작업이다
