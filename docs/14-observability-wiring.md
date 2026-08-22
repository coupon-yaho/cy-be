# 14. 알림을 mock 리시버로 보낸다 (CY-359)

**규칙 파일은 있는데 읽는 프로세스가 없다.** 지금 `infra/prometheus/rules/batch-alerts.yml`
에 알림이 **6개** 있는데 저장소에 compose 파일은 **0개**다. CY-347 이 만든
`ExpireLeavesWorkBehind`·`ExpireSkippingBrokenCoupons`·`ExpireMetricsUnknown` 셋은
**아무도 받을 수 없는 상태**다 — 만료가 조용히 멈춰도 알 방법이 없다.

`docs/13` §2e 가 이것을 *"위 전부의 선행 조건"* 으로 세워 뒀다.

---

## 무엇을 하고 무엇을 안 하나

| | |
|---|---|
| **한다** | Prometheus·Alertmanager·mock 리시버를 compose 로 띄운다 |
| **한다** | 규칙이 **실제로 로드됐는지** 확인한다 (`/api/v1/rules`) |
| **한다** | `severity` 로 경로를 가른다 — critical(서버) / warning(데이터) |
| **한다** | 검증 판정을 지표로 낸다 (`docs/13` §2a, *"가장 큰 구멍"*) |
| **안 한다** | Slack 연동 (`docs/13` §2e 에 근거) |
| **한다** | `batch` 를 컨테이너로 띄운다 — **아래 "왜 앱까지 하나"** |
| **한다** | 기동 가드와 업무 포트 결정 — 세 문서가 이 티켓에 예약해 뒀다 |
| **안 한다** | 아직 없는 잡(`cleanupJob`·회차 생성/전이)의 알림 — 그 잡을 만들 때 단다 |
| **안 한다** | `api` 컨테이너 — 스크레이프 대상이 아니다. Flyway 순서만 가드로 다룬다 |

> **범용인 것은 `BatchJobFailed` 하나뿐이다.** 셀렉터에 잡 이름이 없어 새 잡이 Spring Batch
> 잡이기만 하면 그날 바로 커버된다. **나머지 둘은 `expireJob` 에 박혀 있다** —
> `BatchJobNotRunning` 은 `sum by (spring_batch_job_name)` 으로 접기는 하지만 셀렉터 셋이
> 전부 `{spring_batch_job_name="expireJob"}` 이고, `BatchJobRunningTooLong` 도 같다.
> `by` 는 접기이지 셀렉터가 아니다.
>
> 그리고 **`verifyJob` 에는 `BatchJobNotRunning` 을 붙이면 안 된다.** 크론이 없고,
> `rejectRunningSchedulers` 가 스케줄러 켜진 상태의 실행을 거부하므로 <b>"안 도는 것이
> 정상"</b>이다. 붙이면 영구 critical 이다. 대신 마지막 판정의 나이로 본다(4단계).

---

## 왜 앱까지 하나

처음에는 앱 컨테이너화를 "안 한다" 로 뺐다. **틀린 선이었다.**

- `prometheus.yml` 의 스크레이프 대상이 `batch:9092` 다. `batch` 는 DNS 이름이고 그 이름을
  만드는 것은 compose 서비스뿐이다. 앱이 호스트 JVM 이면 컨테이너에서 해석이 안 된다.
- `application.yml.example` 이 이미 그렇게 적어 뒀다 — *"관제의 스크레이프 대상은
  **컨테이너 기준** `batch:9092` 다 — 내부 네트워크에서 긁는다"*.
- 타깃이 DOWN 이면 `absent_over_time(...)` 이 참이 되어 **`BatchJobNotRunning` critical 이
  영구히 걸린 채 스택이 산다.** 그 상태로는 3·4단계에서 넣을 지표가 <b>한 번도
  스크레이프되지 않아</b> 알림이 뜨는 것을 이 티켓 안에서 확인할 수 없다.

그리고 **세 문서가 이 티켓을 가리키고 있다** — `application.yml.example`(기동 가드),
`docs/11`(같은 가드를 `ApplicationRunner` 로), `docs/13` §3(업무 포트 노출). 그것을 안 하면
예약된 일이 티켓 사이로 떨어진다.

> `Dockerfile` 은 저장소에 **아직 없다.** 이 티켓이 만든다.

## compose 파일은 `base.yml` + `batch.yml` 이다

`compose.yml` 이라는 새 이름을 쓰면 안 된다. `docs/11` 이 이미 분할을 정해 뒀고,
**k6 측정 프로토콜이 그 분할에 기대고 있다** —

```bash
docker compose -f base.yml up                     # batch 를 아예 안 띄운다
docker compose -f base.yml -f batch.yml up batch  # 부하 종료 후 겹쳐 올린다
```

*"`base.yml` 은 한 글자도 안 바뀌므로 비교표의 동일 리소스 limit 이 유지된다"* 가 그 근거다.

**관제는 `base.yml` 에 둔다.** 부하 중(batch 미기동)에는 `BatchJobNotRunning` 이 뜨는데,
그것이 정상임을 alertmanager `inhibit_rules` 나 silence 로 다룬다 — 4단계에서 정한다.

---

## 단계

각 단계는 **그 단계만으로 검증되고 커밋된다.** 앞 단계가 서야 뒤 단계를 실제로 뜨는 걸
보며 만들 수 있다 — 그것이 한 티켓에 묶은 이유다.

### 1단계 — 스택을 띄운다

`base.yml` 에 관제 셋을 올린다. 서비스 이름은 **`prometheus.yml` 이 이미 정해 놨다.**

| 서비스 | 마운트 / 근거 |
|---|---|
| `prometheus` | `infra/prometheus/` → `/etc/prometheus` (`rule_files: "rules/*.yml"` 이 config 디렉터리 기준으로 풀린다) |
| `alertmanager` | `infra/alertmanager/alertmanager.yml` → `/etc/alertmanager/` — `prometheus.yml` 의 `alerting` 이 `alertmanager:9093` 을 가리킨다 |
| `alert-sink` | 받은 알림을 stdout 으로 남기는 mock 리시버. `docker compose logs alert-sink` 로 확인한다 |

> **`infra/prometheus/rules/batch-alerts.yml` 경로는 옮기지 마라.**
> `BatchMetricExposureTest` 가 그 경로를 하드코딩해 규칙 파일과 실제 노출을 잇고 있다.

스크레이프 대상은 `batch:9092` — 서비스 포트가 아니라 `BATCH_MANAGEMENT_PORT` 다.

**검증**
- `promtool check config` 가 통과한다 (`rule_files` 글롭을 따라가 규칙까지 함께 본다)
- 규칙이 **로드됐는지**를 파일 존재가 아니라 `/api/v1/rules` 응답으로 확인한다
- **타깃이 `up` 인지** `/api/v1/targets` 로 확인한다 — 이게 없으면 위 둘이 초록이어도
  지표가 하나도 안 들어와 3·4단계가 통째로 헛돈다. 한 계층 위에서 같은 거짓 초록이 난다
- **CI 에도 건다.** 지금 규칙 파일을 읽는 자동 검사는 `BatchMetricExposureTest` 의 정규식이
  전부라 <b>YAML 도 PromQL 도 파싱하지 않는다.</b> 괄호 하나 틀리면 Prometheus 가 파일
  전체를 거부해 알림 6개가 통째로 사라지는데 PR 은 초록이다

### 2단계 — 알림이 리시버까지 간다

**`severity` 로는 안 갈린다.** 실제 라벨을 보면 warning 셋 중 데이터 축은
`ExpireSkippingBrokenCoupons` 하나뿐이다 — `BatchJobRunningTooLong` 은 성능,
`ExpireMetricsUnknown` 은 설정·서버 축이다. severity 로 라우팅하면 <b>warning 수신함에
서버 문제 둘과 데이터 문제 하나가 섞인다.</b> CY-347 이 실제로 세운 축은 severity 가
아니라 알림 이름과 annotation 본문이었다.

그래서 **라우팅 축을 따로 만든다.** 각 규칙에 `channel: server | data` 를 달고
alertmanager 가 그것으로 가른다. `severity` 는 긴급도로 남긴다.

| 알림 | channel |
|---|---|
| `BatchJobFailed` · `BatchJobNotRunning` · `BatchJobRunningTooLong` | `server` |
| `ExpireLeavesWorkBehind` · `ExpireMetricsUnknown` | `server` |
| `ExpireSkippingBrokenCoupons` | `data` |

**검증** — 양쪽 경로를 태워 리시버가 받은 것을 확인한다. 발화 수단은 임시 스모크 규칙을
**별 파일**(`infra/prometheus/rules/smoke.yml`)에 두고 확인 후 지운다.

> `batch-alerts.yml` 에 스모크 규칙을 넣지 마라 — `BatchMetricExposureTest` 가 그 파일에서
> `cy_*`·`spring_batch_*` 이름을 뽑아 노출 단언에 넣으므로 가짜 지표명을 쓰면 CI 가 깨진다.

### 1·2단계 실측 (2026-08-22)

문서의 주장이 아니라 **띄워서 확인한 것**이다.

| 확인 | 방법 | 결과 |
|---|---|---|
| 규칙 로드 | `/api/v1/rules` | 6개 전부, `channel` 라벨 포함 |
| 타깃 (batch 없이) | `/api/v1/targets` | `down` — `lookup batch` 실패 |
| 타깃 (batch 띄운 뒤) | 같은 것 | `up` |
| 지표 도달 | `cy_expire_unexplained_pending` 조회 | `NaN` — 잡이 한 번도 안 돌았다는 뜻이 관제에 그대로 보인다 |
| 라우팅 | `amtool alert add` 셋 | `[server]` · `[data]` · `[unrouted]` 로 갈림 |
| 실제 알림 | 규칙 상태 | `BatchJobNotRunning`·`ExpireMetricsUnknown` 이 `pending` |

> **타깃 DOWN 이 추정이 아니라 관측이다.** batch 를 안 띄운 상태에서 실제로
> `dial tcp: lookup batch` 가 났다 — 앱 컨테이너화가 이 티켓에 필요한 이유가 확정됐다.

> **`channel` 을 빠뜨린 알림은 `unrouted` 로 잡힌다.** 조용히 사라지지 않는다.

**여기서 새로 알게 된 것** — `BATCH_SCHEDULING_ENABLED=false`(batch.yml 의 기본)로 띄우면
`BatchJobNotRunning` 이 15분 뒤 발화한다. 만료가 안 도는 것이 그 설정에서는 정상인데
알림은 사고로 본다. 4단계에서 `inhibit_rules` 로 다룰 대상이 하나 더 늘었다.

### 3단계 — 검증 판정을 지표로 낸다

```
cy_verification_verdict{dataset,scope}    PASS=0, FAIL=1
cy_verification_findings{dataset,scope}   검출 건수
```

**값을 `afterJob` 이 아니라 주기 조회로 채운다.** `expireJob` 은 5분 크론이라 재시작해도
곧 복구되지만 `verifyJob` 은 **사람이 손으로, 드물게** 돌린다. 프로세스 게이지로 두면
컨테이너를 재배포하는 순간 판정이 사라지는데 `verification_runs.verdict` 는 DB 에 남아 있다 —
**관제와 진실이 갈린다.** 금요일 FAIL 이 주말 재시작으로 없어지는 모양이다.

```
@Scheduled(fixedDelayString = "${batch.verify.metrics-refresh-ms:60000}")
  → verification_runs 의 (dataset, scope) 별 최신 닫힌 행을 읽어 게이지를 갱신
```

재시작해도 1분이면 복구되고, `NaN` 은 <i>"그 조합으로 닫힌 실행이 아예 없다"</i> 라는
정확한 뜻이 된다.

**게이지는 eager 등록하되 `FULL` 조합만 만든다.** lazy 로 두면 `verifyJob` 이 안 돈
컨텍스트에 시계열이 없어 `NaN` 이 아니라 `absent` 가 되고, `!= itself` 알림이 안 먹는다.
`INCREMENTAL` 은 `rejectUnsupportedScope` 가 막고 있어 만들면 **영원히 `NaN`** 이라
집계에 전염된다.

**리스너/조회는 `runId` 없이도 동작해야 한다.** 키는 `VerifyJobConfig.RUN_ID_KEY`(값은
`"runId"`)이고, `startRunStep` 이 **가드 여덟을 전부 통과한 뒤에야** 심는다 —
`runId` 가 없는 실행이 곧 <i>판정을 못 낸 실행</i>이다. 라벨은 `JobParameters` 에서 뽑는다.

**CY-347 에서 값을 치른 것 셋을 그대로 지킨다** — 자세한 근거는 `docs/13` §2a.

1. 판정을 못 낸 실행은 `0`(=PASS)이 아니라 **`NaN`(모름)**
2. 두 지표를 따로 `set` 하지 않는다 — 한 스냅샷
3. 알림 식에서 계산하지 않는다

**`verdict` 를 0/1 로 인코딩하는 것은 값이 둘뿐이라 성립한다.** 셋째가 생기면 규칙 파일을
안 고쳐도 뜻이 바뀌므로, 매핑을 한 곳에 모으고 `VerdictType.values().length == 2` 를
단언하는 가드 테스트를 함께 둔다.

### 4단계 — 알림 둘을 더한다

| 알림 | 조건 | channel | 대응 |
|---|---|---|---|
| `VerificationVerdictFailed` | `cy_verification_verdict == 1` | `data` | 판정은 났고 불일치가 있다 |
| `VerificationMetricsUnknown` | `cy_verification_verdict != itself` | `server` | 판정을 지표로 못 내고 있다 |

**`VerificationCannotJudge` 를 "잡 `FAILED`" 로 정의하면 안 된다.** Step 체인이
`finalizeRunStep → statsAggregateStep` 이라, 통계 Step 이 죽으면 **잡은 `FAILED` 인데
verdict 는 이미 커밋돼 있다.** 그때 <i>"판정을 못 냈다"</i> 는 거짓이고, 실제 조치는
*"쓰기를 멈추고 다시 실행"* 이다. 반대로 `verdict = FAIL` 인 실행은 잡이 `COMPLETED` 다 —
**두 축이 서로 독립인데 한 축으로 매핑하면 오진한다.** 그래서 잡 상태가 아니라
**지표**로 본다.

`NaN` 감시(`VerificationMetricsUnknown`)가 반드시 필요하다. `verifyJob` 은 크론이 없어
"안 돌았다" 축이 없고, 지표가 `NaN` 이면 **어떤 알림도 안 뜬다** — CY-347 이
`ExpireMetricsUnknown` 으로 값을 치른 그 자리다.

`verifyJob` 의 `BatchJobRunningTooLong` 임계를 여기서 정한다 — 300만 전수라 만료와 소요가
다르다. 부하 중(`batch` 미기동) `BatchJobNotRunning` 을 어떻게 다룰지도 여기서 정한다.

---

## 검증 계약

- **규칙 로드** — 파일이 있는 것이 아니라 Prometheus 가 **읽었다**는 것을 응답으로 확인
- **경로 분기** — critical 과 warning 이 리시버에서 갈리는 것을 확인
- **지표 이름** — `BatchMetricExposureTest` 가 규칙 파일에서 이름을 뽑아 `/actuator/prometheus`
  본문과 대조한다. 다만 그것이 보는 것은 **이름뿐**이다 — 리스너를 안 걸어도, 값이 늘
  `NaN` 이어도 초록이다. **값이 실제로 설정되는지는 `VerifyJob*Test` 가 따로 본다**
- **모름 축** — 판정 없는 실행이 `0` 이 아니라 `NaN` 인 것. `startRunStep` 을 실패시켜(예:
  `seedRunId` 누락) 확인한다
- **배선 가드** — `verifyJob` 에 판정 리스너가 실제로 걸려 있는 것

## 안 하기로 한 것과 이유

| 무엇 | 왜 |
|---|---|
| Slack | `docs/13` §2e — PRD 제약(외부 연동 Mocking) + PUBLIC 저장소 |
| Grafana | 우리 대시보드는 Grafana 가 아니다 — PRD 상 Chart.js 폴링 커스텀이고 영역 ⑤ 몫이다 |
| `api` 컨테이너 | 스크레이프 대상이 아니다. 다만 **Flyway 순서**는 다룬다 — `batch` 가 먼저 뜨면 "테이블 없음" SQL 에러로 죽고 스택트레이스가 SQL 계층이라 *"검증 배치가 깨졌다"* 로 읽힌다 |

## 시연 절차 — 한 기동으로는 둘 다 못 본다

`batch.scheduling.enabled` 가 두 잡에 **정반대**를 요구한다. `true` 면 만료가 5분마다 돌지만
`rejectRunningSchedulers` 가 `verifyJob` 을 거부하고, `false` 면 그 반대다.

1. `BATCH_SCHEDULING_ENABLED=true` — 만료 알림 축 확인
2. `false` 로 재기동 + `verifyJob` 트리거 — 검증 알림 축 확인

2 구간에서 만료 쪽 critical 이 배경에 깔리는 것이 정상이다. 리시버 로그에서 방금 넣은 알림을
눈으로 가를 수 있게 `inhibit_rules` 로 억제할지 4단계에서 정한다.
