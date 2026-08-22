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
| **안 한다** | 아직 없는 잡(`cleanupJob`·회차 생성/전이)의 알림 — 그 잡을 만들 때 단다 |

> **범용 알림 셋은 잡 이름을 안 박는다.** `BatchJobFailed`·`BatchJobNotRunning` 은
> `by (spring_batch_job_name)` 으로 묶여 있어, 새 잡이 Spring Batch 잡이기만 하면 그날
> 바로 커버된다. 예외는 `BatchJobRunningTooLong` 하나 —
> `{spring_batch_job_name="expireJob"}` 로 박혀 있다. 잡마다 정상 소요가 달라 임계를
> 따로 줘야 하기 때문이고, `verifyJob` 몫은 4단계에서 정한다.

---

## 단계

각 단계는 **그 단계만으로 검증되고 커밋된다.** 앞 단계가 서야 뒤 단계를 실제로 뜨는 걸
보며 만들 수 있다 — 그것이 한 티켓에 묶은 이유다.

### 1단계 — 스택을 띄운다

`compose.yml` 에 셋을 올린다. 서비스 이름은 **`prometheus.yml` 이 이미 정해 놨다.**

```
prometheus     rules/ 와 prometheus.yml 을 /etc/prometheus 로 마운트
alertmanager   prometheus.yml 의 alerting.alertmanagers 가 alertmanager:9093 을 가리킨다
alert-sink     받은 알림을 그대로 남기는 mock 리시버
```

스크레이프 대상은 `batch:9092` 다 — 서비스 포트가 아니라 `BATCH_MANAGEMENT_PORT` 다.

**검증**
- `promtool check config` / `check rules` 가 통과한다
- 규칙 파일이 **로드됐는지**를 파일 존재가 아니라 `/api/v1/rules` 응답으로 확인한다

### 2단계 — 알림이 리시버까지 간다

`severity` 로 경로를 가른다. 이 티켓의 핵심은 **둘이 서로 다른 경로로 간다**는 것이다 —
`critical` 은 서버를 볼 상황, `warning` 은 데이터를 볼 상황이고, 그 구분이 CY-347 의
설계 원칙이다.

**검증** — 알림을 강제로 발화시켜 리시버가 받은 것을 확인한다. 뜨는 것만 보면 안 되고
**어느 경로로 갔는지**까지 본다.

### 3단계 — 검증 판정을 지표로 낸다

```
cy_verification_verdict{dataset,scope}    PASS=0, FAIL=1
cy_verification_findings{dataset,scope}   검출 건수
```

`verifyJob` 에는 지금 `JobExecutionListener` 가 **하나도 없다**. `runId` 는
`jobExecutionContext[verify.runId]` 에 실려 있어 `afterJob` 에서 되읽을 수 있다.

**CY-347 에서 값을 치른 것 셋을 그대로 지킨다** — 자세한 근거는 `docs/13` §2a.

1. 판정을 못 낸 실행은 `0`(=PASS)이 아니라 **`NaN`(모름)**
2. 두 지표를 따로 `set` 하지 않는다 — 한 스냅샷
3. 알림 식에서 계산하지 않는다

### 4단계 — 알림 둘을 더한다

| 알림 | 조건 | severity | 대응 |
|---|---|---|---|
| `VerificationCannotJudge` | 잡 `FAILED` | critical | 서버를 본다 |
| `VerificationVerdictFailed` | `verdict = FAIL` | warning | 데이터를 본다 |

`verifyJob` 의 `BatchJobRunningTooLong` 임계를 여기서 정한다 — 300만 전수라 만료와 소요가
다르다.

---

## 검증 계약

- **규칙 로드** — 파일이 있는 것이 아니라 Prometheus 가 **읽었다**는 것을 응답으로 확인
- **경로 분기** — critical 과 warning 이 리시버에서 갈리는 것을 확인
- **지표 이름** — `BatchMetricExposureTest` 가 규칙 파일에서 이름을 뽑아 `/actuator/prometheus`
  본문과 대조한다. 새 이름 둘도 그 대조에 자동으로 딸려 온다
- **모름 축** — 판정 없는 실행이 `0` 이 아니라 `NaN` 인 것

## 안 하기로 한 것과 이유

| 무엇 | 왜 |
|---|---|
| Slack | `docs/13` §2e — PRD 제약(외부 연동 Mocking) + PUBLIC 저장소 |
| Grafana | 알림이 나가는 것과 별개다. 대시보드는 그 자체로 티켓이다 |
| 앱(`api`·`batch`) 컨테이너화 | 이 티켓은 **관제 스택**이다. 앱을 어떻게 띄울지는 배포 티켓의 몫이고, 여기서는 스크레이프 대상이 뜬다는 전제만 쓴다 |
