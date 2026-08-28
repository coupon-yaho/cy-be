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
| **한다** | `channel` 라벨로 경로를 가른다 — `server` / `data`. `severity` 는 긴급도로만 남긴다 |
| **한다** | 검증 판정을 지표로 낸다 (`docs/13` §2a, *"가장 큰 구멍"*) |
| **안 한다** | Slack 연동 (`docs/13` §2e 에 근거) |
| **한다** | `batch` 를 컨테이너로 띄운다 — **아래 "왜 앱까지 하나"** |
| **한다** | 기동 가드와 업무 포트 결정 — 세 문서가 이 티켓에 예약해 뒀다 |
| **안 한다** | 아직 없는 잡(회차 **생성**)의 알림 — 그 잡을 만들 때 단다. `cleanupJob` 은 CY-397 이 만들면서 규칙 넷을, **회차 전이는 CY-446 이 만들면서 열을** 함께 달았다 |
| **안 한다** | `api` 컨테이너 — 스크레이프 대상이 아니다. 배포 순서 위반은 `batch` 쪽 기동 가드로 잡는다 |

> **범용인 것은 `BatchJobFailed` 와 `BatchStuckExecution` 둘이다.** 셀렉터에 잡 이름이 없어
> 새 잡이 생기면 그날 바로 커버된다 — 뒤엣것은 CY-392 가 `job` 라벨로 냈고, 지켜보는 잡을
> `Job` 빈에서 모으므로 잡이 늘면 시계열도 따라 는다.
>
> **`ExpireNotSucceeding`·`BatchJobRunningTooLong` 은 `expireJob` 에 박혀 있다.**
> 둘 다 `{spring_batch_job_name="expireJob"}` 로 좁힌다 — 앞엣것은 우리 게이지의 라벨이고
> 뒤엣것은 스프링 배치가 내는 라벨인데, CY-392 가 이름을 맞춰 뒀다.
>
> **그래서 `cleanupJob` 은 자기 판을 따로 갖는다**(CY-397) —
> `CleanupNotSucceeding`(SLA 25h) · `CleanupNeverSucceeded`(NaN) · `CleanupGaugeMissing`(absent)
> · `CleanupRunningTooLong`(900초). **잡 축을 정규식으로 합치지 않았다** — SLA 도 임계도
> 만료와 값이 달라서, 합치면 한쪽을 조정할 때 다른 쪽이 조용히 따라 움직인다.
> 뒤 셋이 만료 판을 그대로 복제한 이유는 비교 필터가 `NaN` 을 떨어뜨리기 때문이다 —
> `CleanupNotSucceeding` 만 두면 *"한 번도 안 돈 정리"* 가 **영구 침묵**이고, 그 상태가
> 정확히 이 잡의 가장 나쁜 결말이다. `promtool` 유닛 테스트가 그 짝을 못 박아 둔다.
>
> **`verifyJob` 도 자기 판을 갖는다(CY-470) — 다만 그레인이 다르다.**
> `VerifyNotSucceeding`(SLA 25h) · `VerifyNeverSucceeded`(NaN) · `VerifyGaugeMissing`(absent)
> · `VerifyRunningTooLong`(1200초). 앞 셋의 셀렉터가 잡 이름이 아니라
> **`cy_verify_last_success_seconds{dataset="CLEAN",scope="FULL"}`** 인 것이 핵심이다 —
> `verifyJob` 하나가 게이트 조합과 리허설(`CORRUPT`)을 함께 도는데, 잡 이름 그레인에
> SLA 를 걸면 <b>리허설 한 번이 시계열을 앞으로 밀어 SLA 를 리셋한다.</b> 정작 게이트
> 조합은 며칠째 안 돌았는데 조용하다. `promtool` 유닛 테스트가 그 상황을 그대로 심어 잰다.
>
> 그전에는 SLA 를 못 걸었다 — 크론이 없어 <b>"안 도는 것이 정상"</b>이었고, 걸면 영구
> critical 이다. CY-392 가 게이지만 미리 내 둔 것은 CY-470 이 <b>감시를 나중에 붙이는
> 상태로 시작하지 않게</b> 하려는 것이었다.
>
> **`VerifyRunningTooLong` 만 잡 이름 그레인이다.** 그 축은 스프링 배치가 내는
> `spring_batch_job_active_seconds_max` 라 `(dataset, scope)` 를 애초에 안 갖는다 —
> "지금 도는 잡이 느리다" 는 조합과 무관한 질문이기도 하다.
>
> **CY-384 전에는 이유가 하나 더 있었다** — `rejectRunningSchedulers` 가 스케줄러 켜진
> 상태의 실행을 아예 거부해, 크론을 붙일 수 있는 조합 자체가 없었다. 그 제약은 풀렸고,
> 검증을 야간 크론으로 옮기는 티켓이 이 알림도 함께 다시 쓴다.

---

## 왜 앱까지 하나

처음에는 앱 컨테이너화를 "안 한다" 로 뺐다. **틀린 선이었다.**

- `prometheus.yml` 의 스크레이프 대상이 `batch:9092` 다. `batch` 는 DNS 이름이고 그 이름을
  만드는 것은 compose 서비스뿐이다. 앱이 호스트 JVM 이면 컨테이너에서 해석이 안 된다.
- `application.yml.example` 이 이미 그렇게 적어 뒀다 — *"관제의 스크레이프 대상은
  **컨테이너 기준** `batch:9092` 다 — 내부 네트워크에서 긁는다"*.
- 타깃이 DOWN 이면 `up{job="cy-batch"}` 가 `0` 이 되어 `BatchTargetDown` 의
  `avg_over_time(up[15m]) < 0.9` 가 참이 되고, **critical 이
  영구히 걸린 채 스택이 산다.** 그 상태로는 3·4단계에서 넣을 지표가 <b>한 번도
  스크레이프되지 않아</b> 알림이 뜨는 것을 이 티켓 안에서 확인할 수 없다.

그리고 **세 문서가 이 티켓을 가리키고 있다** — `application.yml.example`(기동 가드),
`docs/11`(같은 가드를 `ApplicationRunner` 로), `docs/13` §4(업무 포트 노출). 그것을 안 하면
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

**관제는 `base.yml` 에 둔다.** 부하 중(batch 미기동)에는 `BatchTargetDown` 이 뜨는데,
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

이 표가 **대장**이다 — 규칙 파일의 `- alert:` 전부가 여기 있어야 하고,
`AlertChannelRegistryTest` 가 두 집합을 대조한다.

| 알림 | channel |
|---|---|
| `BatchJobFailed` · `ExpireNotSucceeding` · `ExpireNeverSucceeded` | `server` |
| `ExpireGaugeMissing` · `BatchTargetDown` · `BatchJobRunningTooLong` | `server` |
| `BatchStuckExecution` · `BatchRunMetricsUnknown` · `BatchRunMetricsStale` | `server` |
| `ExpireMetricsStale` · `ExpireMetricsBackdated` | `server` |
| `CleanupNotSucceeding` · `CleanupNeverSucceeded` · `CleanupGaugeMissing` | `server` |
| `CleanupRunningTooLong` | `server` |
| `BatchSchemaIndexMissing` | `server` |
| `BatchJdbcTimeoutUnverified` | `server` |
| `BatchDefaultZoneNotUtc` | `server` |
| `VerifyNotSucceeding` · `VerifyNeverSucceeded` · `VerifyGaugeMissing` | `server` |
| `VerifyRunningTooLong` | `server` |
| `ExpireLeavesWorkBehind` · `ExpireMetricsUnknown` | `server` |
| `ExpireMakingNoProgress` · `ExpireFailedWithCode` | `server` |
| `CouponRoundsNotOpening` · `CouponRoundsNotClosing` | `server` |
| `CouponRoundMetricsUnknown` · `CouponRoundMetricsStale` · `CouponRoundMetricsMissing` | `server` |
| `CouponRoundSelectFailing` | `server` |
| `CouponRoundSwitchMetricMissing` | `server` |
| `ExpireSkippingBrokenCoupons` | `data` |
| `CouponRoundBlockedByMissingStock` · `CouponRoundMissedWindow` | `data` |
| `CouponRoundDataMetricsUnknown` | `data` |
| `VerificationVerdictFailed` | `data` |
| `VerificationMetricsUnknown` · `VerificationMetricsStale` | `server` |
| `AlertDeliveryFailing` | `server` |

**검증** — 양쪽 경로를 태워 리시버가 받은 것을 확인한다. 발화 수단은 임시 스모크 규칙을
**별 파일**(`infra/prometheus/rules/smoke.yml`)에 두고 확인 후 지운다.

> `batch-alerts.yml` 에 스모크 규칙을 넣지 마라 — `BatchMetricExposureTest` 가 그 파일에서
> `cy_*`·`spring_batch_*` 이름을 뽑아 노출 단언에 넣으므로 가짜 지표명을 쓰면 CI 가 깨진다.

### 1·2단계 실측 (2026-08-22)

문서의 주장이 아니라 **띄워서 확인한 것**이다.

| 확인 | 방법 | 결과 |
|---|---|---|
| 규칙 로드 | `/api/v1/rules` | 6개 전부, `channel` 라벨 포함 (개수는 `AlertChannelRegistryTest` 가 표와 대조한다 — 여기 적으면 낡는다) |
| 타깃 (batch 없이) | `/api/v1/targets` | `down` — `lookup batch` 실패 |
| 타깃 (batch 띄운 뒤) | 같은 것 | `up` |
| 지표 도달 | `cy_expire_unexplained_pending` 조회 | `NaN` — 잡이 한 번도 안 돌았다는 뜻이 관제에 그대로 보인다 |
| 라우팅 | `amtool alert add` 셋 | `[server]` · `[data]` · `[unrouted]` 로 갈림 |
| 실제 알림 | 규칙 상태 | `BatchJobNotRunning`·`ExpireMetricsUnknown` 이 `pending` |

> ⚠️ **`NaN` 의 뜻이 하나에서 넷이 됐다**(CY-421). 지표가 되읽기로 바뀌면서 —
> 성공한 실행이 7일 창 안에 없거나, 오염 스키마를 보고 있거나, 제외 목록을 못 읽었거나,
> 되읽기가 실패한 것이다. 가르는 순서는 `ExpireMetricsUnknown` 의 description 에 있다.
> 그리고 값이 **0 이어도 어제 것일 수 있다** — `cy_expire_measured_at_seconds` 가
> 그 기준 `asOf` 를 낸다.

> **타깃 DOWN 이 추정이 아니라 관측이다.** batch 를 안 띄운 상태에서 실제로
> `dial tcp: lookup batch` 가 났다 — 앱 컨테이너화가 이 티켓에 필요한 이유가 확정됐다.

> **`channel` 을 빠뜨린 알림은 `unrouted` 로 잡힌다.** 조용히 사라지지 않는다.

**여기서 새로 알게 된 것** — `BATCH_SCHEDULING_ENABLED=false`(batch.yml 의 기본)로 띄우면
그날의 `BatchJobNotRunning`(15분 창 + `for` 15분)이 발화한다. 만료가 안 도는 것이 그
설정에서는 정상인데 알림은 사고로 본다. **4단계에서 silence 로 결정했다** — 규칙에 예외를
파면 *일부러 껐다* 와 *꺼져 버렸다* 가 같은 값이 된다. 시연 절차 **1번**에 명령을 박아 뒀다.

> ⚠️ **이 표는 2026-08-22 의 관측이다.** CY-392 가 그 규칙을 `ExpireNotSucceeding`(마지막
> 성공 시각 축)으로 갈았으므로 위 이름과 발화 지연은 <b>지금 것이 아니다</b>. 새 규칙의
> 발화는 아직 <b>띄워서 확인하지 않았다</b> — 확인하면 아래에 별도 절로 적는다.
> 계산값을 이 표에 써 넣지 않는다. 그러면 다음 사람이 그것을 관측 근거로 인용한다.

### 3·4단계 실측 (2026-08-22)

| 확인 | 결과 |
|---|---|
| 규칙 수 | 6 → **10** (`promtool check config` 통과) |
| 이름↔노출 대조 | `BatchMetricExposureTest` 가 새 이름 **셋**(`cy_verification_verdict`·`cy_verification_findings`·`cy_verification_refresh_failures_total`)을 자동으로 잡는다 — 게이지와 카운터를 미리 등록해서 잡을 안 돌려도 본문에 있다 |
| 값 | `VerificationMetricExposureTest`. PASS=0 · FAIL=1 · 검출 건수 · 안 닫힌 실행 제외 · 판정 없는 행 제외 · **시드 행 제외** · 최신 선택 · 라벨 |
| 돌연변이 | 넷을 심어 **전부 검출** — 모름을 0 으로 · `finished_at` 조건 제거 · 데이터셋 한 칸에 · 정렬 반전 |
| 돌연변이 (리뷰 라운드) | 다섯을 더 심었다. 넷은 검출 — 스케줄러 풀 키 경로 · compose 의 `127.0.0.1:` 제거 · `.example` 의 환경변수 이름 오타 · 가드의 `isEmpty()` 무력화. **하나는 살아남았다** — 어댑터의 `missingCoreTables()` 를 `return List.of()` 로 바꿔도 전부 초록이었다(가드가 있는데 아무것도 안 막는 상태). `catalog` 를 `information_schema` 로 갈아 끼우는 테스트를 더해 막았다 |

### 3단계 — 검증 판정을 지표로 낸다

```
cy_verification_verdict{dataset,scope}    PASS=0, FAIL=1
cy_verification_findings{dataset,scope}   검출 건수
```

**값을 `afterJob` 이 아니라 주기 조회로 채운다.** 프로세스 게이지였다면 재시작 뒤 다음
실행까지 값이 비는데, 만료·정리가 배치 창으로 옮긴 지금 그 공백이 **최대 하루**다(CY-397).
`verifyJob` 은 아예 **사람이 손으로, 드물게** 돌린다. 만료 대기 지표 넷도 같은 이유로
되읽기가 됐다(CY-421) — 그쪽은 **마지막으로 성공한 실행의 `asOf`** 로 다시 센다. 프로세스 게이지로 두면
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

> **아래 한 문단은 최종 구현이 아니다.** 3단계는 리스너가 아니라 **주기 되읽기**로 갔고
> (`VerificationMetricsRefresher`), 라벨은 `JobParameters` 가 아니라 붙어 있는 스키마
> (`rules.hasCleanOnlyConstraints()`)로 정한다 — `CleanSchemaGuard`·`rejectDatasetMismatch`
> 와 **같은 사실**을 본다. `docs/13` §2a 에 같은 무효화 표시를 달아 뒀다. 초안을 남기는 것은
> *왜 리스너로 안 갔나* 가 다음 사람에게 근거로 필요해서다.
>
> ~~리스너/조회는 `runId` 없이도 동작해야 한다. 키는 `VerifyJobConfig.RUN_ID_KEY`(값은
> `"runId"`)이고, `startRunStep` 이 가드 여덟을 전부 통과한 뒤에야 심는다. 라벨은
> `JobParameters` 에서 뽑는다.~~

**CY-347 에서 값을 치른 것 셋을 그대로 지킨다** — 자세한 근거는 `docs/13` §2a.

1. 판정을 못 낸 실행은 `0`(=PASS)이 아니라 **`NaN`(모름)**
2. 두 지표를 따로 `set` 하지 않는다 — 한 스냅샷
3. 알림 식에서 계산하지 않는다

**`verdict` 를 0/1 로 인코딩하는 것은 값이 둘뿐이라 성립한다.** 셋째가 생기면 규칙 파일을
안 고쳐도 뜻이 바뀌므로, 매핑을 한 곳에 모으고 `VerdictType.values().length == 2` 를
단언하는 가드 테스트를 함께 둔다.

### 4단계 — 알림 넷을 더한다

| 알림 | 조건 | channel | 대응 |
|---|---|---|---|
| `VerificationVerdictFailed` | `cy_verification_verdict == 1` | `data` | 판정은 났고 불일치가 있다 |
| `VerificationMetricsUnknown` | `cy_verification_verdict != itself` | `server` | 판정을 지표로 못 내고 있다 |
| `VerificationMetricsStale` | `(cy_verification_refresh_failures_total - … offset 10m) >= 3` | `server` | 되읽기가 실패해 값이 낡고 있다 |
| `AlertDeliveryFailing` | `(alertmanager_notifications_failed_total - … offset 10m) > 0` | `server` | 알림이 리시버까지 못 가고 있다 |

**셋째가 없으면 되읽기 설계에 구멍이 남는다.** 갱신기는 실패해도 게이지를 **안 지운다** —
지우면 정상 재기동 한 번에 판정이 사라져 관제와 DB 가 갈리기 때문이다. 그 대가로
*낡은 값이 현재 값처럼 보이는* 상태가 생기는데, 그때 `verdict` 는 어제의 `0`(PASS) 이라
`VerificationVerdictFailed` 도 `VerificationMetricsUnknown` 도 조용하고 잡이 안 돈 것도
아니라 `BatchJobFailed` 도 아니다. **게이트가 FAIL 인데 관제는 통과라고 말한다** —
이 지표를 만든 이유가 그 상태를 없애는 것이었다. 카운터만 내보내고 보는 사람이 없으면
"축을 진다" 는 선언이 문장으로만 남는다.

**판정 알림을 "잡 `FAILED`" 로 정의하면 안 된다.** Step 체인이
`finalizeRunStep → statsAggregateStep` 이라, 통계 Step 이 죽으면 **잡은 `FAILED` 인데
verdict 는 이미 커밋돼 있다.** 그때 <i>"판정을 못 냈다"</i> 는 거짓이고, 실제 조치는
*"쓰기를 멈추고 다시 실행"* 이다. 반대로 `verdict = FAIL` 인 실행은 잡이 `COMPLETED` 다 —
**두 축이 서로 독립인데 한 축으로 매핑하면 오진한다.** 그래서 잡 상태가 아니라
**지표**로 본다.

`NaN` 감시(`VerificationMetricsUnknown`)가 반드시 필요하다. 그때 `verifyJob` 은 크론이 없어
"안 돌았다" 축이 없었고, 지표가 `NaN` 이면 **어떤 알림도 안 떴다** — CY-347 이
`ExpireMetricsUnknown` 으로 값을 치른 그 자리다. (그 축은 CY-470 이 세웠다 —
`VerifyNotSucceeding` 계열이 `(dataset=CLEAN, scope=FULL)` 그레인으로 진다.)

**~~`verifyJob` 의 `BatchJobRunningTooLong` 은 안 만든다~~ 만들었다 · CY-470.** 그때는
300만 전수의 소요를 **잴 근거가 없었다** — 근거 없는 수치를 규칙 파일에 박으면 그것이 다음
사람에게 기준으로 읽히므로, 적재 뒤로 미뤘다. 이제 쟀다: **472초**(300만 발급 · 516만 이력,
그중 `replayStep` 이 312초). `VerifyRunningTooLong` 의 임계는 그 **2.5배인 1,200초**이고,
`batch.metrics.verify-running-too-long-seconds` 로 열려 있다 — 그 값이 SLA 부등식의
**잡 소요 항**이기도 해서 짧은 크론 배포가 함께 조정할 수 있어야 한다.

### 배포 순서는 compose 가 아니라 `SchemaPresenceGuard` 가 잡는다

`batch` 는 `flyway.enabled:false` 다 — 마이그레이션 소유자는 `api` 하나로 고정돼 있다.
그런데 **빈 DB 에서도 기동이 그냥 성공한다**(`docs/11` 의 "`application.yml` 은 문서가
둘이다" 절). `@Entity` 가 없어
`ddl-auto: validate` 가 공허하게 통과하기 때문이다. compose 로도 못 막는다 —
`batch.yml` 의 `depends_on: mysql: condition: service_healthy` 는 **mysqld 가 살아 있다**
까지만 보장하고, `base.yml` 에는 마이그레이션을 돌릴 `api` 서비스도 없다.

**이 티켓이 그 침묵을 한 단계 더 나쁘게 만들었다.** `VerificationMetrics` 는 자기가 보는
데이터셋을 `rules.hasCleanOnlyConstraints()` 로 정하는데, 그 구현은 인덱스 하나를 묻는
`EXISTS` 라 **테이블이 하나도 없으면 예외 없이 `false`** 를 준다. 그러면 빈 DB 에 붙은
프로세스가 `cy_verification_verdict{dataset="CORRUPT"}` 를 내보내고, 30분 뒤
`VerificationMetricsUnknown` 이 그 라벨로 뜬다 — 실제 원인은 *"스키마가 없다"* 인데 사람은
*"CORRUPT 셋 검증이 안 돌았다"* 로 읽는다. **"0건이 두 뜻을 갖는다" 와 같은 종류다.**

그래서 `docs/11` 이 예약해 둔 가드를 여기서 만든다. `ApplicationRunner` 로 두어
`FlywayMigrationInitializer` 보다 확실히 뒤에 오게 하고(`InitializingBean` 은 그 순서가
보장되지 않는다), `@Order(HIGHEST_PRECEDENCE)` 로 `JobLauncherApplicationRunner`(정렬값 0)
보다 앞에 세운다 — 잡이 먼저 돌면 가드가 말하기 전에 SQL 에러로 죽는다.

**그 과정에서 이 티켓의 범위 밖 사실이 하나 드러났다 — 검증용 셋에는 Spring Batch 메타
테이블이 없다.** `coupon_clean`·`coupon_corrupt` 는 cy-seed 의 `ddl/` 로 만들어지는데
거기에 `BATCH_*` 가 하나도 없고(`CREATE TABLE` 17개 중 0개), 그 DB 를 보게 배치를 띄우는
것이 설정 파일이 문서화한 정상 절차다. 즉 **데이터 넷은 다 있고 메타만 없는 상태가 정상
절차에서 생긴다.** 가드가 붙기 전에는 기동이 통과하고 첫 `verifyJob` 트리거에서
`Table 'BATCH_JOB_INSTANCE' doesn't exist` 로 죽었다 — `docs/11` 이 배포 순서 위반의
증상으로 지목한 그 문자열이다. **지금은 기동에서 `SCHEMA_NOT_MIGRATED` 로 막는다.**

그래서 가드는 두 축을 함께 보고 **메시지를 갈라 준다**. 메타만 없으면 *"`V11__batch_metadata.sql` 을 이 스키마에
부으십시오"*, 데이터가 없으면 *"api 를 먼저 띄우십시오"*, 접속 스키마가 아예 없으면
(`DATABASE()` 가 `NULL` — URL 에서 DB 이름을 빠뜨린 것) *"URL 을 확인하십시오"*. 같은 증상에
조치가 셋인데 한 문장으로 접으면 사람이 엉뚱한 것을 재배포하며 시간을 쓴다.

목록은 그 여덟(데이터 넷 + Spring Batch 메타 넷)에서 멈춘다 — 넓히면 가드가 *배포 순서* 가 아니라 *스키마 최신성* 을 보게
되고 그건 Flyway 의 몫이다.

### `BATCH_SCHEDULING_ENABLED=false` 의 상시 critical — silence 로 간다

`batch.yml` 의 기본값이 `false` 라, README 가 안내하는 그대로 띄우면 만료·정리가 한 번도
안 돈다.

> ⚠️ **이 문단은 2026-08-22 의 규칙 기준이다.** 그때는 `absent_over_time` 이 영구히 참이
> 되어 15분 뒤 critical 이었다. CY-392 가 축을 *"마지막 성공 시각"* 으로 갈면서 시계열은
> **태어나되 값이 `NaN`** 이 됐고(게이지를 `Job` 빈에서 무조건 등록한다), 지금 그 상태를
> 지는 것은 `ExpireNeverSucceeded`(NaN, `for` 10분) · `CleanupNeverSucceeded`(NaN, 30분)이다.
> **`ExpireGaugeMissing` 은 여기 안 들어간다** — 게이지가 `NaN` 으로라도 태어나므로
> `absent()` 가 거짓이다(CY-661 실측). 한때 이 줄에 열거돼 있었다. **결론은 그대로다** — 규칙에 예외를 안 파고
> 사람이 재운다.

### 거는 명령 (CY-661 이 실제로 걸었다)

```bash
docker compose -f base.yml exec -T alertmanager amtool silence add \
  --alertmanager.url=http://localhost:9093 \
  'alertname=~"(Expire|Cleanup)NeverSucceeded|ExpireMetricsUnknown|Verify(NeverSucceeded|NotSucceeding)"' \
  --duration=168h \
  --author="$(git config user.name)" \
  --comment='BATCH_SCHEDULING_ENABLED=false — 만료·정리·검증 크론이 일부러 꺼져 있다 (docs/14). 스케줄러를 켜기 전에 반드시 해제한다.'

# 확인·해제
docker compose -f base.yml exec -T alertmanager amtool silence query  --alertmanager.url=http://localhost:9093
docker compose -f base.yml exec -T alertmanager amtool silence expire --alertmanager.url=http://localhost:9093 <ID>
```

**덮는 것은 다섯이고, 셋은 일부러 뺐다.**

| 덮는다 | 왜 |
|---|---|
| `ExpireNeverSucceeded` · `CleanupNeverSucceeded` | 값이 `NaN` 이라 뜬다 — 한 번도 안 돌았다 |
| `ExpireMetricsUnknown` | 위와 **같은 사건**이다. 대기 건수를 셀 기준 실행이 없다 |
| `VerifyNeverSucceeded` · `VerifyNotSucceeding` | **검증 크론도 같은 스위치에 묶여 있다**(`VerifyScheduler` 의 `@ConditionalOnProperty`). 안 덮으면 25시간 뒤부터 critical 이 시간당 한 번씩 나간다 |

| 안 덮는다 | 왜 |
|---|---|
| `ExpireGaugeMissing` · `CleanupGaugeMissing` | **이 상태에서 안 뜬다.** `BatchRunMetrics` 가 `Job` 빈마다 게이지를 **무조건** 등록하므로 시계열은 태어나고 값만 `NaN` 이다 — `absent()` 가 거짓이다(실측: `cy_batch_last_success_seconds{spring_batch_job_name="expireJob"} NaN`). 덮으면 **뜨지 않는 것을 덮으면서 라벨 축이 무너진 것만 가린다** — 잡 이름이 바뀌거나 스크레이프가 `spring_batch_job_name` 을 개명시키면 만료 감시 축 전체가 조용해지는데, 그것을 잡는 마지막 장치가 이 둘이다 |
| `BatchJobFailed` | 스위치와 무관하다. 실제로 돌다가 죽은 것이다 |

> ⚠️ **`--duration` 이 프로젝트보다 짧아야 한다.** 한때 720시간(30일)으로 걸었는데
> `docs/04` 가 *"3주 프로젝트"* 라 **끝날 때까지 만료되지 않는다** — "그 안에 한 번은 다시
> 본다" 를 강제하는 것이 아무것도 없다. 168시간(1주)이면 게이트 사이에 최소 한 번 재검토가
> 강제된다.

> ⚠️ **스케줄러를 켜기 전에 반드시 푼다.** 살아 있으면 만료가 실제로 실패해도 함께 덮인다.
> 시연 절차에서 `BATCH_SCHEDULING_ENABLED=true` 로 띄우기 **직전**에 이것을 돌린다:
>
> ```bash
> COMMENT='BATCH_SCHEDULING_ENABLED=false — 만료·정리·검증 크론이 일부러 꺼져 있다 (docs/14). 스케줄러를 켜기 전에 반드시 해제한다.'
> docker compose -f base.yml exec -T alertmanager amtool silence query \
>   --alertmanager.url=http://localhost:9093 -o json \
>   | jq -r --arg c "$COMMENT" '.[] | select(.comment == $c) | .id' \
>   | xargs -r -n1 docker compose -f base.yml exec -T alertmanager \
>       amtool silence expire --alertmanager.url=http://localhost:9093
> ```
>
> **부분 일치가 아니라 정확히 같은 `comment` 만 고른다.** 한때
> `test("BATCH_SCHEDULING_ENABLED")` 로 찾았는데, 그 설정 이름을 설명에 쓴 **다른 silence 도
> 함께 풀린다** — 억제 중이던 알림이 예고 없이 쏟아진다. 위 문장은 이 절이 거는 명령의
> `--comment` 와 **글자 그대로 같아야 한다.** 한쪽을 고치면 다른 쪽도 고친다.
>
> 더 확실한 길은 **건 직후 ID 를 적어 두는 것**이다(`amtool silence add` 가 ID 를 낸다).
> 위 방법은 그 ID 를 못 챙긴 경우의 대비책이다.

> **콜드 스타트용 26시간 silence 와 대상이 겹친다. 둘 다 걸지 말 것** —
> 볼륨을 지우고 띄운 직후만이면 그쪽(26h), 스케줄러를 계속 끄고 둘 것이면 이쪽 하나만 건다.

**만료·정리는 규칙에 예외를 안 판다.** `cy_batch_scheduling_enabled` 게이지를 만들면
*일부러 껐다* 와 *꺼져 버렸다* 가 같은 값이 되고, 그 둘의 결말이 다르다 — 만료가 하루 안 돌면
재고가 안 걷힌다. 그래서 아래 시연 절차의 *"먼저 두 알림을 재운다"* 처럼
**끄는 구간에만 사람이 silence 를 건다.**

**회차 상태 전이는 갈랐다 (CY-446).** `cy_coupon_round_scheduling_enabled == 1` 로 갈래를 뺀다.
근거가 다른 것은 **크론이 1분**이기 때문이다 — 만료·정리(일 1회)는 silence 한 번이 슬롯 하나를
덮지만, 1분 크론은 끈 구간이 곧 **상시 점등**이라 덮을 수가 없다. 게다가 compose 기본이
`BATCH_SCHEDULING_ENABLED=false` 라 그 구간이 예외가 아니라 기본값이다.

> ⚠️ **대가를 여기 적어 둔다.** 이 갈래는 *스위치가 사고로 꺼진 것*도 함께 가린다.
> 배포에서 `BATCH_SCHEDULING_ENABLED` 를 넘기던 자리가 빠지면 조용히 `false` 가 되고,
> 회차가 하나도 안 열리는데 `CouponRoundsNotOpening`(critical)이 발화 조건을 못 만든다.
> **그 축은 아무 알림도 안 진다** — `CouponRoundMetricsMissing` 은 빈 자체가 없는 경우만 본다.
> `cy_coupon_round_scheduling_enabled` 가 기대값인지 확인하는 것은 **배포 검증의 몫**이다.

**부하 중 `BatchTargetDown` 도 같은 이유로 silence 로 다룬다.** `base.yml` 만 띄운 상태는 배치가
<b>일부러</b> 없는 것이라 알림이 뜨는 게 맞다 — 규칙이 틀린 것이 아니다. `inhibit_rules` 로
자동 억제하면 <i>진짜로 배치가 죽은 것</i>까지 함께 가려진다. 부하 구간에만 사람이
`amtool silence add` 로 끄고, 끝나면 푼다.

---

## 검증 계약

- **규칙 로드** — 파일이 있는 것이 아니라 Prometheus 가 **읽었다**는 것을 응답으로 확인
- **경로 분기** — `channel: server` / `channel: data` 로 갈리는 것을 확인한다.
  `severity` 는 관계없다 — warning 셋 중 둘이 서버 축이다
- **지표 이름** — `BatchMetricExposureTest` 가 규칙 파일에서 이름을 뽑아 `/actuator/prometheus`
  본문과 대조한다. 다만 그것이 보는 것은 **이름뿐**이다 — 갱신기를 안 걸어도, 값이 늘
  `NaN` 이어도 초록이다. **값은 `VerificationMetricExposureTest` 가 행을 심고 `refresh()`
  를 부른 뒤 본문에서 읽어 본다**
- **모름 축** — 판정 없는 실행이 `0` 이 아니라 `NaN` 인 것. `verification_runs` 를 비우고
  `refresh()` 를 부른 뒤 본문에서 읽어 확인한다
  (`VerificationMetricExposureTest.reportsUnknownWhenNothingHasBeenJudged`). 닫혔는데
  `verdict` 가 비어 있는 행, 안 닫힌 행도 각각 같은 방식으로 본다
- **배선 가드** — `VerificationMetricsRefresher` 가 `@Scheduled` 로 실제 등록되는 것.
  그리고 그 등록이 `batch.scheduling.enabled` 에 <b>안 묶이는 것</b> — 묶이면 스케줄러를
  끈 채 띄우는 기동(부하 측정 중이거나 검증 셋을 볼 때)에서 지표가 통째로 죽는다.
  그때야말로 판정을 봐야 하는 자리다
- **설정 키 생존** — `ResolvedBatchConfigTest` 가 `@Value` 와 `@Scheduled` 의 플레이스홀더를
  전부 훑어 `.example` 에 실재하는지 본다. `fixedDelayString` 축이 안 훑기고 있었다

## 안 하기로 한 것과 이유

| 무엇 | 왜 |
|---|---|
| Slack | `docs/13` §2e — PRD 제약(외부 연동 Mocking) + PUBLIC 저장소 |
| Grafana | 우리 대시보드는 Grafana 가 아니다 — PRD 상 Chart.js 폴링 커스텀이고 영역 ⑤ 몫이다 |
| `api` 컨테이너 | 스크레이프 대상이 아니다. **마이그레이션을 compose 가 돌려 주지도 않는다** — `depends_on: service_healthy` 가 보장하는 것은 `mysqladmin ping` 뿐이다. 대신 `batch` 쪽에서 잡는다(바로 아래) |

## 시연 절차 — 한 기동으로 둘 다 본다

**CY-384 전에는 그럴 수 없었다.** `batch.scheduling.enabled` 가 두 잡에 **정반대**를
요구했다 — `true` 면 만료가 5분마다 돌지만 `rejectRunningSchedulers` 가 `verifyJob` 을
거부하고, `false` 면 그 반대였다. 그래서 축 하나를 보려면 재기동해야 했다.

지금 검증을 막는 것은 그 플래그가 아니라 **실제로 도는 만료 실행**이다. 만료는 배치 창
(04:10 UTC = **13:10 KST**)에 하루 한 번 도므로 그 시각만 피하면 언제든 트리거된다 —
정리도 20분 뒤(13:30 KST)라 함께 피한다. 그 시각에 걸면 정리의 **첫 Step**(검증 파생 행)이
첫 청크에서 물러나 `asof_state` 를 **한 행도 안 걷는다**(종료 코드 `YIELDED`).
**배치 메타는 그래도 걷힌다** — 둘째 Step 은 검증 데이터를 안 건드리므로 걷고 나서 `YIELDED`
를 이어받는다. 그 수치가 종료 설명의 `metaExecutions`·`metaInstances` 다.

1. **먼저 두 알림을 재운다.** `BATCH_SCHEDULING_ENABLED` 를 켜든 끄든 필요하다 —
   만료·정리가 배치 창(04:10 · 04:30 UTC)으로 옮겨(CY-397) **크론 슬롯 한 번이 하루**이므로,
   볼륨을 지우고 새로 띄우면 다음 슬롯까지 최대 하루 동안 게이지가 `NaN` 이다.
   규칙의 `for` 는 10분·30분이라 **이 구간을 못 덮는다** — 콜드 스타트는 오직 이 silence 가
   막는다. 크론 슬롯(25h·26h)을 `for` 예산으로 쓰면 재기동마다 stale 마커로 타이머가
   리셋돼 알림이 **영원히 안 뜬다**(근거는 `batch-alerts.yml` 의 `ExpireNeverSucceeded` 주석).

   ```bash
   docker compose -f base.yml exec alertmanager \
     amtool silence add 'alertname=~"ExpireMetricsUnknown|(Expire|Cleanup)NeverSucceeded"' \
     --duration=26h --comment="daily batch window; fresh DB has no run yet" \
     --alertmanager.url=http://localhost:9093
   ```

   > **`VerifyNeverSucceeded` 는 왜 안 넣나.** 콜드 스타트에서 뜨는 것은 맞다 — 다음
   > 05:00 슬롯까지 최대 하루 동안 `NaN` 이다. 그런데 그 하루는 <b>게이트 판정의 근거가
   > 없는 하루</b>이기도 하다. `CouponRounds(NotOpening|NotClosing)` 를 silence 에서
   > 뺀 것과 같은 판단이다 — 재우면 그 사실을 아무도 말해 주지 않는다.
   >
   > 끄고 싶으면 검증을 한 번 손으로 돌린다. **다만 배치 창을 피해서 부른다** —
   > 만료 04:10 · 정리 04:30 · 검증 05:00 UTC 는 각각 **13:10 · 13:30 · 14:00 KST** 이고,
   > `max-expire-skips=0` 이라 그 근처에서 부른 검증은 만료가 **뚫고 지나가** 버려진다
   > (CY-470). **KST 15:00 이후**가 안전하다. 그 규약은 `docs/15` 와
   > `application.yml.example` 의 `max-expire-skips` 주석이 함께 적는다.
   >
   > **`ExpireMetricsBackdated`·`ExpireMetricsStale` 은 왜 안 넣나.** 둘 다 콜드 스타트에서
   > 뜰 수 없다 — 앞엣것은 `cy_expire_measured_at_seconds` 가 `NaN` 이라 비교가 거짓이고,
   > 뒤엣것은 `cy_expire_refresh_failures_total` 이 0 에서 시작해 델타가 안 생긴다.
   > 위 셋과 달리 **"값이 아직 없다" 가 곧 침묵**인 식이라 재우지 않아도 조용하다.
   >
   > **`CouponRoundsNotOpening`·`NotClosing` 도 안 넣는다.** 크론이 1분이라 <i>"슬롯 한 번이
   > 하루"</i> 라는 근거가 성립하지 않고, 빈 DB 에서는 `COUNT(*)` 가 0 이라 `> 0` 이 거짓이다.
   > 재우면 2단계(`BATCH_SCHEDULING_ENABLED=true`) 이후 **26시간 동안 회차가 안 열리는
   > critical 이 죽는다** — 그 알림의 뜻은 <i>"그 회차는 지금 아무도 발급받을 수 없습니다"</i> 다.

2. `BATCH_SCHEDULING_ENABLED=true` 로 띄운다 — 만료 알림 축이 살아난다

3. **같은 기동에서** `verifyJob` 을 트리거한다 — 검증 알림 축 확인

   마침 만료가 도는 중이면 `409 VERIFICATION-012` 가 온다. **그것도 보여 줄 것이다** —
   재고를 쓰는 잡과 판정하는 잡이 서로를 아는 것이 설계이고, 응답 본문에 그 이유가 있다.
   만료는 한 실행에 수 초라 몇 초 뒤 다시 부르면 접수된다.

   > **접수된 뒤가 더 중요하다.** 만료가 배치 창으로 옮긴 뒤에도 손 트리거는 시각을 안
   > 가리므로, 한쪽 가드만 있으면 **13:10 KST 에 걸린 실행이 크론에 물려
   > `DATASET_MUTATED_DURING_RUN` 으로 버려진다.** 그래서 만료 스케줄러가 **검증이 도는 동안
   > 그 슬롯을 건너뛴다** — 건너뛴 몫은 다음에 도는 슬롯이 통째로 가져간다. 다만 연속 스킵에
   > 상한이 있어(`max-expire-skips`, 기본 1) 최대 지연이 `(상한 + 1) × 크론 최대간격`,
   > 즉 이틀로 묶인다. 상한을 넘으면 재고 쪽을 택해 만료를 돌리고, 그때 그 검증은 버려진다.
   >
   > 버려지면 그 `asOf` 는 **영구히 못 쓴다.** 만료가 찍은 `updated_at` 이 지워지지 않아
   > `rejectIssuancesUpdatedAfterAsOf` 가 그 `asOf` 이하로 영원히 참이 된다 — 재시도가
   > 아니라 **더 뒤의 `asOf`** 로 불러야 한다.

   > **스케줄러를 끈 채 시연하면** 두 게이지가 계속 `NaN` 이다 — 시계열은 태어나되 값이
   > 없다(`BatchRunMetrics` 가 `Job` 빈에서 이름을 받아 무조건 등록한다). 규칙이 틀린 게
   > 아니라 그 설정에서 정상인 상태라, 위 1번의 silence 가 그 구간을 덮는다.

   **`attempt` 는 시드가 점유한 값을 피한다.** 시드는 CLEAN 에 `(FULL,1)`·`(FULL,2)`·
   `(INCREMENTAL,1)` 을, CORRUPT 에 `(FULL,1)` 을 심는다. `uk_run_params` 와
   `rejectExistingRun` 이 그것을 막으므로 `attempt=1` 로 돌리면 `INVALID_RUN_PARAMS` 로
   잡이 죽고 — **보여 주려던 축 대신 `BatchJobFailed` 가 뜬다.**

   **CY-368 이 트리거 API 를 열었다.** 업무 포트가 기본으로 안 열리므로 오버레이를 얹는다.

   ```bash
   docker compose -f base.yml -f batch.yml -f batch-expose.yml up -d batch

   curl -X POST "http://127.0.0.1:9090/api/v1/admin/verify?asOf=<시드의 as_of>"
   # → 202 {"success":true,"data":{"executionId":41,"asOf":...,"dataset":"CLEAN",
   #                                 "scope":"FULL","attempt":3},"error":null}
   ```

   `dataset`·`scope`·`attempt` 는 안 줘도 된다 — 서버가 붙어 있는 스키마와 다음 빈 번호로
   채운다. **`attempt` 를 손으로 줄 때만** 시드가 점유한 값(CLEAN 1·2, CORRUPT 1)을 피해야 한다.

   ```bash
   curl "http://127.0.0.1:9090/api/v1/admin/verify/runs/41"
   # 같은 요청을 **종료 상태까지 반복**한다. 41 은 POST 응답의 data.executionId 다.
   #
   # 도는 중:   {"success":true,"data":{"executionId":41,"runId":null,"status":"STARTED", ...}}
   # 끝난 뒤:   {"success":true,"data":{"executionId":41,"runId":1207,"status":"COMPLETED",
   #                                    "verdict":"PASS","findingCount":0, ...}}
   ```

   위는 CLEAN 이라 **0건이 정상**이다. 검증 알림(`VerificationVerdictFailed`)을 실제로
   띄우려면 오염셋을 돌린다 — 그쪽은 `seedRunId` 가 필수다.

   ```bash
   curl -X POST "http://127.0.0.1:9090/api/v1/admin/verify?asOf=<시드의 as_of>&dataset=CORRUPT&seedRunId=<시드 run>"
   # → 판정 뒤 조회하면:
   #   {"success":true,"data":{"verdict":"PASS","findingCount":800,"statsStatus":"SKIPPED",...}}
   #   ← 검출 800건이 정답 매니페스트와 **양방향으로** 일치했다는 뜻이다(누락 0 · 오탐 0).
   #     judgeAgainstManifest 는 집합이 정확히 같을 때 PASS 를 낸다 — 오염셋에서 FAIL 은
   #     합격이 아니라 **집합이 어긋났다**는 뜻이고, 그때 VerificationVerdictFailed 가 운다.
   #     그 알림을 실제로 띄워 보려면 expected_findings 를 한 행 지운 사본으로 돌린다.
   #     statsStatus 가 SKIPPED 인 것도 정상이다 — 오염 데이터 위의 집계는 뜻이 없다.
   ```

   **`runId` 가 `null` 인 것도 정보다** — 아직 판정 단계에 못 갔거나, 가드에 걸려 끝까지
   못 가는 실행이라는 뜻이다. `status`(잡이 돌았나)와 `verdict`(데이터가 맞나)는 독립이라
   따로 읽는다. 근거는 `docs/15`.

   **검증용 셋에는 Spring Batch 메타 테이블이 없다.** cy-seed 의 `ddl/` 이 안 만든다.
   `SchemaPresenceGuard` 가 기동 시점에 그것을 말해 주므로 배치가 아예 안 뜬다 —
   그 스키마에 **배치 메타 마이그레이션 셋**을 한 번 부어야 한다 — `V11__batch_metadata.sql`(테이블)과
   `V2026082513`·`V2026082514`(인덱스)다. **인덱스를 빼먹으면 `SchemaPresenceGuard` 가 기동을 거절한다**(CY-686).
   거절 메시지가 없는 인덱스마다 파일명과 아래 증상을 함께 말한다. 급하면
   `batch.schema-guard.require-batch-indexes=false` 로 끌 수 있고, 그때는 아래가 실제로 일어난다:
   - `V2026082513`(`STATUS, END_TIME`) 누락 → 되읽기 둘이 7일 창을 인덱스 없이 훑는다. 각자의
     데드라인을 넘기면 **게이지가 `NaN`** 이 된다 — 키가 다르다:
     `BatchRunMetricsRefresher` 는 `batch.metrics.run-timeout-ms`,
     `ExpirePendingRefresher` 는 `batch.metrics.expire-pending-timeout-ms`(둘 다 기본 5초).
     넘기는지는 안 쟀다 — `V2026082513` 헤더가 잰 것은 읽는 행 수(25,950 → 2,016)까지다.
   - `V2026082514`(`CREATE_TIME`) 누락 → 게이지는 멀쩡하다. 대신 `purgeBatchMetadataStep` 의
     대상 선택이 매 청크 전체 스캔이 되어 총비용이 `N²/(2·chunk)` 가 되고,
     **`CleanupRunningTooLong` 으로만** 드러난다.

   ```bash
   # 변수 이름에 주의. MYSQL_ROOT_PASSWORD 는 **컨테이너 안** 이름이고, 호스트 셸이
   # 펼치는 것은 DB_ROOT_PASSWORD 다. 전자를 쓰면 빈 문자열이 되어 mysql 이 대화형
   # 프롬프트로 가고, stdin 은 SQL 파일이라 첫 줄을 비밀번호로 읽고 죽는다.
   # -p 를 인자로 주면 ps 에 남으므로 MYSQL_PWD 로 넘긴다.
   #
   # ⚠️ **두 스키마에 다 부어야 한다.** 아래 CORRUPT 트리거는 coupon_corrupt 를 보는
   #    기동에서 돌리는데, 그 스키마에도 BATCH_* 가 없어 첫 실행이 메타 테이블 오류로 죽는다.
   #
   # ⚠️ **인덱스 둘도 함께 붓는다.** 없어도 기동·동작이 통과한다 — V2026082513 을 빼면 게이지가
   #    NaN 이 되고, V2026082514 를 빼면 게이지는 멀쩡한 채 정리 잡만 매 청크 전체 스캔이 된다.
   for SCHEMA in coupon_clean coupon_corrupt; do
     for F in V11__batch_metadata.sql \
              V2026082513__ix_batch_job_execution_lookup.sql \
              V2026082514__ix_batch_job_execution_history.sql; do
       docker compose -f base.yml exec -T -e MYSQL_PWD="${DB_ROOT_PASSWORD:-root}" mysql \
         mysql -uroot "$SCHEMA" \
         < storage/src/main/resources/db/migration/$F
     done
   done
   ```

   `as_of` 는 시드가 심은 기준 행에서 얻는다. `origin` 으로 좁히는 것은 cy-seed 의
   `bin/seed.py` 가 자가검증에서 같은 이유로 이미 그렇게 하기 때문이다.

   ```sql
   SELECT MAX(as_of) FROM verification_runs WHERE origin = 'SEED';
   ```

   **이 조회가 `Unknown column 'origin'` 으로 죽으면 데이터셋이 cy-seed `1f217b5` 이전
   것이다.** 그 DB 는 cy-seed 의 `ddl/00_schema.sql` 로 만들어져 cy-be 의 Flyway `V2026082512` 가
   닿지 않으므로, 마이그레이션으로는 못 고친다 — **데이터셋을 다시 만드는 것이 유일한
   답이다.** 그대로 두면 되읽기가 매번 실패하고 `VerificationMetricsStale` 이 뜬다.
