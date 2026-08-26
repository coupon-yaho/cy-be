# CY-368 · 검증 배치 온디맨드 트리거 / CY-429 · 만료 복구 API

`verifyJob` 을 사람이 돌릴 수 있게 한다. 지금은 **batch 에 `@RestController` 가 0건**이라
검증을 손으로 시작할 방법이 테스트 말고 없다 — CY-359 가 시연 절차에서 막힌 자리다.

| | |
|---|---|
| **한다** | `POST /api/v1/admin/verify` — 202 + `executionId` |
| **한다** | `GET /api/v1/admin/verify/runs/{executionId}` — 판정·검출 건수·상태 |
| **한다** | `POST /api/v1/admin/verify/runs/{executionId}/stop` — 실행 중단 |
| **한다** | 업무 포트 노출 결정 — `application.yml.example` 이 이 티켓에 예약해 뒀다 |
| **했다 (CY-590)** | `GET /api/v1/admin/verify/reports/latest?dataset=&scope=` — 제출용 리포트. 한때 이 표가 *"안 한다 — 별도 티켓"* 이라고 적었고, 그 별도 티켓이 CY-590 이다 |
| **안 한다** | 인증·인가 — batch 에 Spring Security 가 없다. 아래 "남긴 것" |

> **전제 — 검증은 만료가 도는 동안에는 안 돈다.** `startRunStep` 의 가드가 배치 메타에
> 실행 중인 만료가 있으면 거절한다. 트리거를 여는 것이 그 제약을 푸는 것은 아니다.
> 아래 "아무 때나 못 돈다" 절에 근거가 있다.
>
> **CY-384 가 이 가드의 근거를 바꿨다.** 그전에는 `batch.scheduling.enabled=true` 이기만
> 해도 거절했다 — 운영은 늘 true 라 **검증이 영영 못 돌았고**, 그것이 검증을 온디맨드로
> 밀어낸 원인이었다.

---

## 결정 1 — 업무 포트에 열고, 대신 밖으로 내보내지 않는다

설정 파일이 이 결정을 **이 티켓에 명시적으로 예약**해 뒀다.

> ⚠️ 업무 포트는 이미 밖에서 닿는다. compose 가 `BATCH_HOST_PORT` 로 호스트에 내보낸다.
> 그러니 **admin 트리거를 이 포트에 평범한 컨트롤러로 두면 방어가 하나도 없다.**
> … 커스텀 actuator 엔드포인트로 관리 포트에 올리거나, compose 의 업무 포트 매핑을
> 없애거나 **둘 중 하나를 그 티켓에서 정한다.**

**후자로 간다.** 앞의 것은 경로가 `/actuator/verify` 가 되어 **프론트 계약이 깨진다** —
`docs/05-design-handoff.md:213` 이 `POST /api/v1/admin/verify?asOf={ts}` 로 이미 고정해 뒀고,
그 파일은 프론트가 함수 이름까지 붙여 놓은 인수인계 문서다. 관측 하나를 지키려고 팀 계약을
바꾸는 것은 비용이 반대다.

그래서 `batch.yml` 에서 **`ports` 를 통째로 뺐다.** compose 는 `ports` 를 변수로 비울 수
없어서(빈 값이 파싱 오류다) 오버레이를 하나 더 가른다 — `base`/`batch` 를 이미 그렇게
나눠 온 관행 그대로다.

```bash
docker compose -f base.yml -f batch.yml up batch                        # 안 내보낸다
docker compose -f base.yml -f batch.yml -f batch-expose.yml up batch    # 열 때만
```

**CI 가 그 규율을 검사한다.** 기본 조합에 `batch` 포트가 있으면 실패하고, 오버레이를 얹은
조합에서 `127.0.0.1` 이 빠져도 실패한다. 둘 다 돌연변이로 검출력을 확인했다.

**이것은 방어가 아니라 노출 축소다.** 같은 호스트에서 `docker compose exec` 로 들어가면
여전히 인증 없이 닿는다. PRD 보안 ①이 요구한 `ADMIN 역할`은 여기서 안 한다 — 그 이유와
남긴 것은 맨 아래에.

---

## 결정 2 — 202 에는 `executionId` 를 싣는다

PRD 는 *"202 + runId"* 라고 적었다. **그 시점에 `runId` 는 존재하지 않는다.**

`runId` 는 `verification_runs.id` 이고, `startRunStep` 이 **가드 여덟을 전부 통과한 뒤에야**
`runs.save(...)` 로 만든다(`VerifyJobConfig:768`). CY-359 가 그 사실 위에 계약을 세웠다 —
**`runId` 가 없는 실행이 곧 판정을 못 낸 실행이다.**

컨트롤러가 행을 먼저 넣어 `runId` 를 만들면 그 계약이 무너진다. 가드에 걸려 죽은 실행도
`runId` 를 갖게 되고, `uk_run_params` 중복 판정까지 컨트롤러로 옮겨온다.

그래서 202 는 `executionId`(Spring Batch 가 시작 즉시 주는 값)를 싣고, **조회 응답이
`runId` 를 함께 준다.** 아직 없으면 `null` 이고, **그 `null` 자체가 "아직 판정 단계에
못 갔다" 는 정보다.**

```
202  { "executionId": 41, "asOf": "...", "dataset": "CLEAN", "scope": "FULL", "attempt": 3 }

GET .../runs/41   { "executionId": 41, "runId": null,  "status": "STARTED"   }
GET .../runs/41   { "executionId": 41, "runId": 1207,  "status": "COMPLETED",
                    "verdict": "PASS", "findingCount": 0 }
```

경로의 `{id}` 는 **202 가 돌려준 값**이다. 프론트는 받은 것을 그대로 넣으면 되므로
호출 흐름은 PRD 와 같다.

---

## 결정 3 — verify 전용 비동기 실행기를 따로 둔다

202 를 주려면 잡이 비동기로 떠야 한다. `JobOperator` 에 `TaskExecutor` 를 주면 되는데,
**공용 빈을 그렇게 바꾸면 만료 배치가 깨진다.**

`ExpireScheduler` 는 동기 실행을 **전제**하고, 깨지면 잡아내려고 검사까지 심어 뒀다.

```java
if (status.isRunning()) {
    log.error("만료 배치가 비동기로 떴습니다. 이 잡은 동기 실행을 전제로 겹침을 "
            + "막습니다. JobOperator 의 TaskExecutor 를 확인하십시오. …");
}
```

겹침을 막는 것이 크론 트리거의 순차성(`ReschedulingRunnable` 이 **직전 실행이 끝난 뒤**
다음을 잡는다)이라, 비동기가 되면 **재고를 쓰는 유일한 잡이 자기 자신과 겹친다.**

그래서 `verifyJobOperator` 를 **별도 빈**으로 둔다. 공용 빈은 손대지 않고,
`ExpireScheduler` 의 주입에 `@Qualifier` 를 붙여 **어느 쪽이 무엇을 쓰는지 코드가 답하게**
한다(빈이 둘이 되는 순간 타입 주입이 모호해지므로 어차피 명시가 필요하다).

**그 실행기를 `@Bean` 으로 빼면 안 된다.** `Executor` 타입 빈이 하나라도 생기면 Boot 의
`applicationTaskExecutor` 가 조건에서 떨어져 사라지고, MVC 비동기가 요청당 스레드로 폴백하며
`spring.task.execution.*` 이 죽는다. **이 저장소가 이미 한 번 겪고 되돌린 사고**이고
`BatchJobRepositoryTest` 가 그것을 단언으로 못 박아 뒀다 — 이 티켓에서 다시 밟았다가 그
테스트가 잡았다. 그래서 실행기는 설정 클래스가 필드로 들고 `DisposableBean` 으로 정리한다.

이름도 조심한다. `BatchRegistrar` 는 이름이 `taskExecutor` 인 빈 정의가 있으면 그것을
**공용 `JobOperator` 에 물린다** — 그러면 만료까지 비동기가 되어 위에서 막으려던 상태가 된다.
빈이 아니면 그 경로도 닫힌다.

전용 실행기는 **스레드 하나, 큐 없음**이다. `verifyJob` 은 300만 전수라 둘이 겹치면
서로의 판정 근거를 흔든다 — 만료를 막는 이유(아래 절)와 같은 축이다. 큐를 두면 "받아 놓고 나중에" 가 되는데, 그 사이 `asOf` 가
지나가 **접수 시점과 실행 시점이 다른 데이터를 본다.** 그래서 거절이 맞다 — 429 로 답한다.

**그런데 실행기의 거절 예외는 컨트롤러까지 오지 않는다.** `TaskExecutorJobLauncher` 가
`TaskRejectedException` 을 자기가 잡아 잡을 `FAILED` 로 표시하고 **정상 반환**한다
(바이트코드로 확인). 그대로 두면 429 가 **죽은 코드**이고, 두 번째 요청자는 202 를 받아
놓고 `status: "FAILED"` 만 보며 원인을 찾는다.

그래서 **접수 단계에서 먼저 본다**(`getRunningExecutions`). 그 검사와 `start` 사이에 경합이
남으므로 **반환값도 본다** — 다만 "Step 0개 FAILED" 는 거절의 *필요조건*일 뿐이라 종료
설명에 거절 흔적이 있을 때만 429 로 접는다. 아니면 202 로 넘겨 조회가 원인을 말하게 한다.

---

## 파라미터 — 프론트 계약을 먼저 만족시킨다

`docs/05`(프론트)는 `?asOf={ts}` 하나, PRD 표는 body `{asOf?, dataset, scope, fromTs?}` 다.
`docs/10` 이 이미 이 어긋남을 적어 뒀다. **쿼리 파라미터로 받고 나머지에 기본값을 준다** —
프론트는 `?asOf=` 만 보내도 되고, 운영자는 더 줄 수 있다.

| 파라미터 | 기본값 | 근거 |
|---|---|---|
| `asOf` | 없으면 **거절(400)** | 판정의 기준 시각이다. 서버가 정하면 같은 요청이 매번 다른 것을 본다. **오프셋·`Z`·미래 값도 거절** — 아래 |
| `dataset` | **붙어 있는 스키마** | `hasCleanOnlyConstraints()` — `VerificationMetrics`·`CleanSchemaGuard`·`rejectDatasetMismatch` 가 쓰는 **같은 사실**이다. 넷이 각자 판정하지 않는다 |
| `scope` | `FULL` | `INCREMENTAL` 은 `rejectUnsupportedScope` 가 아직 막는다 |
| `attempt` | **마지막 번호 + 1** | `uk_run_params(as_of, dataset, scope, attempt)` 때문에 필요한데, 시드가 CLEAN 1·2 / CORRUPT 1 을 점유해 사람이 외울 수 없다. 서버가 찾는다 — **두 소스를 함께 본다**, 아래. `1` 미만은 400 |
| `seedRunId` | **CORRUPT 는 필수** | 정답 묶음이 둘 이상인 DB 에서 기본값을 두면 낡은 묶음과 조용히 대조한다. **비식별** 파라미터로 넣는다 — 식별로 넣으면 `uk_run_params` 축과 어긋난다 |
| `fromTs` | **받지 않는다(400)** | 증분 전용인데 증분이 안 열려 있다. `FULL` 이면 `VerificationRun` 생성자가 거부하고 `INCREMENTAL` 은 가드가 막아 **성공 조합이 없다**. PRD 표에는 있으나 지금은 못 쓴다 |

`asOf` 만 기본값을 안 준다. 나머지는 **틀려도 잡이 거절하지만**, `asOf` 는 아무 값이나
성립해서 조용히 다른 것을 판정한다.

**그래서 오프셋을 아예 안 받는다.** `ISO.DATE_TIME` 포맷터는 `2026-03-01T09:00:00Z` 를
파싱에 **성공**시킨 뒤 지역 부분만 뽑아 `09:00` 으로 만든다. JS 의 `toISOString()` 이 항상
`Z` 를 붙이므로 프론트가 보내면 KST 기준으로 아홉 시간이 밀리고, 데이터가 조용한 시각이면
**가드를 다 통과해 틀린 시점으로 `PASS` 를 남긴다.** 그 행에는 밀린 값이 적혀 나중에 봐도
틀린 줄 모른다.

**틀린 입력을 접수 단계에서 막는 이유는 `attempt` 다.** 잡 안에서 죽으면 배치 메타에
인스턴스가 남아 **같은 파라미터로 두 번 다시 못 부른다**(`preventRestart`). 미래 `asOf`·
`attempt < 1`·`fromTs`·`seedRunId` 누락이 전부 그 축이다.

### `attempt` 는 두 소스를 함께 본다

`verification_runs` 행은 `startRunStep` 이 **가드 여덟을 통과한 뒤에야** 만든다. 반면
`BATCH_JOB_INSTANCE` 는 잡이 시작하는 순간 생긴다. 그래서 **가드에 걸려 죽은 시도는 앞
테이블에 흔적이 없고 뒤 테이블에만 남는다.**

앞만 보면 그 번호를 다시 주는데 `verifyJob` 은 `preventRestart` 라 같은 조합을 거절한다 —
**몇 번을 눌러도 400 이고 자기 치유가 없다.** 실측으로 재현했고, 배치 메타까지 함께 보게 고쳤다.

> 두 소스의 `asOf` 를 **문자열로 비교하면 안 된다.** `LocalDateTime.toString()` 은 초가 0이면
> 생략해 `2026-03-01T09:00` 을 주는데 배치는 언제나 `2026-03-01T09:00:00` 으로 적는다.
> 같은 시각인데 문자열이 다르다 — DB 에서 시각으로 정규화해 비교한다.
>
> **잡 이름도 함께 본다.** 안 그러면 질문이 *"누구든 그 네 이름의 파라미터를 그 값으로 쓴 적
> 있나"* 로 넓어진다. 지금 `expireJob` 은 `asOf` 하나만 써서 우연히 안 걸릴 뿐이다.

---

## API 를 열어도 아무 때나 못 돈다 — `rejectRunningExpire`

`startRunStep` 의 가드 여덟 중 하나가 **만료가 도는 중이면 거절**한다.

```java
rejectRunningExpire(runningJobs);   // RunningJobProbe.blockingExecutions("expireJob")
```

만료가 도는 동안 검증하면 **판정 근거가 검증 중에 바뀐다** — 만료는 재고를 쓰는 유일한
잡이고, `dataset_fingerprint` 재료에 `sum(active_count)` 와 `max(updated_at)` 이 들어 있다.

**근거는 배치 메타다.** `JobRepository.findRunningJobExecutions("expireJob")` 이
`STATUS IN ('STARTING','STARTED','STOPPING')` 인 행을 준다(6.0.4 바이트코드로 확인).

### 이 가드는 정확성의 근거가 아니다

통과한 **직후**에 만료 크론이 발화하면 이 검사는 못 막는다. 그 자리를 지키는 것은
`assertFrozenStep` 이다 — 규칙이 다 돈 뒤에 발급건·재고·회차 정책·이력 네 축을 다시 보고,
하나라도 움직였으면 `DATASET_MUTATED_DURING_RUN` 으로 **판정을 버린다.**

그러면 이 가드는 무엇을 하나 — **헛돌지 않게 한다.** 3분 뒤에 버릴 실행을 시작 전에 끝내고,
`attempt` 를 태우지 않는다.

### 그래서 배제는 양방향이다

검증만 만료를 피하면 그 창이 **예외가 아니라 대부분**이 된다. 검증 소요가 수 분인데 만료
크론이 5분이므로, 겹칠 확률은 *"지금 만료가 도는가"*(수 초/주기 ≈ 2%)가 아니라
**"내 실행 시간 안에 크론이 발화하는가"**(≈ 60%)다.

그리고 겹치면 그 `asOf` 는 **영구히 못 쓴다.** 만료가 찍은 `updated_at` 은 지워지지 않으므로
`rejectIssuancesUpdatedAfterAsOf` 가 그 `asOf` 이하로는 영원히 참이다. 재시도해도 같다 —
`asOf` 를 만료 시각 뒤로 올려야만 지나간다.

그래서 **만료 스케줄러가 검증이 도는 중이면 그 슬롯을 건너뛴다.**

```java
// ExpireScheduler.expire()
List<Long> verifying = runningJobs.blockingExecutions(VerifyJobConfig.JOB_NAME);
if (!verifying.isEmpty()) { log.warn(...); return; }
```

**대상이 유실되지는 않는다.** 스케줄러는 주기마다 `asOf` 를 새로 잡고 만료는
`expires_at < asOf` 를 `id > 0` 부터 훑으므로, **건너뛴 슬롯의 몫을 다음에 도는 슬롯이
통째로 가져간다.**

**그래도 상한을 둔다** — `batch.schedule.max-expire-skips`(기본 1). 한때 여기에
*"최대 지연이 정확히 크론 주기 하나"* 라고 적었는데 **거짓이었다**: 매 슬롯마다 다시 묻기
때문에 검증이 주기보다 오래 돌면 그 사이 슬롯이 **전부** 죽는다. 300만에서의 검증 소요는
아직 실측 전이라(`docs/13` §6 의 D) 그 지연의 상한을 모른다.

상한을 두면 최대 지연이 **`(상한 + 1) × 크론 주기`** 로 **구조적으로** 정해진다 — 실측이
필요 없다. **배치 창으로 옮긴 뒤(CY-397)에도 기본값은 `1` 이다.**

> 한때 `0` 으로 내렸다. 근거는 *"일 1회에서 `1` 이면 최대 지연이 이틀이라 SLA 를 48시간 위로
> 올려야 하고, 그러면 만료가 안 도는 것을 이틀 뒤에나 안다"* 였고, 겹침은
> *"일정 분리(만료 04:10 · 검증 05:00)가 막는다"* 였다. 그때는 **그 검증 크론이 없었다** —
> 검증을 띄우는 유일한 경로가 이 문서가 설명하는 **손 트리거**이고, 그것은 시각을 안 가린다.
> 게다가 `batch.schedule.zone` 이 `UTC` 라 04:10 은 **13:10 KST**, 즉 시연·리허설 시간대다.
>
> 막는 것이 없는데 배제를 끄면, 뚫린 검증의 `asOf` 는 `rejectIssuancesUpdatedAfterAsOf`
> 때문에 **영구히 못 쓴다** — 재시딩 말고 복구가 없다. 반대편 대가인 "만료가 하루 밀린다" 는
> 다음 슬롯이 밀린 대상을 함께 가져가므로 되돌릴 수 있다. **되돌릴 수 없는 쪽을 지켰고,**
> 그 값으로 만료 SLA 를 180,000초(50시간)로 뒀다.
>
> **CY-470 이 그 크론(`VerifyScheduler`, 05:00 UTC)을 세우면서 둘을 되돌렸다** —
> `max-expire-skips` 는 `0`, 만료 SLA 는 90,000초(25시간)다.
>
> ⚠️ **그래서 손 트리거는 이제 배치 창(04:10 · 04:30 · 05:00 UTC)을 반드시 피해서 건다.**
> 상한이 `0` 이라 겹치면 **첫 충돌에서 만료가 그대로 지나가고**, 그 검증은 버려진다.

**상한을 넘으면 만료를 돌린다.** 재고는 운영의 진실이고 검증 실행은 진단이다 — 둘 중 하나를
버려야 하면 진단 쪽이다. 그 선택은 ERROR 로 남는다. 긴 전수 검증은 여전히 스케줄러를 끄고
돌리는 것이 맞다.

**건너뛸 때 대기 지표는 안 건드린다.** 한때 여기서 `markUnknown` 을 불렀다 — 관측이
`afterJob` 리스너라 *잡이 안 뜨면 안 불려* 게이지가 직전 실행 값에 얼어붙었기 때문이다.
**CY-421 이 관측을 되읽기로 옮기면서 그 근거가 사라졌다**: 게이지는 이제 *"마지막으로 성공한
실행이 남긴 몫"* 이고, 슬롯을 건너뛰어도 그 값은 여전히 사실이다. 건너뛴 슬롯은 그 WARN
로그와 `ExpireNotSucceeding` 이 진다.

**그래도 마이크로초짜리 창은 남는다** — 양쪽 검사 사이. 그 자리는 `assertFrozenStep` 이
받는다. 구조적으로 없애는 것은 일정 분리인데, **검증 크론(05:00)이 아직 없어 지금은 그 축이
서 있지 않다** — `docs/13` §6 의 D 몫이다.

### 죽은 실행을 어떻게 가려내나 — 나이가 아니라 진도

위 질의에는 `END_TIME` 검사도 시간 상한도 **없다.** 프로세스가 종료 표시를 못 남기고 죽으면
`STARTED` 행이 영원히 남고, 상한이 없으면 `docker compose down` 이 만료 한복판에 한 번
걸린 것만으로 **검증이 그 뒤로 영영 거절된다.** 만료 실행에는 해제 경로도 없다 —
`abandon` 엔드포인트는 `verifyJob` 으로 필터링돼 있다.

**그런데 "시작한 지 오래됐다" 는 "죽었다" 가 아니다.** 처음엔 임계로
`batch-alerts.yml` 의 `BatchJobRunningTooLong`(300초)을 그대로 썼는데, **그 논거는 거꾸로였다** —
그 알림이 읽는 `spring_batch_job_active_seconds_max` 는 JVM 안의 게이지라, 그것이 우는 상태는
정의상 **"살아서 느리게 돌고 있다"** 이다. 나이로 자르면 만료가 밀려 오래 도는 날 —
**가드가 가장 필요한 날** — 가드가 스스로 꺼진다. 300만 적재 직후 첫 만료가 그 상황이다.

**그래서 진도를 본다.** `SimpleJobRepository.update(StepExecution)` 이 청크 커밋마다
`BATCH_STEP_EXECUTION.LAST_UPDATED` 를 다시 찍는다(6.0.4 바이트코드로 확인). 살아 있는 잡은
이 값이 계속 앞으로 가고, 하드킬된 잡은 죽은 순간에 멈춘다. `expireStep` 은
`RepeatStatus.CONTINUABLE` 로 청크마다 트랜잭션을 끊으므로 실제로 움직인다.

추가 질의는 없다 — `SimpleJobExplorer.findRunningJobExecutions` 가
`fillStepExecutionDependencies` 까지 돌려 **StepExecution 을 채워서 준다**(같은 방법으로 확인).

| | 값 | 뜻 |
|---|---|---|
| `BatchJobRunningTooLong` | 300초 | **살아서** 느리게 돌고 있다 |
| `batch.stuck-job-after-ms` | 1,800,000ms | 진도가 멈췄다 — 죽은 것으로 본다 |

**뒤가 앞보다 반드시 커야 한다.** 같은 숫자를 쓰면 *"살아 있다"* 와 *"죽었다"* 가 한 값을
공유한다. `.example` 에 그 제약을 적어 뒀다.

`STARTING` 에서 죽어 Step 이 하나도 없는 행은 실행 시작 시각(없으면 생성 시각)으로 물러난다.
그 갈래가 없으면 그런 행이 영원히 막는다.

> ⚠️ **무시했다는 사실을 알리는 것은 WARN 로그뿐이다.** 위 알림은 프로세스가 죽으면 지표째
> 사라져 시체를 못 잡는다. 지표·알림 축은 `docs/13` §6 의 B 가 진다.

**컨트롤러가 그것을 미리 보고 답한다.** 잡을 띄운 뒤 Step 안에서 죽으면 클라이언트는 202 를
받아 놓고 폴링해야 원인을 안다 — 시작조차 못 할 것이 뻔한 요청은 **접수 단계에서 거절**하는
편이 낫다. 다만 가드 자체는 그대로 둔다. 컨트롤러 검사는 편의이고, **진실은 잡 안에 있다.**

---

## 하드킬로 남은 실행을 걷어내는 경로 — `abandon`

`getRunningExecutions` 는 인메모리가 아니라 **DB 의
`STATUS IN ('STARTING','STARTED','STOPPING')`** 을 본다. 그래서 SIGKILL·OOM 으로 프로세스가
죽으면 `STARTED` 행이 `END_TIME` 이 `NULL` 인 채 영구히 남고, **그 뒤 모든 트리거가 429** 가
된다. 재기동으로도 안 풀린다 — 판정 기준이 프로세스가 아니라 DB 이기 때문이다.

`stop` 으로도 못 푼다. 상태가 `STOPPING` 이 되어 위 목록에 그대로 있다 — 살아 있는
프로세스가 그 신호를 받아 종료시켜 줘야 하는데 그 프로세스가 이미 없다.

```bash
POST /api/v1/admin/verify/runs/{id}/stop      # STARTED → STOPPING
POST /api/v1/admin/verify/runs/{id}/abandon   # STOPPING → ABANDONED
```

`abandon` 은 **중단된 것만**(`STOPPING`·`STOPPED`) 버린다. 살아 있을지도 모르는 것을
함부로 버리지 않고, 이미 끝난 `FAILED`·`COMPLETED` 도 안 건드린다 — 그것을 `ABANDONED` 로
덮어쓰면 **실행 이력이라는 판정 근거가 조용히 바뀐다**. 그리고 끝난 실행은 애초에 트리거를
막지 않는다.

> Spring Batch 자체는 `status.isLessThan(STOPPING)` 일 때만 거부해서 `FAILED` 까지
> 통과시킨다. 그 위에 우리 조건을 하나 더 얹은 것이다.

**하드킬 뒤에는 `stop` 한 번에 곧바로 `STOPPED` 가 된다** — 신호를 받아 줄 잡이 이미
없기 때문이다(실측). `STOPPING` 은 살아 있는 잡이 청크 경계를 기다리는 동안의 상태다. 이것은 정상 절차가 아니라 **복구**이고, 버린 실행이 남긴
`verification_runs` 행은 `verdict` 없이 열린 채 남는다(되읽기가 `verdict IS NOT NULL` 로
안 집는 것이 맞다).

**막고 있는 `executionId` 는 `GET /runs/running` 으로 얻는다.** 429 응답 본문에는 안 싣는다 —
저장소 규약이 *"클라이언트에 나가는 문구는 `errorCode.getMessage()`"* 로 못 박았고, 인증이
없는 API 라 자유 문장에 내부 값을 담지 않는 편이 맞다. 그 id 없이는 `stop`·`abandon` 을
부를 방법이 없으므로 **조회 경로를 따로 연다.**

응답은 성공도 실패도 `ResponseEnvelope` 다. 이것도 규약이고, batch 가 그것을 쓰려면
`core` 에 있어야 해서 이 티켓이 `api` 모듈에서 옮겼다.

---

## 만료 쪽에도 복구 경로를 냈다 — `expireJob` (CY-429)

`BatchStuckExecution` 은 **울리는데 처방이 손 SQL 뿐이었다.** 위 두 엔드포인트는
`verifyJob` 으로 필터링돼 있고, 만료는 `docs/13` §6 의
`UPDATE BATCH_JOB_EXECUTION ... VERSION = VERSION + 1` 을 사람이 직접 쳐야 했다.

```bash
GET  /api/v1/admin/expire/runs/stuck            # 진도가 멈춘 실행 + 얼마나 멎었는지
POST /api/v1/admin/expire/runs/{id}/recover     # 한 번. 재시도해도 안전하다
```

### 안전 근거는 `VERSION` 이 아니라 판정이다

**손 SQL 과 이 API 는 같은 성격의 쓰기를 한다** — 둘 다 `VERSION` 을 올린다
(API 는 선점 UPDATE + `recover` 의 갱신으로 JOB 행 기준 **+2**, 손 SQL 은 +1)
(`JdbcJobExecutionDao` 의 `UPDATE ... VERSION = VERSION + 1 WHERE ... AND VERSION = ?`).
그러니 *"API 는 `VERSION` 을 안 올려서 안전하다"* 는 말은 거짓이다.

다른 것은 하나다 — **선행 조건이 코드에 있다.** `RunningJobProbe.stuckExecutions` 가
시체로 판정한 실행에만 그 쓰기를 한다. 손 SQL 은 그 임계를 사람이 SQL 에 직접 적어야
했고, 그 숫자가 코드와 갈리는 순간 운영자가 **살아 있는 만료를 걷어낸다.**
따라서 **`batch.stuck-job-after-ms` 를 내리는 변경은 이 API 의 안전을 직접 깎는다.**

판정은 **나이가 아니라 진도**다(위 "죽은 실행을 어떻게 가려내나" 절과 같은 근거).

### 왜 `abandon` 이 아니라 `recover` 인가

`JobOperator.recover` 가 **정확히 이 용도로 있다**(6.0.4). 돌던 `StepExecution` 을 전부
`FAILED` + `END_TIME` 으로 닫고, `JobExecution` 도 그렇게 닫고, 실행 문맥에
`batch.recovered` 를 남긴다.

> **그 플래그는 멱등의 근거로 못 쓴다.** `JobRepository.update(JobExecution)` 이 문맥을
> 영속하지 않아 DB 에 안 남고(별도 메서드 `updateExecutionContext` 가 그 일을 한다),
> 억지로 영속시키면 **다음 실행이 그것을 물려받는다** — `TaskExecutorJobLauncher` 가 새
> 실행을 만들 때 직전 실행의 문맥을 그대로 복사한다(6.0.4 바이트코드). 그러면 재실행이
> 시체가 돼도 복구가 아무것도 안 한 채 200 을 낸다. 그래서 `ExpireRecoveryService` 는
> **실행 상태**(`FAILED` + `END_TIME`)로 판정한다 — 실행마다 새로 생기는 값이라 안 상속된다. 처음에는 위 verify 모양을 그대로 옮겨 `stop → abandon`
2단계로 지었는데, 리뷰가 이 메서드를 짚었다. 넷이 낫다.

| | 2단계 `abandon` | `recover` |
|---|---|---|
| 호출 | `stop` → `abandon` | **한 번** |
| 재시도 | 두 번째가 409, 문구가 상황과 반대로 나감 | **실행 상태로 멱등**(200) |
| 동시 요청 | 낙관적 락에 기댔다(Step 없는 실행엔 검사 0개) | **조건부 갱신의 affected rows** |
| 결과 | `ABANDONED` — 그 `asOf` 슬롯 영구 소각 | **`FAILED` — 그 문을 안 닫음** |
| Step 행 | `STARTED` 로 남음 | **함께 닫힘** |

`ABANDONED` 가 되돌릴 수 없는 근거는 `TaskExecutorJobLauncher` 다 — 마지막 실행이
`COMPLETED` **또는 `ABANDONED`** 면 `JobInstanceAlreadyCompleteException` 을 던진다.
만료는 `asOf` 가 식별 파라미터라 그 크론 슬롯이 통째로 사라진다.

> `stop` 은 **언제나 `STOPPING` 을 남긴다** — `SimpleJobOperator.stop` 이
> `setStatus(STOPPING)`·`setExitStatus(STOPPED)`·`setEndTime(now)` 를 함께 쓴다(6.0.4
> 바이트코드). 한때 이 문서와 두 컨트롤러 주석이 *"하드킬이면 곧바로 `STOPPED`"* 라고
> 적었는데, `STATUS` 와 `EXIT_CODE` 를 혼동한 것이었다.

### ⚠️ 트리거는 열지 않는다

이 컨트롤러에 **만료를 띄우는 엔드포인트를 추가하면 안 된다.** CY-421 이
`ExpirePendingRefresher` 의 조회를 `asOf` 가 아니라 `END_TIME` 정렬로 바꾼 **유일한 근거가
"만료 손 트리거가 이 저장소에 없다"** 이다. 트리거가 생기면 과거 `asOf` 실행이 나중에 끝날
수 있고, 그때 게이지가 **더 좁은 창의 더 작은 값**을 내 관제가 그것을 *"밀린 것이 없다"* 로
읽는다. `ExpireRecoveryTest.exposesExactlyTheRecoveryEndpoints` 가 이 컨트롤러의 매핑
**전체**를 단언하므로, 엔드포인트를 더하는 티켓은 그 목록을 고치며 이 결정을 함께 본다.

### 제출물을 뜨는 절차

`scripts/dump-verify-report.sh` 가 `CLEAN FULL` 과 `CORRUPT FULL` 을 떠서
**`reports` 브랜치**의 `verify/YYYY-MM-DD-{dataset}-{scope}-run{runId}.json` 으로 쌓고,
**바뀐 날만** 커밋한다.

**파일명에 `runId` 가 있는 것이 중요하다.** 날짜만 쓰면 같은 날 판정이 둘 날 때 뒤엣것이
앞엣것을 덮는다 — 오전 FAIL 을 고쳐 오후 PASS 가 나면 그날 트리에는 PASS 만 남는다.
`runId` 를 넣으면 **실행 하나가 파일 하나**라 덮어쓸 일이 없고, 같은 실행을 두 번 떠도
이름과 내용이 같아 `cmp` 가 조용히 건너뛴다.

```bash
bash scripts/dump-verify-report.sh              # 커밋까지만
REPORT_PUSH=1 bash scripts/dump-verify-report.sh   # 밀기까지
```

**작업 브랜치가 아니라 전용 worktree 에 쌓는다.** 예약 작업이 사람의 작업 트리를 쓰면
`git commit` 이 **인덱스 전체**를, `git push` 가 **브랜치 전체**를 밀어 버린다 — 사람이 그
순간을 못 본다. 브랜치를 **빈 뿌리**로 시작해 코드 히스토리와 섞이지 않게 한다.

**디렉터리 이름이 `verify/` 인 것은 브랜치명 때문이다.** `reports/` 로 두면
`git log reports` 가 브랜치인지 경로인지 몰라 *"애매한 인자"* 로 죽는다.

**`curl -sSf` 인 것이 중요하다.** `-s` 만 쓰면 4xx/5xx 에서 조용히 실패하는데, `>` 리다이렉션은
그것을 신경 쓰지 않고 **에러 본문을 제출물 파일에 박는다.** 배치가 안 떠 있을 때와 404 일 때
파일이 안 생기는 것을 확인했다. 200 인데 **본문이 잘린** 경우는 `jq` 가 걸러 낸다.

**"오늘 안 돈 판정" 을 반드시 막는다.** `/reports/latest` 는 *가장 최근에 닫힌* run 을 주고
**언제 닫혔는지는 안 본다** — `SELECT_LATEST_CLOSED` 에 시간 하한이 없다. 오늘 검증이 안 돌면
**어제 판정**이 오는데, 그것을 오늘 커밋하면 *"이 날 검증이 돌았다"* 를 주장하면서 그 주장을
확인하지 않는 셈이다.

**날짜가 아니라 나이로 잰다.** 한때 `finishedAt` 앞 10자를 `date -u +%F` 와 비교했는데,
그 창은 **폭이 0~24시간으로 변한다**:

| | |
|---|---|
| `00:05 UTC` 판정을 `23:55 UTC` 에 뜬다 | **통과한다** — 24시간 된 판정이 오늘 것으로 커밋된다 |
| `23:58 UTC` 판정을 `00:02 UTC` 에 뜬다 | **거부한다** — 4분 된 판정인데 |
| 예약을 `09:00 KST`(= `00:00 UTC`)로 옮긴다 | **매일 실패한다** — 스케줄 시각 하나로 스크립트가 죽는다 |
| 두 조합이 자정을 걸친다 | 성공한 쪽까지 버린다(all-or-nothing) |

지금은 `NOW` 를 **한 번만** 재고 `REPORT_MAX_AGE`(기본 6시간)와 비교한다. 미래 시각도
거부한다 — 컨테이너 시간대가 어긋나면 그렇게 들어오는데, 통과시키면 그 어긋남이 영영 안 보인다.

**`asOf` 도 같이 본다.** `finishedAt` 만 보면 *"과거 데이터를 오늘 재실행한 판정"* 이 통과한다.
`attempt` 를 바꿔 결정론을 확인하는 것이 이 과제의 핵심 실험이라 실제로 일어나는 일이고,
그날 정기 배치가 실패했으면 **어제 데이터의 판정이 오늘 증적으로 커밋된다** — 막으려던 위증 그 자체다.

**시간축은 실측했다.** `finished_at` 은 DDL 에 `DEFAULT` 절이 없고 앱이 `TimeProvider` 로 쓴다.
컨테이너 `TZ=UTC`, MySQL `time_zone=+00:00`. 그래서 DB 세션 시간대를 타지 않는다.

**같은 판정을 다시 뜨면 커밋하지 않는다.** 매일 파일이 바뀌면 그 커밋들이
*"매일 뭔가 달라졌다"* 로 읽힌다 — 달라진 날만 남아야 diff 가 뜻을 갖는다.

**예약은 호스트 `launchd` 다.** 검증 FULL 이 05:00 UTC(= 14:00 KST)에 도므로 14:30 KST 에 건다.
컨테이너가 아니라 호스트인 이유는 `docs/16` §2·§3 에 있다 — 요약하면 **이 저장소가 PUBLIC 이라
배치 컨테이너에 GitHub 자격증명을 넣지 않기로 했고**, 호스트에는 이미 있다.

> #### ⚠️ 이 증적이 말하지 **않는** 것
>
> **커밋이 없는 날을 "검증이 실패했다" 로 읽으면 안 된다.** 커밋 로그에서 이 넷이 전부 같은
> 모양이다 — 검증이 안 돈 날 · 머신이 꺼져 있던 날 · 배치가 안 떠 있던 날 · 판정이 어제
> 것뿐이라 스크립트가 거부한 날. **없음은 실패의 증거가 아니다.**
>
> 실패를 남기는 축은 이 스크립트 몫이 아니라 `docs/16` 몫이다. 그것이 필요해지면
> 거기 §2·§3 을 다시 본다.
>
> **`REPORT_PUSH` 의 기본이 0 인 것도 같은 이유다.** 무인 푸시는 사람이 한 번 켜야 한다 —
> 켜지 않은 상태에서 로컬 커밋만 쌓이는 것을 *"올라가고 있다"* 로 읽지 말 것.

#### 판정 값이 세 가지인 이유

`manifest.matches` 는 `true`·`false`·**`null`** 이다. 게이트는 `matches == true` 로 읽는다.

`null` 은 *"못 쟀다"* 다 — 정답 묶음이 사라져(시드 재주입) 대조가 성립하지 않은 경우다.
한때 이것을 `false` 로 접었는데 **그것이 반대쪽 거짓말이었다**: 정답이 사라진 것과 검증기가
틀린 것이 한 값이 되고, 제출물이 `verdict=PASS` 와 `matches=false` 를 **같은 본문에** 싣는다.
그것을 보는 사람은 어느 쪽을 믿을지 알 수 없다. 이유는 `present` 에 남는다.

같은 이유로 대조를 못 한 상태에서는 **수치 넷이 전부 `null`** 이다. `expectedCount=0` 은
*"정답 묶음이 사라졌다"* 와 *"정답이 0건인 시드다"* 를 뭉치고, `missingCount=0` 을 보는 쪽은
**그것을 합격으로 읽는다.** 레코드 생성자가 그 조합 자체를 거부한다.

#### 조회 하나가 트랜잭션 하나다

`latest()` 에 `@Transactional(readOnly = true)` 가 걸려 있다. 없으면 다섯 SELECT 가
autocommit 으로 각자 스냅샷을 열고, 그 사이 `expected_findings` 가 바뀌면 **한 응답 안에서
앞뒤가 다른 것을 대조한 결과**가 나온다 — `missing` 은 옛 정답 기준, `unexpected` 는 새 정답
기준이 되어 *"검증기가 800건을 오탐했다"* 가 `verdict=PASS` 옆에 실린다. 이 DB 는
`REPEATABLE-READ`(실측)라 한 트랜잭션이면 다섯이 한 스냅샷에 묶인다.

**`exists` 만으로는 부족하다.** 그것은 한 순간만 보므로 그 뒤의 변경을 못 막는다. 둘 다 있어야 한다.

#### 무엇까지 실제로 재 봤나

**한 번 이 자리에서 과장했다.** `launchctl` 이 `runs=1 / exit 0` 을 보여 준 것을 두고
*"예약이 실제로 돌았다"* 고 적었는데, 그 종료코드 0 은 옛 스크립트의 **브랜치 가드가 건너뛴
것**이었다 — `curl` 도 `git` 도 그때 안 돌았다. 그래서 이번에는 구간을 갈라 적는다.

| 구간 | 어떻게 쟀나 | 결과 |
|---|---|---|
| launchd → bash → 스크립트 | `launchctl kickstart -k` | **돈다.** `runs` 가 오르고 로그에 스크립트 출력이 남는다 |
| worktree 생성·빈 뿌리 | 같은 실행 | `../cy-be-reports` 가 생기고 `reports` 가 부모 0으로 시작한다 |
| 브랜치를 다른 worktree 가 잡은 경우 | 일부러 잡아 두고 실행 | **어디가 잡았는지 말하고** 종료코드 1 |
| `compose exec` → 컨테이너 안 `curl` | 배치를 띄우고 실행 | **닿는다.** 봉투 씌운 404 를 받고 `-sSf` 가 종료코드로 만든다 |
| 404·미기동·JSON 잘림·어제 판정 | 각각 재현 | 넷 다 **파일 0개 · 종료코드 1** |
| 오늘 판정 → 커밋 → 재실행 시 건너뛰기 | 고정 응답(fixture) | 파일 2개 + 커밋, 두 번째는 `cmp` 로 건너뛴다 |
| **성공 경로를 살아 있는 배치로** | — | **못 쟀다.** 오늘 닫힌 판정이 있어야 하고, 그러려면 시드 적재 + FULL 1회가 먼저다 |

**마지막 줄이 남은 구멍이다.** 그 앞 구간이 전부 실측이라 위험은 작지만, *"돌 것이다"* 와
*"돌았다"* 는 다르다 — D13 전에 판정을 한 번 낸 뒤 이 표의 마지막 줄을 채운다.

### 에러 형식이 두 벌이 되지 않게

`BatchApiExceptionHandler` 는 `assignableTypes` 로 컨트롤러를 **이름으로** 묶는다. 그 javadoc 이
*"다른 컨트롤러가 생기는 날 그쪽 규약까지 이 클래스가 지게 된다"* 고 예고했고, 그날이 왔다.
**패키지 전체로 넓히지 않는다** — 넓히면 다음 컨트롤러가 이 규약을 의식하지 않고도 따라오는데,
그때 어긋나는 것은 *"에러 형식이 두 벌"* 이라 클라이언트에서만 드러난다.

> **⚠️ 그 장치가 한 번 안 통했다.** CY-590 의 `VerifyReportController` 가 등록 없이 들어왔고,
> 404 로 설계한 `RUN_NOT_FOUND` 가 **500 + 스프링 기본 본문**으로 나갔다. 이 문단도,
> 그 javadoc 도 사람에게 거는 기대였고 **아무 테스트도 안 막았다.**
>
> 지금은 `BatchApiExceptionHandlerCoverageTest` 가 `assignableTypes` 배열과 실제 매핑된
> `batch.api` 컨트롤러 집합이 **같은지**(부분집합이 아니라 등식) 기계로 확인한다.
> 등록을 빼는 돌연변이로 빨개지는 것을 확인했다.

### `cleanupJob` 은 아직 없다

`BatchStuckExecution` 은 `Job` 빈 **셋 전부**에 뜨는데(`BatchRunMetrics` 가 `List<Job>` 에서
이름을 모은다) 컨트롤러는 둘이다. `cleanupJob` 시체는 여전히 `docs/13` §6 의 손 SQL 이
유일한 길이고, 알림 description 이 그 사실을 명시한다. 잡 이름을 경로 변수로 받는 형태로
일반화하는 것은 별도 티켓이다 — 그때 "트리거는 열지 않는다" 규율도 함께 옮겨야 한다.

---

## 검증 계약

이 티켓이 초록이라고 말하려면 아래가 각각 **깨졌을 때 빨개져야** 한다.

- **비동기** — `start` 가 즉시 반환하는 것. 전용 실행기를 동기로 바꾸면 응답이 늦어지는데,
  그건 타이밍이라 단언이 어렵다. 대신 **`ExpireScheduler` 가 여전히 동기인 것**을 본다 —
  공용 빈이 오염되면 그쪽이 깨진다
- **격리** — `verifyJobOperator` 와 공용 `JobOperator` 가 **다른 인스턴스**인 것
- **거절** — 도는 실행이 있으면 429. 실행기의 거절 예외가 컨트롤러에 안 오므로
  접수 단계 선검사와 반환값 검사 둘이 그것을 만든다
- **입력** — 모르는 enum 값·오프셋 붙은 `asOf`·미래 `asOf`·`attempt < 1`·`fromTs`·
  `seedRunId` 누락(CORRUPT)이 전부 400 (500 이 아니다)
- **실패 원인** — 가드에 걸린 실행의 `failure` 에 **어느 Step 에서 왜** 죽었는지가 한 줄로
  실리는 것. `getAllFailureExceptions()` 는 DB 조회 객체에서 언제나 비어 있어 쓰면 안 된다
- **잡 격리** — `expireJob` 의 `executionId` 를 이 경로로 조회·중단하면 404
- **`attempt` 소진** — 가드에 걸려 죽은 뒤 **같은 요청을 다시 부르면 접수되는 것**
- **`runId` 의 뜻** — 시작 직후 조회는 `runId: null`, 판정 뒤에는 값이 있다.
  가드에 걸려 죽은 실행은 **끝까지 `null`** 이다
- **`asOf` 누락** — 400. 서버가 임의로 채우지 않는다
- **`attempt` 자동 배정** — 시드가 점유한 번호를 피한다
- **스케줄러 켜짐** — 409. 그리고 그 검사를 지워도 **잡 안의 가드가 여전히 막는 것**
- **노출** — 기본 조합에 `batch` 포트가 **없는** 것, 그리고 오버레이를 얹어도 `127.0.0.1` 인 것

---

**CY-429 (만료 복구)** — 아래도 깨지면 빨개져야 한다.

- **시체 판정** — 진도가 도는 만료 실행에 `recover` 를 부르면 409/`EXPIRATION-007`,
  상태가 그대로다(`VERSION` 도 안 움직인다)
- **멱등** — 이미 걷어낸 실행에 다시 불러도 이력이 다시 안 쓰인다
- **`FAILED` 로 닫는다** — `ABANDONED` 면 그 `asOf` 슬롯을 영원히 못 돌린다.
  돌던 Step 행도 함께 닫혀야 한다
- **잡 격리** — `verifyJob` 의 `executionId` 를 `/expire/` 경로로 부르면 404,
  없는 번호와 **같은 코드**
- **Step 없는 실행** — 표시(`lastProgress`)가 판정과 같은 폴백(`START_TIME`)을 따른다
- **트리거 부재** — 이 컨트롤러의 매핑이 조회 하나 + 복구 하나뿐이다(CY-421 의 정렬 근거)
- **동시 요청** — 같은 실행에 요청 둘이 동시에 와도 실제 쓰기는 **정확히 하나**다.
  Step 행이 없는 실행에서도 그렇다 — 근거는 조건부 갱신의 `affected rows` 이지 낙관적
  락이 아니다(`update(JobExecution)` 은 쓰기 직전 `synchronizeStatus` 로 버전을
  재동기화해 `WHERE VERSION = ?` 이 항상 통과한다. 버전 검사는 `update(StepExecution)`
  뿐이라 Step 없는 실행엔 검사가 0개다)
- **실행 중 상태 셋** — `STARTING`·`STARTED`·`STOPPING` 을 모두 걷는다.
  선점문의 목록에서 하나만 빠져도 그 상태의 시체가 조용히 안 걷힌다
- **정상 완료는 대상이 아니다** — `COMPLETED` 에 `recover` 를 부르면 409 다.
  200 이 나가면 실행 번호 오타 한 자리가 진짜 시체를 놓치게 만든다

## 남긴 것

| 무엇 | 왜 |
|---|---|
| **인증·인가** | PRD 보안 ①이 `/api/v1/admin/**` 에 `ADMIN` 역할을 요구한다. batch 에 Spring Security 가 없고, 토큰 발급·검증은 영역 ③(인증)의 몫이라 여기서 규약을 혼자 정하면 두 벌이 된다. **이 티켓은 노출을 줄이는 데까지만 간다.** ⚠️ **그 축소가 막는 것은 인터넷뿐이다** — compose 서비스가 같은 기본 네트워크를 쓰므로 `docker compose exec prometheus curl http://batch:9090/...` 는 그대로 통한다. 내부 신뢰 경계는 없다 |
| **`INCREMENTAL` scope** | `rejectUnsupportedScope` 가 막는 상태 그대로 둔다. 여는 것은 증분 검증 티켓 |
| ~~**리포트 덤프**~~ | **CY-590 이 했다** — `GET /reports/latest`. 이 티켓의 `/runs/{executionId}` 는 **배치 실행**이 어떻게 됐나이고, 그쪽은 **그 실행이 낸 판정**이다. `docs/10` 이 "두 얼굴" 로 가른 그 둘이다 |
| **진행률** | `GET` 이 Step 단위 진행을 안 준다. 300만 전수라 사람이 궁금해할 값인데, 지금 지표로 그 축이 없다 |
