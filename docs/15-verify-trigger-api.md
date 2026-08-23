# CY-368 · 검증 배치를 온디맨드로 트리거하고 결과를 조회한다

`verifyJob` 을 사람이 돌릴 수 있게 한다. 지금은 **batch 에 `@RestController` 가 0건**이라
검증을 손으로 시작할 방법이 테스트 말고 없다 — CY-359 가 시연 절차에서 막힌 자리다.

| | |
|---|---|
| **한다** | `POST /api/v1/admin/verify` — 202 + `executionId` |
| **한다** | `GET /api/v1/admin/verify/runs/{id}` — 판정·검출 건수·상태 |
| **한다** | `POST /api/v1/admin/verify/runs/{id}/stop` — 실행 중단 |
| **한다** | 업무 포트 노출 결정 — `application.yml.example` 이 이 티켓에 예약해 뒀다 |
| **안 한다** | 인증·인가 — batch 에 Spring Security 가 없다. 아래 "남긴 것" |
| **안 한다** | 리포트 덤프 — 별도 티켓(영역 ④ 범위표의 "리포트") |

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
> *"일정 분리(만료 04:10 · 검증 05:00)가 막는다"* 였다. **그 검증 크론이 아직 없다** —
> 검증을 띄우는 유일한 경로가 이 문서가 설명하는 **손 트리거**이고, 그것은 시각을 안 가린다.
> 게다가 `batch.schedule.zone` 이 `UTC` 라 04:10 은 **13:10 KST**, 즉 시연·리허설 시간대다.
>
> 막는 것이 없는데 배제를 끄면, 뚫린 검증의 `asOf` 는 `rejectIssuancesUpdatedAfterAsOf`
> 때문에 **영구히 못 쓴다** — 재시딩 말고 복구가 없다. 반대편 대가인 "만료가 하루 밀린다" 는
> 다음 슬롯이 밀린 대상을 함께 가져가므로 되돌릴 수 있다. **되돌릴 수 없는 쪽을 지켰고,**
> 그 값으로 만료 SLA 를 180,000초(50시간)로 뒀다. `docs/13` §6 의 D 가 검증 크론을 세우면
> 그때 `0` 으로 내리고 SLA 를 25시간으로 되돌린다.
>
> ⚠️ **그때까지 손 트리거는 04:10 UTC 를 피해서 건다.** 겹치면 그 슬롯을 한 번은 건너뛰지만
> 연속 둘째부터는 만료가 지나간다.

**상한을 넘으면 만료를 돌린다.** 재고는 운영의 진실이고 검증 실행은 진단이다 — 둘 중 하나를
버려야 하면 진단 쪽이다. 그 선택은 ERROR 로 남는다. 긴 전수 검증은 여전히 스케줄러를 끄고
돌리는 것이 맞다.

**건너뛸 때 대기 지표를 "모름" 으로 되돌린다.** `reportPending` 은 `afterJob` 리스너라
**잡이 안 뜨면 안 불린다** — 그대로 두면 게이지가 직전 실행 값에 얼어붙어, 백로그가 쌓이는
바로 그 구간에 관제가 *"밀린 것이 없다"* 를 본다.

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

## 남긴 것

| 무엇 | 왜 |
|---|---|
| **인증·인가** | PRD 보안 ①이 `/api/v1/admin/**` 에 `ADMIN` 역할을 요구한다. batch 에 Spring Security 가 없고, 토큰 발급·검증은 영역 ③(인증)의 몫이라 여기서 규약을 혼자 정하면 두 벌이 된다. **이 티켓은 노출을 줄이는 데까지만 간다.** ⚠️ **그 축소가 막는 것은 인터넷뿐이다** — compose 서비스가 같은 기본 네트워크를 쓰므로 `docker compose exec prometheus curl http://batch:9090/...` 는 그대로 통한다. 내부 신뢰 경계는 없다 |
| **`INCREMENTAL` scope** | `rejectUnsupportedScope` 가 막는 상태 그대로 둔다. 여는 것은 증분 검증 티켓 |
| **리포트 덤프** | 영역 ④ 범위표의 "리포트" 는 별도 티켓. 이 조회는 **판정 요약**이지 리포트가 아니다 |
| **진행률** | `GET` 이 Step 단위 진행을 안 준다. 300만 전수라 사람이 궁금해할 값인데, 지금 지표로 그 축이 없다 |
