# 배치 — 남은 일

주석과 문서에 흩어져 있던 "다음에 할 것"을 한곳에 모은다. **각 항목은 언제 하는지가 조건으로
적혀 있다** — "나중에" 가 아니라 무엇이 열려야 의미가 생기는지다.

---

## 0. 이 문서를 관통하는 원칙

설계에서 정한 것이고, 알림을 가르는 기준이기도 하다.

> **데이터가 틀렸다는 판정이 나와도 배치는 정상 종료다.
> 배치가 실패했다고 할 때는 판정을 내지 못한 경우뿐이다.**
>
> 이 둘을 같은 알람으로 묶으면 **서버를 고쳐야 할 상황과 데이터를 확인해야 할 상황이
> 구분되지 않는다.**

검증 배치는 이 원칙대로 서 있다 — 불일치는 `verdict=FAIL` 로 남기고 계속 가고,
판정 불가(`DATASET_MUTATED_DURING_RUN`·`RUNTIME_NOT_QUIESCED` 등 7종)일 때만 죽는다.

**만료 배치도 데이터 축에서는 이제 그렇다.** 재고가 어긋난 회차는 판정으로 남기고 배치는
정상 종료한다 — 어떻게 맞췄는지가 아래 1번이다.

> **만료가 죽는 자리는 그대로 남아 있고, 그것이 맞다.** `EXPIRE_HISTORY_COUNT_MISMATCH` 는
> <b>우리 쓰기가 어긋났다</b>는 뜻이라 판정이 아니라 사고다. `EXPIRE_ASOF_IN_FUTURE` 는 넘긴
> 파라미터가, `EXPIRE_ON_CORRUPT_SCHEMA` 는 접속 설정이 틀린 것이다. 셋 다 <i>"판정을 내지
> 못한 경우"</i> 에 든다. 정상 종료로 바뀐 것은 <b>`STOCK_*` 축, 즉 데이터가 이미 어긋나
> 있다는 판정</b> 하나다.

---

## 1. 만료 배치 — 회차 격리 (완료 · CY-347)

### 무엇이 문제였나

가드 셋이 전부 예외를 던져 **잡을 실패**시켰다. 그중 둘은 *"데이터가 이미 어긋나 있다"* 인데,
그 판정을 내고도 배치를 죽였다 — 위 원칙과 반대였다.

| 가드 | 뜻 | CY-347 전 | 지금 |
|---|---|---|---|
| `EXPIRE_HISTORY_COUNT_MISMATCH` | **우리 쓰기가 어긋났다** | 실패 | **실패 유지** |
| `STOCK_ROW_MISSING` | 재고 행 없는 회차가 섞였다 | 실패 | **성공 + 알림** |
| `STOCK_UNDERFLOW` | 재고가 이미 모자랐다 | 실패 | **성공 + 알림** |

첫째만 성격이 다르다. 넘긴 건수와 쓴 이력 수가 다르다는 것은 **이 잡이 방금 깨뜨렸다**는
뜻이라, 성공으로 넘기면 이력 없는 발급건이 커밋된다. 그건 판정이 아니라 사고다.

### 대가가 얼마나 컸나

오염 회차 **하나**가 그 뒤 id **전부**의 만료를 영구히 막았다.

- 청크 단위로 롤백되므로 **같은 청크에 실린 남의 회차까지** 되돌아갔다
  (운영 `chunk-size` 는 1000 이다)
- 진도는 JobInstance 안에서만 살고 주기마다 새 인스턴스라, 다음 주기도 `id > 0` 부터 훑다
  **같은 자리에서 죽었다** — 하루 288회, 사람이 손볼 때까지
- 만료 누락은 검증 finding 이 아니므로(설계상 관측 지표) **검증도 안 잡아 줬다**

그리고 이 프로젝트는 그 상태가 **존재한다고 전제한다** — CORRUPT 스키마가 `ck_stock_range` 를
일부러 떼고 오염 유형이 `active_count` 를 흔든다. 검증용 DB 를 보게 띄운 배치는 **확실히**
이 경로에 들어갔다. 그 자리는 이제 `CleanSchemaGuard` 가 막는다.

그 대가는 `ExpireUnderflowBlastRadiusTest` 가 단언으로 못 박고 있었다. 그 파일은
`ExpireBlockedCouponIsolationTest` 로 바뀌었고 단언이 뒤집혔다 — javadoc 이 예고한 대로다.

### 어떻게 바꿨나

**건너뛰는 단위가 청크가 아니라 회차다.** 넘긴 뒤에 재고만 빼먹으면
`EXPIRED` 인데 재고는 안 돌아온 상태가 커밋된다 — 예전에 롤백이 막고 있던 바로 그 상태다.

```
잡 시작(첫 청크)
  → 남은 대기 **전체**를 회차별로 묶어 재고가 어긋난 회차를 한 번 구한다
      (issuances 파생테이블 LEFT JOIN coupon_stocks,
       WHERE s.coupon_id IS NULL OR s.active_count < x.pending)
  → 결과를 JobExecution 세대와 함께 ExecutionContext 에 실어 그 실행 내내 재사용
  → EXPIRE_BATCH 에 AND coupon_id NOT IN (:blocked) 를 붙여 그 회차를 애초에 제외
  → 나머지는 정상 커밋. 잡은 COMPLETED
  → 제외한 회차를 메트릭·로그로 내보낸다
```

> **원래 이 문서가 지시한 "이 청크 구간에서" 를 안 썼다.** 청크 기준으로 막힘을 정의하면
> **제외한 만큼 `LIMIT` 자리가 비어 다른 회차의 행이 창 안으로 들어오는데**, 그 회차는
> 판정한 적이 없어 또 막혀 있을 수 있다 — 재고 없이 만료된 상태가 커밋된다. 남은 대기
> 전체와 견주면 제외 대상이 창 구성과 무관해져서, 밀려 들어오는 것은 언제나 성한 회차뿐이다.
> 성능을 이유로 청크 스코프로 되돌리려는 다음 사람은 이 문단을 먼저 읽어라.

> **`updated_at <= :committedAt` 캡처 창도 일부러 안 걸었다.** `committedAt` 은 청크마다
> 새로 잡혀 **뒤 청크의 창이 더 넓다.** 판정에 창을 걸면 그 틈의 행이 대기로 안 세졌는데
> 만료는 되어, 회차별 차감 합계가 판정이 본 대기를 넘고 `STOCK_UNDERFLOW` 로 죽는다 —
> 이 티켓이 없애려던 그 실패다. 창을 빼면 그 값이 **상계**가 되어 어떤 `committedAt`
> 수열에서도 부등식이 선다. 회귀 테스트는
> `BlockedCouponTest.countsRowsUpdatedAfterTheCaptureWindow` 다.

### 무엇을 함께 손대야 하나

| 대상 | 왜 |
|---|---|
| `ExpirationRepository` · `ExpirationJdbcAdapter` | ✅ 포트가 둘 늘었다(`blockedCoupons`·`countPending`). **둘 다 락을 안 잡는 읽기라 락 순서 계약 밖**이고 — 계약을 지는 것은 쓰는 셋뿐이다 — 그 성질을 `ExpirationLockScopeTest.readOnlyQueriesTakeNoLocks` 가 계측으로 지킨다 |
| `ExpireJobConfig` 태스클릿 | ✅ 가드 둘이 예외에서 **제외 + 집계**로 바뀌었다 |
| `ExpireUnderflowBlastRadiusTest` | ✅ `ExpireBlockedCouponIsolationTest` 로 개명 + 단언 반전 |
| `ExpirationErrorCode` | ✅ 두 코드의 javadoc 에 "CY-347 이후 뜻이 바뀌었다 — 도달했다면 제외 논리가 샌 것" 을 적었다 |
| `batch-alerts.yml` | ✅ 머리말의 실패 자리 목록을 새 뜻으로 고쳤다 |
| `CleanSchemaGuard` (신규) | ✅ **이 변경이 넓힌 폭을 막는다.** 아래 참조 |
| `docs/12` | ⏸ 아래 "남은 것" 참조 |

### 함께 막은 것 — 오염 스키마 가드

**회차 격리가 오염셋을 갈아엎을 위험을 넓혔다.** 예전에는 첫 오염 회차에서 잡이 죽어
그 뒤로는 아무것도 안 건드렸다. 지금은 막힌 회차만 빼고 **나머지 전부**를 넘긴다.

만료는 원본을 **쓰는** 유일한 배치이고, 오염 유형 2(`history 는 USED 인데 status 는 ISSUED`)와
7(`status 는 ISSUED 인데 활성 usages 행이 남아 있음`)은 **둘 다 `ISSUED`** 다 — 합쳐 200건.
그것이 `EXPIRED` 로 넘어가고 `EXPIRE` 이력이 붙으면 리플레이가 `USED → EXPIRED` 라는
전이표에 없는 조합을 만나 **`expected_findings` 800행에 없는 검출**이 생기고
`dataset_fingerprint` 도 움직인다.

> **나쁜 것은 모양이다.** 누락 0 · 오탐 0 이 합격 조건인데, 그것이 *"검증기가 틀렸다"* 로
> 보이는 형태로 깨진다. 실제 원인은 **만료가 한 번 지나간 것**이고 그 사실은 어디에도 안 적힌다.
> `verifyJob` 의 `rejectRunningExpire` 로는 못 막는다 — 그것은 *검증이 도는 동안* 을
> 막지, 그 **전에** 만료가 지나간 것은 못 본다.

`uk_coupon_member` 존재 여부로 가르고, 판정 근거는 `VerificationRuleRepository`
(`verifyJob` 의 `rejectDatasetMismatch` 와 **같은 것**)를 빌려 쓴다. 같은 사실을 두 곳이
각자 판정하면 둘이 어긋나는 날 어느 쪽이 맞는지 아무도 모른다.

이것은 **판정이 아니라 실패**다 — *"데이터가 틀렸다"* 가 아니라 *"여기서 돌면 안 되는 배치가
돌았다"* 이고, 고칠 곳은 데이터가 아니라 접속 설정이다.

### 검증 (완료)

- **락 범위·스캔 축** — `ExpirationLockScopeTest.keepsScanBoundedWhenExclusionFiltersCandidates`
  가 제외 목록이 **채워진** 상태로 락과 읽은 행을 잰다. 제외 술어가 `V11 (status, expires_at)`
  선택을 흔들면 스캔 축이 깨진다
- **읽기 문장이 락을 안 잡는다** — `readOnlyQueriesTakeNoLocks`. 주석으로만 적혀 있으면
  누가 `FOR SHARE` 를 붙여도 초록이라, `performance_schema.data_locks` 로 실제로 잰다
- **멱등성** — `keepsRunningOnLaterCycles`. 재고를 3 으로 두는 것이 검출력의 전부다.
  1 이면 하한 가드가 0 에서 클램프해 **이중 차감과 정상이 같은 값으로 끝난다**
- **청크 이어짐** — `ExpireBlockedCouponChunkingTest`(`chunk-size=1`). 위 두 논거는
  청크가 2개 이상일 때만 뜻이 있다
- **재시작** — `recomputesExclusionOnRestart`. Step 문맥이 복원되므로 세대를 안 보면
  "실행당 한 번" 이 "JobInstance 당 한 번" 이 된다
- **가드 도달성** — `ExpireGuardTest.failsWhenExclusionLeaksACoupon`. 이제 데이터로는
  `STOCK_UNDERFLOW` 에 갈 수 없어 제외 목록을 비우는 주입으로 도달시킨다
- **오염 스키마** — `CleanSchemaGuardTest`(`batch.config`). 한 건도 안 넘기고 죽는지, 그리고 정상
  스키마에서는 그대로 도는지 짝으로 본다(막는 쪽만 보면 **항상 던지는 가드**도 통과한다)
- **돌연변이 (실측)** — 여덟을 심어 **일곱이 검출됨**: 캡처 창 복원 · 세대 표식 무시 ·
  미래 `asOf` 그대로 집계 · `NOT IN` 절 제거 · 오염 스키마 가드 배선 제거 ·
  제외 판정을 `uk_coupon_member` 로 강제 · 만료를 `PRIMARY` 로 강제.
  **여덟째 `ORDER BY` 제거는 어떤 테스트도 깨뜨리지 못했다** — 지금 실행계획이 회차 id 순으로
  내보내기 때문이다. 깨지지 않는 단언은 두지 않았고, 그 사실을 SQL javadoc 에 적었다

### 남은 것

| 무엇 | 왜 미뤘나 |
|---|---|
| ~~`docs/12` 절대 수치 재측정~~ **검증 쪽만 완료 · CY-470** | 300만·516만에서 `verifyJob` FULL 이 **472초**다(`docs/12` §8). **만료는 아직 안 쟀다** — 그 잡은 `issuances` 를 쓰므로 돌리면 시드의 `dataset_fingerprint` 가 바뀌어 교차 검증 기준선을 잃는다. 재려면 `EXPLAIN ANALYZE` 로 리더 질의만 보거나 별도 스키마를 떠야 한다 |
| `blockedCoupons` 소요 실측 | 축소 픽스처에서 잰 값은 운영 규모를 대변하지 못한다. 코드 주석에서 근거 없는 수치를 뺐다. **실행계획 축은 `keepsBlockedCouponScanProportionalToPending` 이 잡는다** — `uk_coupon_member` 를 타면 깨진다 |
| `unexplained` 이 캡처 창 지연분을 포함한다 | `COUNT_PENDING` 은 `updated_at` 창을 안 걸어, 설계상 다음 주기로 미룬 행도 "배치가 안 한 몫" 으로 센다. **지금은 도달 불가다** — `issuances` 의 상태를 쓰는 문장이 `EXPIRE_BATCH` 하나뿐이다. `CANCEL_USE`(`USED → ISSUED`)가 붙는 취소·사용 티켓에서 가른다 |
| ↑ **CY-421 이 그 축을 알림 경로로 넓혔다** | 되읽기가 `COUNT_PENDING` 을 60초마다 다시 치므로, 실행이 끝난 **뒤에** `CANCEL_USE` 로 되돌아온 행이 새로 세어진다. 그 회차는 얼린 제외 목록에 없어 `unexplained` 로 들어가고 `ExpireLeavesWorkBehind`(critical · server)가 뜬다 — **배치는 안 틀렸는데 서버를 보라고 나가고, 만료가 일 1회라 최대 하루 간다.** `afterJob` 이 종료 시점에 한 번 세던 시절에는 구조적으로 불가능했다. 같은 티켓에서 `updated_at` 창과 함께 본다. **선행 조건이 있다** — `EXPIRE_BATCH` 의 창은 `updated_at <= :committedAt` 인데 그 `committedAt` 은 청크마다 새로 잡히고 영속되지 않아, 되읽기가 같은 창을 걸려면 마지막 청크의 값을 Step 문맥에 먼저 실어야 한다 |
| `(coupon_id, status, expires_at)` 인덱스 | 막힌 회차의 대기 행이 매 주기 재스캔된다. **인덱스 도입은 실측 뒤 별도 판단이다** |
| 나머지 배치 테스트의 시계 고정 | 이 티켓이 건드린 테스트 중 **잡을 실제로 돌리는 것은 전부** `FixedClock` 으로 고정했다. `ExpireCancelRaceTest`·`ExpireJobHistoryGuardTest`·`ExpireJobIsolationTest`·`VerifyJob*` 은 아직 벽시계인데, 이 티켓이 안 건드린 파일이라 여기서 안 넓혔다. (storage 의 만료 테스트는 `asOf` 가 SQL 바인드 파라미터일 뿐이라 시계와 무관하다) |
| JVM 기본 존을 UTC 로 강제하는 기동 가드 | `VerifyJobConfig` 가 *"이 문서로 미뤘다"* 고 가리키는데 **여기 항목이 없었다**(CY-429 가 세운다). **미룬 이유는 `batch/build.gradle` 의 `test` 태스크가 `user.timezone=Asia/Seoul` 을 일부러 준다는 것이다**(CY-392 — 존 버그를 재현하려고 DB(UTC)와 어긋나게 뒀다). 기동 가드를 넣으면 batch 의 모든 `@SpringBootTest` 가 거절당하므로, 그 테스트를 어떻게 면제할지 먼저 정해야 한다. 지금 UTC 를 지키는 것은 `batch.yml` 의 `TZ` 와 `build.gradle` 의 `bootRun user.timezone` 둘이고, 사각은 IDE 실행과 `java -jar` 다 |
| admin API 에 읽기 타임아웃이 없다 | `VerifyTriggerController` 도 `ExpireAdminController` 도 톰캣 스레드에서 `jobRepository` 를 맨몸으로 부른다 — 형제인 `BatchRunMetricsRefresher` 는 readOnly + 초 단위 타임아웃으로 감싼다. **이 API 들은 정확히 DB 가 아플 때 불리는데** 그때 진단 도구가 매달린다. 만료 쪽에만 걸면 admin API 둘의 신뢰성 계약이 갈리므로 **함께** 정해야 하고, 뿌리는 JDBC URL 에 `socketTimeout` 이 **전 저장소에 0건**인 것이라 storage 레벨 결정이다 |
| `cleanupJob` 시체를 걷을 API | `BatchStuckExecution` 은 `Job` 빈 셋 전부에 뜨는데(`BatchRunMetrics` 가 `List<Job>` 에서 이름을 모은다) 컨트롤러는 둘이다. `cleanupJob` 만 손 SQL 이 유일한 길이고 알림이 그 사실을 명시한다. 잡 이름을 경로 변수로 받는 형태로 일반화할 때 **"트리거는 열지 않는다"(`docs/15`) 규율도 함께 옮겨야 한다** |
| `verifyJob` 의 `stop` 에 시체 판정이 없다 | 만료 쪽은 CY-429 가 `RunningJobProbe.stuckExecutions` 를 통과한 실행만 건드리는데, verify 의 `stop` 은 그 판정을 안 지난다 — **살아서 도는 300만 전수 검증을 아무나 멈출 수 있다.** 알림 description 이 그 차이를 명시하지만, 코드로 막는 것이 맞다 |
| 리뷰 반복 결함의 기계적 검사 | 이 티켓의 리뷰에서 **같은 종류가 반복해서 나왔다** — 떠 있는 javadoc(4회), 개명 뒤 끊긴 문서 참조, 개수 주장과 실제 불일치("넷"↔"다섯"), 지표 단언 누락. 넷 다 파일을 읽어 기계적으로 잡을 수 있다. `BatchMetricExposureTest` 가 규칙 파일↔노출을 잇는 것과 같은 방식으로 테스트화할 자리다 |
| ~~`README` 의 batch 패키지 트리~~ | **해결됐다** — README 에 `api`/`config`/`job`/`replay`/`schedule` 트리가 들어갔다 |

### 알림

```
cy_expire_blocked_coupons   게이지. **마지막으로 성공한 실행이** 제외한 회차 수
```

```yaml
- alert: ExpireSkippingBrokenCoupons
  expr: cy_expire_blocked_coupons > 0
  for: 10m          # 두 주기. **재고를 고쳐도 다음 만료(04:10)가 성공해야 내려간다** —
                    # 게이지가 마지막으로 성공한 실행의 결정이라서다(CY-421)
  severity: warning # 배치는 성공했다. 데이터를 봐야 하는 상황이다
```

`BatchJobFailed`(critical)와 **반드시 갈라야 한다** — 하나는 서버를 보는 알림이고
이것은 데이터를 보는 알림이다.

---

## 2. 알림 — "성공했는데 맞게 했나" 를 아무도 안 본다

규칙 여섯 중 셋(`BatchJobFailed`·`BatchJobNotRunning`·`BatchJobRunningTooLong`)은
**잡의 생사**만 본다. 셋 다 통과하면서 아무것도 안 하는 상태가 있고, 2b 가 그 축 밖을 메웠다
(`ExpireLeavesWorkBehind`·`ExpireSkippingBrokenCoupons`·`ExpireMetricsUnknown`).

### 2a. ~~검증 판정이 알림으로 안 나간다~~ **완료 · CY-359**

> **아래는 그때의 설계 초안이다.** `cy_verification_verdict{dataset,scope}` ·
> `cy_verification_findings` 가 노출되고 규칙도 걸렸다. 판정 근거를 남기려고
> 본문을 그대로 두지만, **읽는 사람은 미완으로 오해하지 말 것.**

`verdict = FAIL` 은 **정상 종료**다(원칙대로). 그래서 **그때는 알림이 하나도 안 울렸다.**
`verification_runs.verdict` 가 DB 에 남을 뿐, 누가 그 행을 조회하기 전까지 아무도 모른다.
게이트 판정의 본체인데 통로가 없다.

```
cy_verification_verdict{dataset,scope}    마지막 실행의 판정 (PASS=0, FAIL=1)
cy_verification_findings{dataset,scope}   검출 건수
```

> **접두는 `cy_` 다.** 지금 나가는 것 전부가 그 접두이고, 다른 영역이 배치 지표를 더할
> 때도 맞춘다(관제 파트 OBS-23 문의로 정했다). Micrometer 이름의 `.` 은 `_` 로 변환되므로
> 처음부터 `_` 로 적는 편이 헷갈림이 적다.

> **이름에 `_total` 을 붙이면 안 된다.** 처음에는 `cy_verification_findings_total` 로 적어
> 뒀는데, 그것은 카운터 규약이라 Micrometer 의 Prometheus 렌더러가 **게이지에서는 떼어
> 낸다** — 코드가 부르는 이름과 관제가 보는 이름이 갈려 알림이 영원히 안 뜬다.
> CY-347 에서 실제로 그렇게 만들었다가 노출 테스트가 잡았다(`ExpireMetrics` javadoc 참조).
> 이 둘은 <b>마지막 실행의 값</b>을 들고 있는 게이지이지 누적 카운터가 아니다.

> **라벨은 `{dataset, scope}` 둘 다 단다.** 다만 <b>지금 실제로 생기는 것은 둘</b>이다 —
> `rejectUnsupportedScope` 가 `INCREMENTAL` 을 시작 전에 거부하고(`VerifyJobConfig`),
> `finalizeRunStep` 도 <i>"증분 판정 규칙이 정해지기 전까지 이 경로는 열리면 안 된다"</i> 로
> 막는다. `scope` 를 미리 다는 것은 <b>증분이 열릴 때 지표 이름을 안 바꾸려는 것</b>이고,
> 그때 두 범위가 한 시계열을 덮어쓰는 것을 막는다.

| 알림 | 조건 | 대응 |
|---|---|---|
| ~~`VerificationCannotJudge`~~ | ~~잡 `FAILED`~~ | **안 만들었다.** 통계 Step 이 죽으면 잡은 `FAILED` 인데 판정은 이미 커밋돼 있어 *"판정을 못 냈다"* 가 거짓이 된다. 두 축이 독립이라 지표로 본다 — `VerificationMetricsUnknown`(`docs/14` 4단계) |
| `VerificationVerdictFailed` | `verdict = FAIL` | **데이터를 본다** — 판정은 났고 불일치가 있다 |

**CY-347 에서 값을 치른 것 셋을 그대로 적용한다.**

1. **판정을 못 낸 실행은 `0` 이 아니라 `NaN`(모름)이다.** `0` 은 `PASS` 라 <b>합격으로
   읽힌다</b> — 잡이 죽어 판정이 없는데 관제는 통과로 본다. `ExpireMetrics.markUnknown()`
   과 같은 자리다.
2. **두 지표를 따로 `set` 하지 않는다.** 스크레이프가 사이에 끼면 `verdict` 는 새 실행,
   `findings` 는 앞 실행 값인 샘플이 나온다. 한 스냅샷으로 묶는다.
3. **알림 식에서 계산하지 않는다.** 필요한 값은 코드에서 만들어 <b>한 시계열</b>로 낸다.

> **3단계는 리스너가 아니라 주기 되읽기로 갔다.** 아래 리스너 설계는 그 결정 전의
> 초안이다 — 최종 구현과 근거는 `docs/14` 3단계에 있다. 검증은 사람이 손으로 드물게
> 돌려서, 프로세스 게이지로 두면 재배포에 판정이 사라지는데 DB 에는 남아 관제와 진실이
> 갈린다. 그리고 되읽기는 **시드가 심은 기준 행을 걸러야 한다**(`origin='BATCH'`).

**붙일 자리** — `verifyJob` 에는 지금 `JobExecutionListener` 가 <b>하나도 없다</b>
(`expireJob` 과 다르다).

> **키는 `runId` 다.** 처음에 `verify.runId` 로 적어 뒀는데 그런 키는 없다 —
> `VerifyJobConfig.RUN_ID_KEY` 의 값이 그냥 `"runId"` 다(접두사를 쓰는 것은
> `manifest.seedRunId` 쪽이다). `ExecutionContext.get` 은 없는 키에 예외가 아니라
> <b>{@code null}</b> 을 주므로, 그대로 구현했으면 <b>모든 실행이 조용히 "모름"</b> 이 됐다.
> 문자열을 다시 쓰지 말고 상수를 참조한다.

> **`runId` 가 없는 실행이 곧 "판정을 못 낸 실행" 이다.** `startRunStep` 은 가드 여덟
> (`rejectDatasetMismatch`·`rejectUnsupportedScope`·`rejectRunningExpire`·
> `rejectAsOfBeforeLatestHistory`·`rejectIssuancesUpdatedAfterAsOf`·
> `rejectStocksUpdatedAfterAsOf`·`validateSeedRunId`·`rejectExistingRun`)를 <b>전부
> 통과한 뒤에야</b> 컨텍스트에 심는다. 리스너는 `runId` 없이도 동작해야 하고, 없으면
> 그것이 <i>모름</i>이다 — 라벨은 `JobParameters` 에서 뽑는다.

> **`afterJob` 이 아예 안 불리는 경로가 있다.** 파라미터 검증 실패와 `preventRestart()` 는
> `JobExecution` 이 만들어지기 <b>전에</b> 런처에서 던진다. 그때는 잡 메트릭도 안 오르고
> 리스너도 안 불려 <b>앞 실행 값이 그대로 남는다.</b> 이 축은 지표로 못 덮으므로
> 트리거 경로가 책임진다.

### 2b. 만료 누락

성공한 실행 뒤에도 `status='ISSUED' AND expires_at < now` 인 행이 남는 상태.
1번의 오염 회차 말고도 원인이 여럿이다(슬롯 계산 오류, 설정 실수).

```
cy_expire_pending                 기한이 지났는데 아직 ISSUED 인 발급건 수
cy_expire_blocked_pending         그중 막힌 회차의 몫
cy_expire_unexplained_pending     그 둘의 차. **알림이 보는 것은 이것 하나다**
cy_expire_blocked_coupons         그 실행이 건너뛴 회차 수
cy_expire_measured_at_seconds     위 넷이 기준으로 삼은 asOf. **0 이어도 어제 것일 수 있다**
cy_expire_clean_schema            되읽기가 붙은 스키마 (1 정상 · 0 오염 · NaN 모름)
cy_expire_refresh_failures_total  되읽기 실패 횟수(카운터)
```

> **계약 셋 (CY-421).** ① **기록자는 `ExpirePendingRefresher` 하나다** — 잡도 스케줄러도
> 게이지를 안 건드린다. 그래서 순서 규칙 없이 마지막 기록이 곧 진실이다.
> ② **실린 것은 "마지막으로 성공한 실행" 뿐이다** — `STATUS='COMPLETED'` 로 좁히므로
> 실패한 실행의 잔여는 여기 안 뜬다. 그 축은 `BatchJobFailed`·`ExpireNotSucceeding` 이 진다.
> ③ **제외 목록은 그 실행의 결정이라 재고를 고쳐도 안 바뀐다** — 다시 만들면 데이터를 고친
> 것이 서버 critical 로 나간다. 얼리는 것은 **목록**이고 **행 수는 매 주기 다시 센다.**

> **이름에 `_total` 을 붙이면 안 된다.** 카운터 규약이라 Micrometer 의 Prometheus 렌더러가
> 게이지에서 **떼어 낸다** — 붙이면 코드가 부르는 이름과 관제가 보는 이름이 갈린다.
> 실제로 그렇게 만들었다가 노출 테스트가 잡았다.

> **차를 알림 식에서 빼면 안 된다.** 게이지 둘을 따로 `set` 하는 사이에 스크레이프가 끼면
> 한쪽만 새 값인 샘플이 나온다. 한 문장에서 세어 한 시계열로 내보낸다.

> **스크레이프 때 세면 안 된다.** 300만 행에 `COUNT(*)` 를 15초마다 때리는 꼴이다.
> 한때 잡이 끝나는 시점에 한 번 세서 게이지에 넣었는데, 그러면 **값이 프로세스와 함께
> 죽는다.** 지금은 되읽기가 60초 주기로 <b>마지막으로 성공한 실행의 `asOf`</b> 로 다시
> 센다(CY-421) — 그 비용이 대기 건수에 비례한다는 것은 실측했다(§6 의 그 항목).

`for: 10m`(두 주기). **한 실행이 대상을 다 비운다** — 청크는 0 이 나올 때까지 반복하므로
"대상이 많아 다음 주기로 넘어간다" 는 일이 없다. 그래도 남는 것은 막힌 회차의 몫(이미
뺐다)이거나 step-timeout 으로 청크가 끊긴 경우뿐이라 길게 볼 이유가 없다.

**2b 는 CY-347 에서 함께 구현했다.**

### 2c. 처리량 — "돌기는 도는데 안 줄어든다"

`writeCount` 가 메트릭에 없다. Micrometer 가 자동 등록하는 것은 횟수와 진행 중 시간뿐이고,
`BATCH_STEP_EXECUTION.WRITE_COUNT` 에는 남지만 **알림이 SQL 을 못 읽는다.**

```
cy_expire_processed_total   카운터. 청크마다 넘긴 건수를 더한다
```

```yaml
expr: increase(cy_expire_processed_total[30m]) == 0 and cy_expire_unexplained_pending > 0
```

두 조건을 **AND 로 묶는 것이 핵심**이다. 처리량 0 자체는 정상이다 — 만료할 게 없는 날이
대부분이다. 남은 대상이 있는데 0 인 것이 사건이다.

> **`cy_expire_pending` 이 아니라 `cy_expire_unexplained_pending` 이다.** 막힌 회차의 대기는
> 설계상 계속 남으므로, 전체를 보면 그 회차를 사람이 고칠 때까지 **처리량이 정상인 날에도
> 정체 알림이 뜬다.** 회차 격리가 세운 구분(배치가 안 한 몫 vs 데이터가 어긋난 몫)이
> 여기서도 그대로 적용된다.

### 2d. 실패 원인을 알림이 못 가른다

`spring_batch_job_seconds_count` 에 **에러코드 라벨이 없다.** 그래서 만료의 실패 자리 **다섯**이
한 시계열로 뭉쳐 나오고, 어느 자리였는지는 배치 로그의 `EXPIRATION-00N` 으로만 갈린다.
각각 봐야 할 곳이 다르다 — 셋은 코드, 하나는 넘긴 파라미터, 하나는 접속 URL 이다.

> 코드의 **뜻**이 어긋나던 문제는 CY-347 에서 정리했다(`ExpirationErrorCode` 와 알림 머리말).
> 여기 남은 것은 라벨 하나뿐이다.

### 2e. ~~Prometheus 가 규칙을 아직 안 읽는다~~ **완료 · CY-359**

> 규칙 32개가 로드되고 mock 리시버까지 배선됐다(CY-359 당시 22개, CY-446 이 열을 더했다). CI 가 `promtool check config` 와
> `test rules` 를 매번 돌린다. **아래는 그때의 초안이다.**

그때는 규칙 파일은 있는데 **읽는 프로세스가 없었다.** `prometheus.yml` 이 그 사실을 스스로 적어 뒀다.
**언제** — CY-359. 단계와 검증 계약은 `docs/14-observability-wiring.md` 에 있다.
그 전까지 위 알림을 아무리 잘 써도 아무도 안 본다.

**받는 쪽은 Slack 이 아니라 mock 리시버다.** 근거 둘 —

- PRD 의 제약이 *"외부 연동 Mocking"* 이고, 실시간 드리프트 계층도 *"알람(Mock)"* 으로
  정해져 있다(영역 ①). 배치 알림만 실제 외부를 붙일 이유가 없고, 두 계층이 같은 방식을
  쓰는 편이 낫다.
- **이 저장소는 PUBLIC 이다.** Slack 을 붙이면 웹훅 URL 이 시크릿으로 들어가고 compose 에
  그 참조가 남는다.

> CI 의 `Slack 전송` 은 **GitHub Actions 가 PR 리뷰를 알리는 것**이지 앱 런타임과 무관하다.
> 앱 쪽에는 Slack·웹훅 설정이 하나도 없다. 그것을 선례로 삼지 마라.

mock 리시버로도 증명할 것은 다 증명된다 — 알림이 <b>뜨는 것</b>, 그리고 이 설계의 핵심인
`channel: server`(서버를 봐라)와 `channel: data`(데이터를 봐라)가 <b>서로 다른 경로로
갈리는 것</b>.

> **가르는 축은 `severity` 가 아니다.** warning 셋 중 데이터 축은
> `ExpireSkippingBrokenCoupons` 하나뿐이고 나머지 둘은 서버 축이라, severity 로 가르면
> *"데이터를 봐라"* 로 라우팅된 것의 원인이 접속 URL 인 상황이 나온다.
> `severity` 는 긴급도로 남긴다.

**`prometheus.yml` 이 이미 배선을 전제하고 있다.** compose 의 서비스 이름이 여기 맞아야 한다 —
알림은 `alertmanager:9093`, 스크레이프 대상은 `batch:9092`(= `BATCH_MANAGEMENT_PORT`).

---

## 3. 만료 배치 — 실측으로 대가를 남긴 것들

수치는 전부 `docs/12` 에 있다. 여기는 **언제 손대는지**만 적는다.

| 무엇 | 언제 | 왜 그때인가 |
|---|---|---|
| `EXPIRE_BATCH` 의 `ORDER BY`·`LIMIT` → 상한 방식 | **취소·사용 API 티켓** | 후보 ≫ `LIMIT` 이면 후보 전부를 X 락한다(5,000건 실측). 막히는 것은 취소·사용뿐인데 그 경로가 아직 없다 |
| 표식 → `run_id` 컬럼 | **배치 다중화 직전(차단 조건)** | 인스턴스가 하나면 닿을 수 없다. 두 대가 되면 `LAST_EXPIRED_ID` 의 상한이 남의 행에 밀린다 |
| 청크 실행 시간 실측 | **300만 건 적재 직후** | 락 보유 시간이 `innodb_lock_wait_timeout`(50초)을 넘는지가 "막힌다" 와 "1205 로 실패한다" 를 가른다 |
| 인덱스 둘의 쓰기 비용 | **세 번째 인덱스 얘기가 나올 때** | 지금 둘은 가용성과 5분 주기로 정당화됐고 쓰기 축은 본 적이 없다 |

---

## 4. 운영

| 무엇 | 언제 |
|---|---|
| ~~`BATCH_*` 정리~~ | **완료 · CY-436.** `cleanupJob` 의 `purgeBatchMetadataStep` 이 `batch.cleanup.metadata-keep-days`(기본 30, **최소 8 = 되읽기 창 7일 초과, 기동 거절**)로 걷는다. 딸린 행을 FK 역순으로, 고아 `JobInstance` 를 같은 트랜잭션에서 함께 지운다. `END_TIME` 이 비어 있는 행(시체)은 대상이 아니다 — 그 축은 §6 시체 절이 지고 CY-429 의 복구 API 가 닫는다. 청크는 `batch.cleanup.metadata-chunk-size`(기본 500, **잡 실행 수**, 1..5000)다. **삭제는 `IN` 목록이 아니라 id 하나씩** — `IN` 목록이 테이블 행 수 대비 커지면 옵티마이저가 풀스캔을 골라 대상이 아닌 행까지 잠근다. 그러면 양방향이 다 깨진다(실측): RR 에서는 메타 테이블 전체 + 갭 + supremum 에 X 락이 걸려 **다른 잡 기동과 도는 잡의 하트비트 커밋이 막히고**, `READ COMMITTED` 로 내려도 풀스캔은 **남이 잡은 행에서 대기**해 청크가 `ERROR 1205` 로 죽는다. id 하나씩이면 여섯 문장이 전부 `rows=1` 이라 RR·RC 양쪽에서 네 프로브가 다 통과한다 — **격리수준은 기본값 그대로**다. 대가는 5,000 실행 기준 680ms → 1,980ms(약 2.9배, 청크당 200ms 수준). 만료가 일 1회로 옮겨 인스턴스 순증은 하루 288 → **1** |
| 정리 Step 1 이 `expireJob` 을 안 본다 | **미착수.** `purgeVerificationRunsStep` 의 프로브가 `verifyJob` 만 본다(`blockingExecutions(VerifyJobConfig.JOB_NAME)`). 만료 04:10 과 정리 04:30 이 겹치는 밤에 정리가 물러나지 않는다. CY-436 이 닫은 것은 **Step 2(배치 메타)가 남을 잠그거나 남에게 잠기는 경로**이고 그건 삭제 계획을 고쳐서지 격리수준이 아니다. Step 1 은 기본 격리수준 그대로이고, 이 항목이 말하는 것은 락이 아니라 프로브가 만료를 안 봐서 **정리가 물러나지 않는 것**이다 |
| 버려진 실행 컷오프가 안 얼어 있다 | **미착수.** Step 1 의 `abandonedBefore` 는 청크마다 다시 잡는다(Step 2 의 메타 컷오프는 첫 청크에 얼린다). 드레인이 길어지면 시작 때 대상이 아니던 검증이 컷오프 안으로 들어와 그 입력(`asof_state`)이 걷힐 수 있다 — 지금은 `verifyJob` 프로브가 가려 줄 뿐이다 |
| ~~회차 상태 전이 스케줄러~~ | **완료 · CY-446.** `CouponRoundScheduler` 가 1분마다 `open_at` 도달 회차를 열고 `close_at` 도달 회차를 닫는다. **Spring Batch 잡이 아니다** — 1분 주기로 배치 메타를 쓰면 하루 1,440 인스턴스가 되어 CY-436 이 정리한 축이 되살아난다. **대상을 고르고 id 하나씩 조건부 UPDATE** 로 바꾸고, **어댑터를 `READ COMMITTED` 로 연다**. ⚠️ **발급을 살리는 것은 격리수준이다** — 기본(`REPEATABLE READ`)에서는 이 테이블을 훑는 `UPDATE` 가 X 락 151(전부 + supremum)을 잡아 재고 소진 `CLOSED` 와 발급 전 `FOR SHARE` 가 둘 다 `ERROR 1205` 였고, RC 에서는 `X,REC_NOT_GAP` 10 만 잡고 둘 다 통과했다. **id 단건은 그것과 별개**이고 격리수준이 되돌아가는 날의 두 번째 겹이다(돌연변이 확인: RC 에서는 집합 `UPDATE` 로 되돌려도 락 테스트가 전부 초록이었다). `close_at` 은 갱신하지 않고(docs/02 F5) `coupon_stocks` 도 안 건드린다. 관측은 **결과 축**이다 — 게이지 **다섯**: 대기 넷(`cy_coupon_round_pending_open`·`_pending_close`·`_missed_window`·`_blocked_no_stock`)을 **한 문장으로** 되읽고(문장을 나누면 RC 에서 read view 가 갈려 회차가 어느 게이지에도 안 잡히거나 이중 계상된다), `_scheduling_enabled` 가 **끈 구간을 알림 갈래에서 빼는 축**이다(만료·정리는 그 축을 안 쓰고 사람이 silence 를 건다 — docs/14), 카운터 넷(`_ticks_total`·`_select_failures_total`·`_transition_failures_total`·`_refresh_failures_total`)이 진단을 진다. 알림은 **열**이고 데이터 축 셋(`BlockedByMissingStock`·`MissedWindow`·`DataMetricsUnknown`)은 `channel: data` 로 갈랐다 |
| ~~회차 생성 스케줄러~~ | **범위 밖 · CY-503.** 그때는 회차를 만드는 경로가 시드뿐이라 batch 가 그 축을 맡는 것이 자연스러웠는데, 지금은 **관리자 API 가 그 일을 한다**(`POST /api/v1/admin/coupon-templates/{id}/rounds` · CY-5). 배치가 매일 새벽에 하나 더 만들면 같은 테이블에 회차를 만드는 경로가 둘이 된다. 자리표시였던 `batch.schedule.coupon-create-cron` 을 걷었다 — 남겨 두면 다음 사람이 그것을 "하기로 되어 있는 일" 로 읽는다. **전이가 지는 전제는 그대로다** — 재고 행 없는 회차를 일부러 안 연다(발급 경로가 죽는다). 자동 생성이 필요해지면 그때 어느 쪽에 둘지 다시 정한다 |
| 기동 가드가 배치 메타 **인덱스**를 안 본다 | **미착수.** `SchemaPresenceGuard` 는 테이블과 핵심 컬럼만 본다. `V14`·`V15` 가 빠져도 기동과 동작이 통과하고, 되읽기 데드라인 초과(게이지 NaN)나 정리 잡의 매 청크 전체 스캔으로만 드러난다. `information_schema.statistics` 를 보는 셋째 축이 필요하다 — 지금은 가드 메시지가 그 사실을 말하는 데까지만 했다 |
| 업무 포트 노출 | ~~compose 티켓~~ ~~CY-359~~ **CY-368 에서 다시 정했다.** 그 포트에 인증 없는 admin 트리거가 열려 `batch.yml` 은 업무 포트를 **아예 안 내보낸다** — 필요할 때만 `batch-expose.yml` 을 얹어 `127.0.0.1:${BATCH_HOST_PORT:-9090}:9090` 으로 연다. 관리 포트(9092)는 어느 경우에도 안 올린다 |

---

## 4a. 검증용 셋에 Spring Batch 메타 테이블이 없다 (CY-359 가 발견)

`coupon_clean`·`coupon_corrupt` 는 cy-seed 의 `ddl/00_schema.sql` 로 만들어지는데
`CREATE TABLE` 17개 중 **`BATCH_*` 는 0개**다. cy-be 의 `V2__batch_metadata.sql`(과 인덱스 둘 `V14`·`V15`)은
Flyway 소유자인 `api` 만 돌리고, 검증용 셋은 그 Flyway 가 닿는 DB 가 아니다.

그런데 그 DB 를 보게 배치를 띄우는 것이 `application.yml.example` 이 문서화한 정상 절차다.
**데이터 테이블은 다 있고 메타만 없는 상태가 정상 절차에서 생긴다.** 그러면 기동은 통과하고
첫 잡 실행에서 `Table 'BATCH_JOB_INSTANCE' doesn't exist` 로 죽는다.

CY-359 는 `SchemaPresenceGuard` 로 그것을 **기동 시점에 드러내고 메시지로 조치를 가르는**
데까지만 했다. 남은 결정은 **누가 언제 붓는가** 다.

| 후보 | 대가 |
|---|---|
| 시드 생성 절차에 `V2` 를 넣는다 (cy-seed 쪽) | 시드 저장소가 cy-be 의 마이그레이션 파일을 알아야 한다 — 지금은 스키마 주인이 cy-be 라는 규율과 맞물려 사본 관리가 하나 더 는다 |
| compose 에 마이그레이션 원샷 서비스를 넣는다 | `api` 이미지를 `--spring.batch.job.enabled=false` 로 한 번 돌린다. `base.yml` 이 `api` 를 알아야 한다 |
| 문서화된 수동 절차로 둔다 (현재) | `docs/14` 시연 절차에 `V2`·`V14`·`V15` 주입 명령을 박아 뒀다. **테이블 누락은 가드가 잡지만 인덱스 누락은 못 잡는다** — 빠뜨려도 기동과 동작이 통과한다 |

지금은 셋째다 — 가드가 있어 **조용히 실패하지는 않는다**. 검증을 자동으로 돌리는 티켓이
열리면 그때 앞의 둘 중 하나로 간다.

**같은 구멍이 README 경로에도 있었다.** 검증용 셋만의 문제가 아니다 — `base.yml` 의 mysql 은
빈 `app` DB 만 만들고 스택에 마이그레이션 주체가 없어서, README 대로 띄우면 데이터 넷까지
전부 없다. CY-359 는 README 에 `api` 를 한 번 띄우는 단계를 박는 것으로 닫았다.
`base.yml` 에 원샷 서비스를 넣는 쪽은 `api` 이미지(Dockerfile)가 아직 없어 미뤘다 —
그것이 생기는 날 위 표의 둘째로 옮기면 두 구멍이 한 번에 닫힌다.

---

## 4b. ~~응답 봉투가 두 벌이다~~ 해결됨 (CY-368)

batch 에 컨트롤러를 열면서 `api` 의 `ResponseEnvelope`·`ErrorResponse` 를 못 써
같은 규약을 batch 쪽에 다시 세울 뻔했다. `.coderabbit.yaml` 이 전 Java 공통으로
**"응답은 항상 `ResponseEnvelope` 로 감싼다"** 를 못 박아 뒀고, batch 는 `core` 만
의존하므로 **그 봉투를 `core/support/response` 로 옮기는 것**이 규약을 지키는 유일한
길이었다. `api` 는 import 만 바뀌고 동작은 같다.

`core` 에 `jackson-annotations` 하나가 늘었다 — `@JsonProperty("success")` 때문이다.
컴포넌트 이름을 `success` 로 바꿔 없애려 했지만 정적 팩토리 `success(T)` 와 accessor 가
충돌한다. 직렬화 구현이 아니라 애노테이션 jar 라 도메인 모듈이 무거워지지 않는다.

`requestId` 는 batch 에서 `null` 이다 — 그것을 MDC 에 심는 `RequestIdFilter` 가 없다.

---

## 4c. 전체 빌드가 5분이다 — 캐시·병렬이 꺼져 있다 (CY-368 이 실측)

**테스트 메서드 합계는 100초 남짓인데 `./gradlew build` 는 5분이다.** 나머지는
Testcontainers MySQL 기동과 Spring 컨텍스트 로딩이다.

| 무엇 | 지금 | 왜 |
|---|---|---|
| `gradle.properties` | **한 번도 만든 적이 없다** | Spring Initializr 가 안 만드는 파일이다. 지운 게 아니라 처음부터 없어서 병렬·캐시가 전부 기본값(꺼짐)이다. `.gitignore` 의 `.gradle` 은 캐시 **디렉터리**라 무관하다 |
| Testcontainers `withReuse` | 안 켬 | 클래스마다 컨테이너를 새로 띄운다 |
| Spring 컨텍스트 | 배치만 6벌 | `@SpringBootTest` 의 `properties` 조합이 다르면 캐시가 안 걸린다 |

**병렬은 효과가 제한적일 수 있다.** 느린 것은 `storage`(73초)와 `batch` 인데 둘 다 같은
MySQL 컨테이너 자원을 두고 경쟁하고, `batch` 는 `storage` 의 `testFixtures` 에 의존해
완전 병렬이 안 된다. **캐시 쪽이 실효가 커 보인다** — 문서만 고친 라운드에서 Java 테스트를
통째로 건너뛸 수 있다. 다만 **둘 다 재 보고 정한다.** 근거 없는 수치를 넣지 않는다.

**앞의 둘은 검토 대상이고 셋째는 대체로 불가피하다.** 예컨대 실행 중인 만료를 심는 클래스와
안 심는 클래스는 그 축을 재려고 일부러 나눈 것이다 — 합치면 검증이 사라진다.
다만 조합을 줄일 여지가 있는지는 한 번 훑을 값어치가 있다.

**`withReuse` 는 격리와 맞바꾼다.** 컨테이너를 재사용하면 앞 테스트가 남긴 데이터가
다음으로 넘어가는데, 이 저장소는 이미 그 경계에서 여러 번 데였다(`VerificationSeed.clear()`
가 존재하는 이유, 그리고 CY-368 이 비동기 잡으로 남의 테스트를 깨뜨린 일). **켤 거면
정리 규율을 먼저 세운다.**

> 착수 조건 — 지금 당장 급하지 않다. CI 는 8분에 돌고 사람은 개발 중 `--tests` 로 좁혀
> 돌리면 20초다. **전체 빌드는 커밋 직전 1회면 충분하다.** 이 항목이 값어치를 갖는 것은
> 그 1회가 병목이 될 만큼 잦아지는 때 — 예컨대 잡이 늘어 컨텍스트가 열 벌을 넘길 때다.

---

## 5. 검증 — 미결정

**통계 집계도 발급을 막을 수 있다.** 5,000행에서 락 5,023 · 발급 INSERT 1205 실측
(`docs/12` §9). 처방은 만료와 같을 수 없다 — 통계는 `dataset_fingerprint` 와 함께 판정
근거가 되므로 격리를 내리면 집계 중에 원본이 바뀐다.

**결정할 것** — 검증 중에는 발급도 멈추는 운영 규율로 갈지, 통계 Step 도 격리를 내릴지.
지금 막아 주는 `rejectRunningExpire` 가 보는 것은 **배치 메타의 만료 실행뿐**이라
api 의 쓰기는 애초에 안 잡힌다.

---

## 6. 배치 주기를 실무 기준으로 되돌린다 (CY-384 가 첫 단계)

**지금 주기는 실무 세 칸 중 어디에도 안 맞는다.** 실무의 배치는 준실시간(초~분) ·
배치 창(일 1회 새벽) · 온디맨드로 갈리는데, **만료·검증·정리는 셋 다 배치 창**이다.

| | 처음 | 지금 |
|---|---|---|
| 만료 | 5분 크론 | **배치 창 04:10 (일 1회)** — CY-397 |
| 정리 | **잡이 없었다** | **배치 창 04:30 (일 1회)** — CY-397 이 만들었다 |
| 검증 | 온디맨드만 | **배치 창 05:00 (일 1회)** — CY-470. 온디맨드 API 도 그대로 산다 |

> ⚠️ **"정리 1시간" 은 사실이 아니었다.** `.example` 에 `cleanup-cron` 값과
> `asof-state-keep-runs` 가 있었을 뿐 **읽는 코드가 하나도 없었다.** 옮길 대상이 아니라
> 만들 대상이었다 — 이 표가 그것을 "지금 1시간" 으로 적고 있었다.

**만료가 5분인 대가를 실측했다.** `valid_days` 30~180(평균 105)에 ISSUED 약 66만이면
하루 만료는 약 6,300건이고, **5분 실행 한 번이 손대는 것은 약 22건** — 청크 크기 1000의
2%다. 이벤트 진행 중 만료는 **0건**이다(열린 회차의 쿠폰은 방금 발급된 것이라 기한이 남았다).
`idx_issuance_status_expires` 도 이미 있다.

**검증이 온디맨드인 것은 절반이 우리가 만든 제약이었다.** 크론을 못 건 이유가
`rejectRunningSchedulers` 였고, **CY-384 가 그것을 풀었다.** 남은 것은 아래 순서다.

### 순서를 바꾸면 안 된다

| | 무엇 | 선행 |
|---|---|---|
| **A** | ~~검증 가드를 실행 중 검사로~~ **완료 · CY-384** | — |
| **B** | ~~배치 감시를 마지막 성공 시각으로~~ **완료 · CY-392** — 지표 셋(`cy_batch_last_success_seconds{spring_batch_job_name}`·`cy_batch_stuck_executions{spring_batch_job_name}`·`cy_batch_refresh_failures_total`) + 규칙 일곱(`ExpireNotSucceeding`·`ExpireNeverSucceeded`·`ExpireGaugeMissing`·`BatchTargetDown`·`BatchStuckExecution`·`BatchRunMetricsUnknown`·`BatchRunMetricsStale`) | — |
| **C** | ~~만료·정리를 배치 창으로~~ **완료 · CY-397** — 만료 04:10 · `cleanupJob` 신설 04:30 · 만료 SLA 180,000(50h) · `max-expire-skips` 1 · `BatchJobRunningTooLong` 600초 · 규칙 넷 신설(`CleanupNotSucceeding`·`CleanupNeverSucceeded`·`CleanupGaugeMissing`·`CleanupRunningTooLong`) · `NeverSucceeded` 의 `for` 는 10분·30분 유지(크론 슬롯을 예산으로 쓰면 재기동마다 리셋돼 영원히 안 뜬다 — 시도했다가 되돌린 근거는 `batch-alerts.yml` 의 그 규칙 주석) · `started_at`/`finished_at` 을 도메인 시계로 | A · B |
| **D** | ~~300만에서 검증 소요 실측 → 05:00 슬롯 배정~~ **완료 · CY-470** — 실측 **472초**(판정 경로) · `VerifyScheduler` 05:00 신설 · 게이지 `cy_verify_last_success_seconds{dataset,scope}`(`BATCH_JOB_EXECUTION_PARAMS` 조인) · 규칙 넷 신설(`VerifyNotSucceeding`·`VerifyNeverSucceeded`·`VerifyGaugeMissing`·`VerifyRunningTooLong` 1200초) · **C 가 미룬 셋** 전부 — `max-expire-skips` 1→**0** · 만료 SLA 180,000→**90,000**(식 둘 다) · `cleanup.abandoned-after-hours` 24→**6** · 되읽기 창(7일)을 `BatchMetadataWindow.LOOKBACK_DAYS` 로 묶어 SQL 리터럴 둘을 없앰 · `statsAggregateStep` 의 집계 둘을 쪼갬(아래) | C · 300만 적재 |

> **~~만료 지표가 아직 프로세스 게이지다~~ 완료 · CY-421.** `cy_expire_*` 넷이 `afterJob`
> 에서만 채워져, 일 1회로 옮긴 뒤 재기동부터 다음 04:10 까지 **최대 하루가 `NaN`** 이었고
> 그 사이 `ExpireLeavesWorkBehind` 가 발화할 수 없었다. `ExpirePendingRefresher` 가 60초마다
> **마지막으로 성공한 실행의 `asOf`** 로 다시 세어 그 창을 닫았다.
>
> 그 티켓이 함께 정한 것 둘 — **기록자를 하나로** 했고(잡의 `afterJob` 관측 리스너와
> 스케줄러의 `markUnknown` 을 걷어냈다), **`blocked` 는 다시 계산하지 않고 배치 메타에서
> 가져온다.** 다시 계산하면 어긋난 재고를 고치는 순간 그 몫이 `unexplained` 로 옮겨 가
> *데이터를 고쳤더니 서버 critical 이 뜨는* 모양이 된다.
>
> 비용은 실측했다 — 발급 40만·대기 800건에서 `COUNT_PENDING` 한 질의가
> `idx_issuance_status_expires` 의 range 스캔이라 **전체 행이 아니라 대기 건수에**
> 비례해 1ms 다. `BLOCKED_COUPONS` 는 되읽기 경로에 없다 — 잡의 태스클릿이
> 부르고 되읽기는 그 결과를 배치 메타에서 읽는다.

> **~~만료 SLA 가 50시간인 것은 C 의 남은 빚이다~~ 완료 · CY-470.** 일 1회에서
> `max-expire-skips=1` 이면 최대 지연이 이틀이라 SLA 가 그만큼 무뎌졌다. C 가 그 손잡이를
> 0 으로 못 내린 이유는 근거가 *"겹침은 일정 분리가 막는다(만료 04:10 · 검증 05:00)"* 인데
> **검증 05:00 크론이 없었기** 때문이다 — 검증을 띄우는 유일한 경로가 손 트리거였고
> 04:10 UTC 는 **13:10 KST**, 즉 시연 시간대다. D 가 그 크론을 세워 둘을 함께 되돌렸다:
> `max-expire-skips` 0 · SLA 90,000. 부등식은 `(0+1) × 86,400 + 60 +
> BatchJobRunningTooLong(600) = 87,060 < 90,000` 이라 여유가 **2,940초**다 — 잡 소요 항은
> CY-470 리뷰가 넣게 했다(게이지가 `END_TIME` 이라 잡이 도는 동안 나이가 자란다).
>
> ⚠️ **온디맨드 API 는 그대로 살아 있다.** 손으로 배치 창에 트리거하면 여전히 겹치고,
> 이제 만료가 **뚫고 지나간다** — 그 검증의 `asOf` 는 `rejectIssuancesUpdatedAfterAsOf`
> 때문에 **영구히 못 쓴다**(재시딩 말고 복구가 없다). 손 트리거는 배치 창을 피한다.

> **~~`verifyJob` SLA 는 C 가 못 세웠다~~ 완료 · CY-470.** 크론이 없는 동안은
> *"안 도는 게 정상"* 이라 걸면 영구 발화였다. 걸면서 **`(dataset=CLEAN, scope=FULL)`
> 그레인**으로 세웠다 — 잡 이름 그레인이면 `CORRUPT` 손트리거 한 번이 SLA 를 리셋한다.
> 게이지는 `BATCH_JOB_EXECUTION_PARAMS` 를 조인해 그 축을 만든다.

> **D 가 함께 푼 것 — `statsAggregateStep` 이 DB 를 죽였다.** 300만 전수를 처음 돌려 보니
> 판정 경로(472초)는 멀쩡히 끝나는데 마지막 통계 Step 에서 **MySQL 이 강제 종료**됐다.
> 세 번 재현했고, 에러 로그에 정상 종료 메시지 없이 `starting as process 1` 만 남았다 —
> 강제 종료 뒤 컨테이너 재시작의 흔적이다. mysqld 상주가 907MiB → **1,163MiB** 로 12초 만에
> 부풀었고, **결과가 936행인데도 그랬다.**
>
> ⚠️ **왜 부푸는지는 못 밝혔다.** 처음에는 *"인덱스로 정렬 못 하는 `GROUP BY` 둘
> (`grade_stats` 의 `issued_grade`, `hourly_stats` 의 `ELT(WEEKDAY(…))`)이라 TempTable 엔진이
> `temptable_max_ram`(1GiB)까지 디스크로 안 넘기고 RAM 에 쥔다"* 고 적었는데, **그 설명이
> 재현되지 않았다.** 같은 버전·같은 버퍼풀(8.4.11 · 512MiB · 유휴 899MiB)로 띄운 깨끗한
> 컨테이너에서 294만 행에 같은 질의를 돌리니 상주가 **+3MiB** 로 평평하고
> `Created_tmp_disk_tables` 가 올라 **디스크로 흘러넘쳤다** — MySQL 이 문서대로 동작한 것이다.
> 그 서버에서만 안 넘친 것이고, 차이가 무엇인지(행 폭 · 조인 · 호스트 메모리 압박) 모른다.
> **처방의 근거는 메커니즘이 아니라 그 서버에서 잰 결과다** — 한 문장은 세 번 다 죽었고
> 쪼갠 판은 완주했다. 재현 가능한 최소 케이스를 만드는 것이 남았다.
>
> 처방도 실측으로 골랐다. `SQL_BIG_RESULT` 힌트와 `tmp_table_size` 인하는 둘 다 같은 곡선으로
> 죽었고 `internal_tmp_mem_storage_engine` 은 앱 계정에 권한이 없다 — **쪼개는 것만 들었다.**
> 회차 단위 147회로 나누니 상주가 900 → 903MiB 로 **평평했고** 26초에 끝났다(한 문장 판은
> 12초에 죽었으므로 완주 시간은 오히려 이쪽이 짧다). `hourly_stats` 는 회차 축이 없어
> 이력 id 를 50만 폭으로 훑고 부분합을 자바에서 접는다.
>
> 안 쪼갠 둘의 근거도 실측이다 — `coupon_stats` 는 인덱스 순서를 타는 스트리밍
> `Group aggregate` 라 903MiB 평평(24초)이었고, `broken_issue_history` 는 290만 행 파생
> 테이블을 만들지만 **디스크 임시 테이블로 흘러넘쳐**(`Created_tmp_disk_tables` +2) 902MiB 였다.
> 즉 무거워 보이는 쪽이 안전하고 결과가 49행인 쪽이 위험했다.

**B 없이 C 를 하면 감시 공백이 생긴다.** 예전 `BatchJobNotRunning` 은 15분 창의 증분으로 봤는데,
일 1회로 옮기면 그 창이 **하루의 대부분 비어** 영구 critical 이 된다. 규칙이 틀린 게 아니라
축이 안 맞는 것이라, 먼저 *"마지막 성공이 언제였나"* 로 갈아야 했다 — **CY-392 가 그것을 했다.**
`ExpireNotSucceeding` 은 주기를 안 타므로 **축은 그대로다.** 다만 C 가 손댈 곳은 하나가 아니다 —
숫자가 **열세 자리**에 박혀 있고 — `batch-alerts.yml` 넷(주석 둘·식·summary),
**`.example` 여섯**(설명 둘 + 산술 넷), `docs/15` 하나, 그리고
**`BatchRunMetricsRefresher` 의 javadoc 과 기동 거절 예외 메시지 둘**
(여기가 틀리면 진단 문장이 거짓 숫자를 말한다) —
`BatchJobRunningTooLong` 의 `expireJob` 임계는 **`> 300` → `> 600`** 으로, summary 는
*"주기(5분)보다 오래"* → *"600초 넘게"* 로 축을 갈았다 — 일 1회에서 *"주기보다 오래"* 는
24시간이라 아무것도 못 잡으므로, 대신 **일감의 크기**(하루 약 6,300건 = 일곱 청크)에서 잡는다.
**~~`BatchRunMetricsRefresher` 의 조회 창(7일)과 `verifyJob` SLA 의 그레인은 D 몫으로 남았다~~ 완료 · CY-470.**
창은 `BatchMetadataWindow.LOOKBACK_DAYS` 한 상수가 되어 SQL 리터럴 둘이 사라졌고,
SLA 그레인은 `cy_verify_last_success_seconds{dataset,scope}` 가 진다.

**~~같이 풀 것 — SLA 예산을 기동 때 검사하는 곳이 없다.~~ 완료 · CY-392(만료)·CY-397(정리).**
`ExpireScheduler`·`CleanupScheduler` 생성자가 `CronSlot.maxGap` 으로 각자 검사하고 안 맞으면
기동을 거절한다. 아래는 그 결정을 남긴 기록이다. 성립해야 하는 관계는
`(max-expire-skips + 1) × 크론 주기 + run-refresh-ms < SLA` 인데, 앞의 두 항이
환경변수로 자유롭게 커진다. `MAX_EXPIRE_SKIPS=3` 만 줘도 `4 × 300 + 60 = 1260 > 900` 이라
**아무 사고 없이 critical 이 하루 몇 번씩 뜬다.** `run-refresh-ms` 에는 상한 가드가 있는데
훨씬 큰 항이 무방비다. `batch.metrics.expire-sla-seconds` 설정 키를 파서 규칙·코드·문서가
한 값을 보게 하면 위의 아홉 자리도 함께 접힌다.

> **그 절반은 이미 됐다** — `CY-392`(`ac23406`)가 설정 키와 `CronSlot.maxGap` 을 넣고
> `(max-expire-skips + 1) × 크론 최대간격 + run-refresh-ms < SLA` 를 기동 때 검사한다.
> (값을 배치 창에 맞춰 180000 으로 다시 잡은 것은 CY-397 이고, 검증 크론이 서면서
> 90000 으로 되돌린 것은 CY-470 이다.)
> 남은 절반은 규칙 파일이다 — 프로메테우스는 앱 설정을 못 읽으므로
> `batch-alerts.yml` 이 아직 `90000` 을 **네 자리**에 박고 있다(만료·정리·검증·
> `ExpireMetricsBackdated`). 기동 가드의 거절 메시지가 그 규칙들을 **이름으로 부르는 것**이
> 지금의 방어이고, 그것을 기계 검사로 바꾸는 것이 남았다.

**~~C 는 A 의 남은 창을 못 닫았다 — D 몫이다~~ 완료 · CY-470.** `rejectRunningExpire` 는
통과 직후 만료가 발화하는 창을 못 막고 `assertFrozenStep` 이 그것을 잡는다(`docs/15`).
*"만료 04:10 · 검증 05:00 이면 겹칠 구조가 사라진다"* 고 적었는데 그때는 **그 검증 크론이
없었다** — 검증은 손 트리거뿐이라 시각을 안 가렸고, 그래서 C 는 상호 배제를 **끄는 대신
남겼다**(`max-expire-skips=1`). D 가 05:00 슬롯을 배정하며 그 구조를 없애고 `0` 으로 내렸다.

> ⚠️ **크론끼리는 안 겹치지만 손 트리거는 여전히 겹칠 수 있다.** 그리고 상한이 `0` 이라
> 이제는 **첫 충돌에서 만료가 지나간다** — 그 검증은 버려진다. `docs/15` 가 그 규약을 적는다.
>
> **남은 창 — 잡 전체 데드라인이 없다.** 아래 가드는 검증이 정상 상한
> (`verify-running-too-long-seconds`) 안에 끝난다는 **어림** 위에 선다. 이 저장소에는
> 실행 전체를 끊는 수단이 없어서(`step-timeout-ms` 는 청크 데드라인이다 — `.example` 의
> `expire.step-timeout-ms` 주석) 그 상한을 넘겨 도는 실행은 가드를 통과한 뒤에도 만료와
> 겹친다. **만료·정리·검증 셋에 공통인 빈자리**이고, 생기는 날 이 가드의 기준도 그 값으로
> 바꾼다. 그전까지는 "안 하는 것보다 낫다" 가 근거다 — 이 가드가 없으면 시연 시간대에
> 누른 검증이 **반드시** 버려진다.
>
> **~~남은 것 — 그 규약이 아직 코드가 아니다~~ 완료 · CY-470(CodeRabbit 지적).**
> `VerifyTriggerController` 가 *이미 도는* 만료만 보고(`rejectRunningExpire`) *곧 뜰* 만료는
> 안 봤다. 즉 13:05 KST 에 손 트리거를 걸면 접수는 통과하고 13:10 에 만료가 지나가 그 실행이
> 버려졌다 — 시연 직전이 하필 그 시각대다. 이제 접수 단계에서 **만료의 다음 발화 시각**을
> 보고 거절한다(`VERIFICATION-017`, 409). 판정 기준은 검증 최악 소요
> (`batch.metrics.verify-running-too-long-seconds`)이고, 크론 문자열은
> `ExpireStepContext.CRON` 한 자리에서 스케줄러와 함께 읽는다.

### C 가 함께 져야 하는 것 — 버려진 실행의 `asof_state`

`assertFrozenStep` 이 판정을 버리면 `finalizeRunStep` 이 안 돌아 실행 행이 열린 채 남는데
(설계 의도다), **그 실행이 이미 쓴 `asof_state` 최대 300만 행을 아무도 안 지웠다.**

CY-384 전에는 이 상황이 아예 못 생겼다 — 스케줄러를 켠 기동에서는 검증이 시작조차 못 했다.
지금은 양방향 가드가 대부분을 막지만 **마이크로초짜리 창과 하드킬은 남는다.**

**CY-397 의 `cleanupJob` 이 그 축을 진다.** 대상은 둘이다 —
`purgeableRunIds`(보존 창 밖)와 `abandonedRunIds`(`verdict IS NULL` 이면서 하루 지난 것).
`DELETE FROM asof_state` 는 이제 `CleanupJdbcAdapter#deleteAsOfStateChunk` 하나뿐이고,
**태스클릿 한 번이 청크 하나**라 커밋이 나뉜다(한 트랜잭션으로 300만을 지우면 `LIMIT` 을
붙여도 언두와 잠금은 전량을 통째로 든다).

> ⚠️ **하루라는 창의 근거는 미측정이다.** 300만 전수의 소요를 아직 안 쟀으므로(D) 고른
> 값이라, 그것 하나에 파괴적 삭제를 맡기지 않는다 — 정리 Step 이 청크마다
> `RunningJobProbe` 로 *"지금 검증이 도는가"* 를 먼저 묻고, 돌면 거기서 멈춘다.
> D 가 실측하면 이 값을 다시 정한다.

### 시체 실행 걷어내기 — `BatchStuckExecution` 이 가리키는 절

종료 표시를 못 남기고 죽은 실행은 `STATUS` 조회에 `END_TIME` 검사도 시간 상한도 없어
**영원히** 남는다. 그동안 만료↔검증 상호 배제가 그 실행에 대해 꺼져 있다(CY-384).

> **CY-429 가 `expireJob` 쪽에도 API 를 냈다.** 아래 손 SQL 은 이제 **참고용**이다 —
> 그 SQL 은 `VERSION` 을 올려 **살아 있는 실행의 다음 `update()` 를 터뜨리는데**,
> 새벽에 알림을 받고 깨어난 사람에게 그 판단을 맡기고 있었다.
> API 도 **같은 쓰기를 한다.** 다른 것은 임계가 코드에 있다는 것뿐이고, 그 판정은
> `LAST_UPDATED` 하트비트 휴리스틱이지 증명이 아니다 — `batch.stuck-job-after-ms` 를
> 내리는 변경은 이 API 의 안전을 직접 깎는다.

```bash
# ① 무엇이 남아 있나. 도는 실행은 여기 안 나온다.
curl -s localhost:9090/api/v1/admin/expire/runs/stuck | jq .data
#   [{ "executionId": 41, "status": "STARTED", "createTime": "...", "startTime": "...",
#      "lastProgress": "...", "stalledSeconds": 7412 }]

# ② 한 번이면 된다. 재시도해도 안전하다(FAILED + END_TIME 으로 판정한다).
curl -s -XPOST localhost:9090/api/v1/admin/expire/runs/41/recover | jq '.data, .error'
#   409 / EXPIRATION-007 이면 걷어낼 대상이 아니다 — ① 을 다시 본다.
#   404 / EXPIRATION-006 이면 만료 실행이 아니거나 없는 번호다.
```

`recover` 는 돌던 Step 까지 `FAILED` 로 닫는다. **`ABANDONED` 로 만들지 않는 것이 계약이다** —
그 상태는 `COMPLETED` 와 같은 취급이라 그 `JobInstance` 를 같은 `asOf` 로 영원히 못 돌린다.

업무 포트가 안 열려 있으면 `batch-expose.yml` 을 얹는다(§4). `verifyJob` 쪽은
`/api/v1/admin/verify` 에 `stop → abandon` 이 있다 — **그쪽 `stop` 에는 시체 판정이 없으므로**
프로세스 부재를 먼저 확인한다. `cleanupJob` 은 아직 API 가 없어 아래 손 SQL 이 유일한 길이다.

**아래는 API 가 없던 시절의 절차다.** 배치가 안 떠 있어 API 를 못 부르는 경우에만 쓴다.

```sql
-- 찾기. **임계를 SQL 에 건다** — 안 걸면 지금 정상적으로 도는 실행도 함께 나온다.
-- verifyJob 은 Step 열하나 중 열이 단발 태스클릿이라, 300만 전수 검증이 도는 내내
-- last_progress 가 정상인데도 몇십 분 전이다. 그것을 시체와 구별해 주지 않으면
-- 운영자가 살아 있는 검증을 걷어낸다.
SELECT e.JOB_EXECUTION_ID, i.JOB_NAME, e.STATUS, e.CREATE_TIME, e.START_TIME,
       p.last_progress
  FROM BATCH_JOB_EXECUTION e
  JOIN BATCH_JOB_INSTANCE i ON i.JOB_INSTANCE_ID = e.JOB_INSTANCE_ID
  LEFT JOIN (SELECT JOB_EXECUTION_ID, MAX(LAST_UPDATED) AS last_progress
               FROM BATCH_STEP_EXECUTION GROUP BY JOB_EXECUTION_ID) p
    ON p.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID
 WHERE e.STATUS IN ('STARTING','STARTED','STOPPING')
   -- ⚠️ **폴백을 여기 그대로 옮긴다** — RunningJobProbe.lastProgress 가
   --    MAX(LAST_UPDATED) → START_TIME → CREATE_TIME 순으로 떨어진다.
   --    `p.last_progress IS NULL` 로 두면 **Step 행이 아직 없는 실행이 무조건 나온다** —
   --    방금 뜬 STARTING 실행이 그 모양이라 살아 있는 잡을 시체로 신고한다.
   AND COALESCE(p.last_progress, e.START_TIME, e.CREATE_TIME)
       < NOW() - INTERVAL 30 MINUTE                        -- batch.stuck-job-after-ms
 ORDER BY e.JOB_EXECUTION_ID;
```

**닫기 전에 셋을 확인한다.**

1. **배치가 떠 있으면 먼저 `docker compose ps batch` 를 본다.** 아래 UPDATE 는 `VERSION` 을
   올리므로 **살아 있는 실행의 다음 `jobRepository.update()` 를 터뜨린다** — 잡이 중간에
   죽고, 그 실행이 쓴 `asof_state` 최대 300만 행이 아래 절의 상태로 남는다.
2. **API 를 먼저 쓴다.** `expireJob` 이면 `POST /api/v1/admin/expire/runs/{id}/recover`
   **한 번**이다(CY-429). `verifyJob` 이면 `/api/v1/admin/verify` 의 `stop → abandon`
   이다(CY-368). **모양이 다르다** — 만료 쪽은 `FAILED` 로 닫아 `asOf` 슬롯을 안 태운다(3번).
   `cleanupJob` 은 API 가 없어 아래 SQL 이 유일한 길이다.
   손 SQL 은 배치가 안 떠 있을 때만이다.
3. **`ABANDONED` 는 되돌릴 수 없다.** 그 상태는 `COMPLETED` 와 같은 취급이라
   (Spring Batch 6.0.4 의 실행 시작 경로가 그렇게 가른다) **그 JobInstance 를 같은
   파라미터로 다시 못 돌린다.** 만료는 `asOf` 가 식별 파라미터이므로 그 크론 슬롯을
   손으로 재시도할 방법이 사라진다. **그래서 API 는 `recover`(→ `FAILED`)를 쓴다** —
   `FAILED` 는 그 문을 안 닫는다. 만료 트리거는 일부러 안 만들었으므로(`docs/15`
   "트리거는 열지 않는다") 어느 쪽이든 손 재시도는 없고, 남은 대상은 다음 슬롯이
   함께 가져간다.

```sql
-- 닫기. **FAILED 다 — ABANDONED 가 아니다.** 둘 다 위 조회의 STATUS IN (...) 에서
-- 빠지는데, ABANDONED 는 TaskExecutorJobLauncher 가 COMPLETED 와 함께 막아(6.0.4)
-- 그 asOf 슬롯을 영원히 태운다.
-- EXIT_CODE 는 안 건드린다 — recover 도 setExitStatus 를 안 부르므로 하드킬된 행은
-- 'UNKNOWN' 인 채로 남는다(6.0.4). 두 경로의 산출물을 같게 두려면 여기도 그래야 한다.
-- ⚠️ **API 의 선점문과 같은 조건을 건다.** id 만 걸면 한 자리 오타가 **남의 잡**을,
--    또는 **살아 있는 실행**을 닫는다 — 그러면 그 실행의 다음 update() 가 낙관적 락에
--    걸려 죽고, 만료는 재고를 쓰는 유일한 잡이다.
--    :stuckBefore 는 batch.stuck-job-after-ms 만큼 과거다(기본 30분).
-- **변경 행 수를 반드시 확인한다.** 0이면 대상이 아니었다는 뜻이므로 멈춘다.
UPDATE BATCH_JOB_EXECUTION je
  JOIN BATCH_JOB_INSTANCE i ON i.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
   SET je.STATUS = 'FAILED', je.END_TIME = NOW(6), je.VERSION = je.VERSION + 1
 WHERE je.JOB_EXECUTION_ID = :id
   AND i.JOB_NAME = 'expireJob'
   AND je.STATUS IN ('STARTING','STARTED','STOPPING')
   -- 위 찾기와 **같은 판정**이어야 한다. NOT EXISTS 만 쓰면 Step 행이 없는 실행에서
   -- 무조건 참이 되어 :stuckBefore 와 무관하게 닫힌다.
   AND COALESCE((SELECT MAX(se.LAST_UPDATED) FROM BATCH_STEP_EXECUTION se
                  WHERE se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID),
                je.START_TIME, je.CREATE_TIME) <= :stuckBefore;

-- recover 는 Step 행도 닫는다. 손 SQL 도 같이 해야 두 경로의 산출물이 같다.
-- 위 UPDATE 가 1행일 때만 친다.
UPDATE BATCH_STEP_EXECUTION
   SET STATUS = 'FAILED', END_TIME = NOW(6), VERSION = VERSION + 1
 WHERE JOB_EXECUTION_ID = :id AND STATUS IN ('STARTING','STARTED','STOPPING');
```

> **`cleanupJob` 은 이 축을 안 진다 — CY-436 뒤에도 그렇다.** `purgeBatchMetadataStep` 은
> `END_TIME IS NOT NULL` 인 행만 걷는다(`CleanupJdbcAdapter#deleteBatchMetadataChunk`,
> `CleanupJobTest#keepsUnfinishedExecutionsHoweverOld` 가 고정). **시체를 지우면
> `BatchStuckExecution` 이 조용해지는데 그건 고친 게 아니라 증거를 지운 것**이다.
> 그 행은 CY-429 의 복구 API 가 사람의 판단으로 닫는다 — 위 절차가 그것이다.
>
> `BATCH_*` **메타 보존 삭제**는 별개 축이고 CY-436 이 졌다 — §4 운영 표를 보라.

> ⚠️ **아래 `asof_state` DELETE 와 헷갈리지 마라.** 그쪽은 버려진 **검증 실행**이 남긴
> 파생 행을 걷는 것이고 이 절과 다른 사고다.

**잡이 꺼져 있을 때(`BATCH_SCHEDULING_ENABLED=false`)의 수동 절차.** 평소에는
`cleanupJob` 이 04:30 에 같은 일을 하므로 손으로 칠 일이 없다 — 스케줄러를 끈 채 띄운
환경이나, `CleanupNotSucceeding` 이 울고 있는데 원인을 못 찾은 경우가 이 절의 대상이다.

```sql
-- 버려진 실행 찾기. 지우기 전에 반드시 눈으로 본다.
-- **잡과 같은 조건을 쓴다**(CleanupJdbcAdapter#abandonedRunIds). 조건이 갈리면 손 SQL 이
-- 잡은 안 건드리는 행까지 지운다 — 특히 origin='SEED' 는 게이트의 기준값이라 재시딩 말고
-- 복구가 없고, v_latest_stats_run 이 가리키는 행을 지우면 대시보드가 조용히 빈다.
SELECT r.id, r.as_of, r.dataset, r.scope, r.attempt, r.started_at,
       (SELECT COUNT(*) FROM asof_state s WHERE s.run_id = r.id) AS asof_rows
  FROM verification_runs r
 WHERE r.origin = 'BATCH'
   AND r.verdict IS NULL
   AND r.started_at < NOW() - INTERVAL 1 DAY
   AND r.id NOT IN (SELECT id FROM v_latest_stats_run)
 ORDER BY r.id;

-- 파생 행 걷기. 0행이 나올 때까지 반복한다.
-- LIMIT 은 batch.cleanup.chunk-size 와 같은 값을 쓴다 — 손과 잡이 다른 크기를 쓰면
-- 어느 쪽이 계약인지 모호해진다.
DELETE FROM asof_state WHERE run_id = :runId LIMIT 10000;
```

> ⚠️ **통계 셋은 손으로 지우지 마라.** 잡은 통계를 지울 때 `stats_status` 를 함께 내리는데
> (안 내리면 `v_latest_stats_run` 이 행 0개짜리 실행을 *"완결된 최신 스냅샷"* 으로 가리킨다),
> 그 짝을 손 SQL 로 맞추기 어렵다. **`COMPLETE` 였던 행만** 내려야 하는 것도 함께 지켜야 한다 —
> `SKIPPED` 는 *"오염셋이라 안 했다"* · *"불합격이라 안 했다"* 라는 뜻이 실린 값이다.
>
> 위 조회로 찾은 **버려진 실행**의 검출 행은 손으로 지워도 된다 — 설명할 판정이 없기 때문이다.
> ```sql
> DELETE FROM verification_findings WHERE run_id = :runId;   -- verdict IS NULL 인 실행만
> ```
> **판정이 있는 실행의 검출 행은 절대 지우지 마라.** `FAIL` 은 무엇이 틀렸는지의 유일한 근거이고
> (`VerificationVerdictFailed` 의 runbook 이 그 행을 가리킨다), 오염셋의 `PASS` 800행은
> *"누락 0 · 오탐 0"* 을 보여 주는 이 과제의 산출물이다.

**언제** — A·B·C 완료. D 는 300만 적재 뒤.

---

## 7. 테스트 컨테이너를 스프링 컨텍스트마다 띄우고 있다

**실측(2026-08-23).** `:batch:test` 가 도는 동안 `docker ps` 로 세었다.

| | |
|---|---|
| 동시에 떠 있는 MySQL 컨테이너 | **18개** |
| 컨테이너당 메모리 | 약 **450MB** |
| 합계 | 약 **8GB** |
| 개발 기기 Docker VM 가용 | **7.65GB** |

`MySqlContainerConfig` 가 `@Bean` 이라 **스프링 컨텍스트마다 컨테이너를 하나씩** 만든다
(`static` 홀더도 `withReuse` 도 없다). 스프링 테스트는 컨텍스트를 캐시하고 기본 상한이 32 라,
실제로 그 수만큼 mysqld 가 동시에 산다.

**그 선을 넘으면 컨테이너가 죽고 다음 컨텍스트가 `Connection refused` 로 실패한다.**
증상이 *"테스트가 틀렸다"* 가 아니라 **컨테이너 기동 실패**로 보여서 원인까지 가는 길이 멀다 —
실패 문자열의 `total=0, idle=0` 이 *"풀이 말랐다"* 가 아니라 *"서버가 없다"* 를 뜻한다는 것이
유일한 단서였다. CI 에서 실제로 세 번 그렇게 깨졌다(CY-392).

**지금은 `spring.test.context.cache.maxSize=4` 로 묶어 뒀다**(루트 `build.gradle`).
밀려난 컨텍스트가 닫히면 그 컨테이너도 내려간다 — 18개가 4개가 되는 것을 확인했다.
대가는 재생성 비용이고, 빌드가 여전히 5분대다.

**근본 해결은 컨테이너를 컨텍스트마다 안 띄우는 것이다.** 다만 단순 싱글턴이 안 된다 —
이 저장소는 **정상 스키마와 오염 스키마를 나눠** 써야 하고(`CorruptRepositoryTest` 가 그것을
명시한다), 락 측정 테스트는 `performance_schema` 를 요구한다. 그래서 **스키마 종류별로 갈린
JVM 싱글턴**이 필요하다.

**언제** — 빌드 시간이 발표 준비를 막을 때. 지금은 상한으로 버틴다.
`docs/13` §4c(빌드 캐시·병렬)와 같은 축이므로 함께 보는 것이 낫다.

---

## 이 문서를 쓰는 법

티켓으로 옮길 때 **"언제"** 를 그대로 선행 조건에 적는다. 조건이 안 열린 항목을 지금 하면
검증할 방법이 없는 채로 코드만 늘어난다 — 3번의 앞 두 줄이 정확히 그 상태다.
