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
| `docs/12` 재측정 절 추가 | 락·스캔 축은 위 테스트가 잡는다. 문서의 **절대 수치**는 300만 건 적재 후에 다시 재야 뜻이 있다 |
| `blockedCoupons` 소요 실측 | 축소 픽스처에서 잰 값은 운영 규모를 대변하지 못한다. 코드 주석에서 근거 없는 수치를 뺐다. **실행계획 축은 `keepsBlockedCouponScanProportionalToPending` 이 잡는다** — `uk_coupon_member` 를 타면 깨진다 |
| `unexplained` 이 캡처 창 지연분을 포함한다 | `COUNT_PENDING` 은 `updated_at` 창을 안 걸어, 설계상 다음 주기로 미룬 행도 "배치가 안 한 몫" 으로 센다. **지금은 도달 불가다** — `issuances` 를 쓰는 문장이 `EXPIRE_BATCH` 하나뿐이다. `CANCEL_USE`(`USED → ISSUED`)가 붙는 취소·사용 티켓에서 가른다 |
| `(coupon_id, status, expires_at)` 인덱스 | 막힌 회차의 대기 행이 매 주기 재스캔된다. **인덱스 도입은 실측 뒤 별도 판단이다** |
| 나머지 배치 테스트의 시계 고정 | 이 티켓이 건드린 테스트 중 **잡을 실제로 돌리는 것은 전부** `FixedClock` 으로 고정했다. `ExpireCancelRaceTest`·`ExpireJobHistoryGuardTest`·`ExpireJobIsolationTest`·`VerifyJob*` 은 아직 벽시계인데, 이 티켓이 안 건드린 파일이라 여기서 안 넓혔다. (storage 의 만료 테스트는 `asOf` 가 SQL 바인드 파라미터일 뿐이라 시계와 무관하다) |
| 리뷰 반복 결함의 기계적 검사 | 이 티켓의 리뷰에서 **같은 종류가 반복해서 나왔다** — 떠 있는 javadoc(4회), 개명 뒤 끊긴 문서 참조, 개수 주장과 실제 불일치("넷"↔"다섯"), 지표 단언 누락. 넷 다 파일을 읽어 기계적으로 잡을 수 있다. `BatchMetricExposureTest` 가 규칙 파일↔노출을 잇는 것과 같은 방식으로 테스트화할 자리다 |
| `README` 의 batch 패키지 트리 | `config`/`job`/`schedule`/`replay` 는 CY-347 **이전부터** 있었고 이 티켓은 새 패키지를 안 만든다. README 가 침묵일 뿐 모순이 아니라, 채우는 것은 소유자가 정할 별도 티켓이다 |

### 알림

```
cy_expire_blocked_coupons   게이지. 이번 실행이 제외한 회차 수
```

```yaml
- alert: ExpireSkippingBrokenCoupons
  expr: cy_expire_blocked_coupons > 0
  for: 10m          # 두 주기. 누가 재고를 고치는 중이면 자연 해소된다
  severity: warning # 배치는 성공했다. 데이터를 봐야 하는 상황이다
```

`BatchJobFailed`(critical)와 **반드시 갈라야 한다** — 하나는 서버를 보는 알림이고
이것은 데이터를 보는 알림이다.

---

## 2. 알림 — "성공했는데 맞게 했나" 를 아무도 안 본다

규칙 여섯 중 셋(`BatchJobFailed`·`BatchJobNotRunning`·`BatchJobRunningTooLong`)은
**잡의 생사**만 본다. 셋 다 통과하면서 아무것도 안 하는 상태가 있고, 2b 가 그 축 밖을 메웠다
(`ExpireLeavesWorkBehind`·`ExpireSkippingBrokenCoupons`·`ExpireMetricsUnknown`).

### 2a. 검증 판정이 알림으로 안 나간다 — 가장 큰 구멍

`verdict = FAIL` 은 **정상 종료**다(원칙대로). 그래서 **알림이 하나도 안 울린다.**
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
cy_expire_pending              기한이 지났는데 아직 ISSUED 인 발급건 수
cy_expire_blocked_pending      그중 막힌 회차의 몫
cy_expire_unexplained_pending  그 둘의 차. **알림이 보는 것은 이것 하나다**
```

> **이름에 `_total` 을 붙이면 안 된다.** 카운터 규약이라 Micrometer 의 Prometheus 렌더러가
> 게이지에서 **떼어 낸다** — 붙이면 코드가 부르는 이름과 관제가 보는 이름이 갈린다.
> 실제로 그렇게 만들었다가 노출 테스트가 잡았다.

> **차를 알림 식에서 빼면 안 된다.** 게이지 둘을 따로 `set` 하는 사이에 스크레이프가 끼면
> 한쪽만 새 값인 샘플이 나온다. 한 문장에서 세어 한 시계열로 내보낸다.

> **스크레이프 때 세면 안 된다.** 300만 행에 `COUNT(*)` 를 15초마다 때리는 꼴이다.
> 잡이 끝나는 시점에 한 번 세서 게이지에 넣는다 — 그때 이미 같은 창을 훑고 있다.

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

### 2e. Prometheus 가 규칙을 아직 안 읽는다 — **위 전부의 선행 조건**

규칙 파일은 있는데 **읽는 프로세스가 없다.** `prometheus.yml` 이 그 사실을 스스로 적어 뒀다.
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
| `BATCH_*` 정리 | **`cleanupJob` 티켓.** 하루 288 인스턴스. 3주 시연(약 3.6만 행)은 무시할 수 있다. `batch.metadata.keep-days` 자리를 설정에 예고해 뒀다 |
| 업무 포트 노출 | ~~compose 티켓~~ ~~CY-359~~ **CY-368 에서 다시 정했다.** 그 포트에 인증 없는 admin 트리거가 열려 `batch.yml` 은 업무 포트를 **아예 안 내보낸다** — 필요할 때만 `batch-expose.yml` 을 얹어 `127.0.0.1:${BATCH_HOST_PORT:-9090}:9090` 으로 연다. 관리 포트(9092)는 어느 경우에도 안 올린다 |

---

## 4a. 검증용 셋에 Spring Batch 메타 테이블이 없다 (CY-359 가 발견)

`coupon_clean`·`coupon_corrupt` 는 cy-seed 의 `ddl/00_schema.sql` 로 만들어지는데
`CREATE TABLE` 17개 중 **`BATCH_*` 는 0개**다. cy-be 의 `V2__batch_metadata.sql` 은
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
| 문서화된 수동 절차로 둔다 (현재) | `docs/14` 시연 절차 2번에 명령을 박아 뒀다. 사람이 빠뜨리면 가드가 잡는다 |

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

| | 지금 | 가야 할 곳 |
|---|---|---|
| 만료 | 5분 크론 | 배치 창 04:10 (일 1회) |
| 검증 | 온디맨드만 | 배치 창 05:00 (일 1회 전수) |
| 정리 | 1시간 | 배치 창 04:30 (일 1회) |

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
| **B** | `expire`·`verify` 의 `last_success_seconds` 게이지 + SLA 알림. `BatchJobNotRunning` 대체. **진도가 멈춘 실행(`cy_stuck_executions`)도 여기서 지표로 낸다** — 지금은 WARN 로그가 유일한 신호다 | — |
| **C** | 만료 04:10 · 정리 04:30 으로 이동 + 시연용 크론 override. **정리에 `asof_state` 를 넣는다** — 아래 참조 | A · B |
| **D** | 300만에서 검증 소요 실측 → 05:00 슬롯 배정. `BatchJobRunningTooLong` 의 `verifyJob` 임계도 여기서 | C · 300만 적재 |

**B 없이 C 를 하면 감시 공백이 생긴다.** `BatchJobNotRunning` 은 15분 창의 증분으로 보는데,
일 1회로 옮기면 그 창이 **하루의 대부분 비어** 영구 critical 이 된다. 규칙이 틀린 게 아니라
축이 안 맞는 것이라, 먼저 *"마지막 성공이 언제였나"* 로 갈아야 한다.

**C 가 A 의 남은 창도 닫는다.** `rejectRunningExpire` 는 통과 직후 만료가 발화하는 창을
못 막고 `assertFrozenStep` 이 그것을 잡는다(`docs/15`). 만료 04:10 · 검증 05:00 이면
겹칠 구조 자체가 사라지므로, 상호 배제를 더 만들 필요가 없다.

### C 가 함께 져야 하는 것 — 버려진 실행의 `asof_state`

`assertFrozenStep` 이 판정을 버리면 `finalizeRunStep` 이 안 돌아 실행 행이 열린 채 남는데
(설계 의도다), **그 실행이 이미 쓴 `asof_state` 최대 300만 행은 아무도 안 지운다.**
`DELETE FROM asof_state` 는 저장소의 테스트 밖에 존재하지 않는다.

CY-384 전에는 이 상황이 아예 못 생겼다 — 스케줄러를 켠 기동에서는 검증이 시작조차 못 했다.
지금은 양방향 가드가 대부분을 막지만 **마이크로초짜리 창과 하드킬은 남으므로**, 정리 잡이
`verdict IS NULL AND started_at < NOW() - INTERVAL 1 DAY` 를 훑어 파생 행을 걷어야 한다.
한 문장으로 300만을 지우면 언두 로그가 터지므로 `LIMIT` 으로 나눠 지운다.

**정리 잡이 오기 전까지는 손으로 한다.** 그 티켓이 최소 둘 뒤라, 지금 이 상황을 만난
운영자가 칠 것을 여기 적어 둔다.

```sql
-- 버려진 실행 찾기. 지우기 전에 반드시 눈으로 본다.
SELECT r.id, r.as_of, r.dataset, r.scope, r.attempt, r.started_at,
       (SELECT COUNT(*) FROM asof_state s WHERE s.run_id = r.id) AS asof_rows
  FROM verification_runs r
 WHERE r.verdict IS NULL
   AND r.started_at < NOW() - INTERVAL 1 DAY
 ORDER BY r.id;

-- 파생 행 걷기. 0행이 나올 때까지 반복한다.
DELETE FROM asof_state WHERE run_id = :runId LIMIT 50000;
```

**언제** — B 는 지금. C 는 B 뒤. D 는 300만 적재 뒤.
`asof_state` 정리만은 B 를 안 기다려도 된다 — 이 티켓이 그 누수를 처음 도달 가능하게 만들었다.

---

## 이 문서를 쓰는 법

티켓으로 옮길 때 **"언제"** 를 그대로 선행 조건에 적는다. 조건이 안 열린 항목을 지금 하면
검증할 방법이 없는 채로 코드만 늘어난다 — 3번의 앞 두 줄이 정확히 그 상태다.
