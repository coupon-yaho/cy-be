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
| `ExpirationRepository` · `ExpirationJdbcAdapter` | ✅ 포트가 둘 늘었다(`blockedCoupons`·`countPending`). **둘 다 락을 안 잡는 읽기라 락 순서 계약 밖**이고 그 성질을 `ExpirationLockScopeTest.readOnlyQueriesTakeNoLocks` 가 계측으로 지킨다. **그 뒤 락 순서 자체가 뒤집혔다** — 계약을 지는 것은 이제 **쓰는 넷**(`lockStock`→`expireBatch`→`appendExpireHistories`→`releaseStock`)이고, `nextCandidates` 가 읽기 쪽에 하나 더 늘었다. 근거는 `docs/12` §11 |
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
  가 제외 목록이 **채워진** 상태로 락과 읽은 행을 잰다. 제외 술어가 `V2026082510 (status, expires_at)`
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
| ~~`docs/12` 절대 수치 재측정~~ **다시 쟀다 · CY-744** | **300만 발급·534만 이력에서 `verifyJob` FULL 이 평균 180.8초**(3회 편차 ±2%, Step 별 표는 `docs/12` §8). 같은 실행에서 결정론 2회 일치(지문·checksum)도 확인했다. ⚠️ **옛 472초는 낡은 게 아니라 재현 불가였다** — 그 셋은 `coupon_templates.created_at` 이 없어 배치 앱이 기동조차 못 한다. `usageCountStep` 이 30%(2위)로 올라온 원인은 아직 안 쟀다. 아래는 옛 기록이다 — 300만·516만에서 `verifyJob` FULL 이 **472초**, **만료가 340,529건에 9.64초**(초당 35,300건)다(`docs/12` §8). 만료는 `issuances` 를 쓰므로 원본에서 돌리면 `dataset_fingerprint` 가 바뀌어 교차 검증 기준선을 잃는다 — `coupon_clean` 을 `coupon_expire_bench` 로 복제해 거기서 쟀고 **원본은 안 건드렸다**. ⚠️ 처음엔 `expires_at` 을 한 값으로 덮어 쟀는데 옵티마이저가 **커버링 인덱스 스캔**을 골라 원본의 **PK 레인지 스캔**과 다른 것을 재고 있었다. 창을 통째로 6개월 미는 방식으로 분포(고유값 340,254개)를 살리고 `ANALYZE TABLE` 로 통계를 갱신해 계획을 맞춘 뒤의 값이다. 앞선 실측점(859,471건 25.7초)과 처리량이 6% 안에서 일치해 건수에 선형이다. **평상시는 훨씬 가볍다** — 만료일이 28일에 걸쳐 있어 하루 최대가 22,963건이다 |
| ~~`blockedCoupons` 소요 실측~~ **닫혔다 · CY-742** | 300만 발급 원본에서 직접 쟀다(읽기 전용이라 원본에서 안전하다). **평상시 대기 8,183건에 28.9ms**, **28일치가 한꺼번에 대기하는 상황(340,529건)에 333ms** — 만료 잡 전체(9.64초)의 **3.5%** 다. 실행계획 축은 여전히 `keepsBlockedCouponScanProportionalToPending` 이 잡는다(`uk_coupon_member` 를 타면 깨진다). 대기 건수에 비례하고 절대값이 작아 `(coupon_id, status, expires_at)` 인덱스의 근거로는 약하다 |
| `unexplained` 이 캡처 창 지연분을 포함한다 | `COUNT_PENDING` 은 `updated_at` 창을 안 걸어, 설계상 다음 주기로 미룬 행도 "배치가 안 한 몫" 으로 센다. **지금은 도달 불가다** — `issuances` 의 상태를 쓰는 문장이 `EXPIRE_BATCH` 하나뿐이다. `CANCEL_USE`(`USED → ISSUED`)가 붙는 취소·사용 티켓에서 가른다 |
| ↑ **CY-421 이 그 축을 알림 경로로 넓혔다** | 되읽기가 `COUNT_PENDING` 을 60초마다 다시 치므로, 실행이 끝난 **뒤에** `CANCEL_USE` 로 되돌아온 행이 새로 세어진다. 그 회차는 얼린 제외 목록에 없어 `unexplained` 로 들어가고 `ExpireLeavesWorkBehind`(critical · server)가 뜬다 — **배치는 안 틀렸는데 서버를 보라고 나가고, 만료가 일 1회라 최대 하루 간다.** `afterJob` 이 종료 시점에 한 번 세던 시절에는 구조적으로 불가능했다. 같은 티켓에서 `updated_at` 창과 함께 본다. **선행 조건이 있다** — `EXPIRE_BATCH` 의 창은 `updated_at <= :committedAt` 인데 그 `committedAt` 은 청크마다 새로 잡히고 영속되지 않아, 되읽기가 같은 창을 걸려면 마지막 청크의 값을 Step 문맥에 먼저 실어야 한다 |
| ~~`(coupon_id, status, expires_at)` 인덱스~~ **안 만든다 · CY-742 에서 실측** | **넣어도 이득이 없다.** 리더 `NEXT_CANDIDATES` 는 선두 컬럼 조건이 `coupon_id NOT IN (...)` 이라 **등치가 아니어서 못 탄다**. 라이터 `EXPIRE_BATCH` 는 등치·등치·범위라 **탈 수는 있지만** 이미 `idx_issuance_status_id` 로 **rows=1** 이라 줄일 것이 없다(EXPLAIN 실측). **대신 진짜 개선 여지는 리더의 계획 선택에 있다** — 옵티마이저가 이미 있는 `idx_issuance_status_id` 를 안 고르고 PRIMARY 로 145만 행을 훑는다. `ISSUED` 가 id 꼬리(1,768,045~3,000,000)에 몰려 있어 비용이 **첫 청크 한 번에만** 쏠린다 — 키셋이라 그 청크가 끝나면 `afterId` 가 곧바로 1,770,631 로 점프한다(실측). 그 한 번이 **약 0.35초**이고 잡 전체 9.64초의 **3.6%** 다. `docs/12` §8 의 `afterId` 별 표는 **누적이 아니라** "테이블이 커지면 첫 청크가 얼마나 자라는가" 다. `FORCE INDEX(idx_issuance_status_id)` 면 **1.4ms** 로 평평해진다. **지금은 안 넣는다** — 이득이 3.6%인데 힌트는 옵티마이저를 못 박아 분포가 바뀌면 역효과다. ⚠️ **다시 볼 기준: 첫 청크가 1초를 넘으면 그때 넣는다.** 이 비용은 테이블 크기에 비례해 자라므로(3천만 건이면 약 3.5초) 규모가 커지면 반드시 다시 잰다 |
| 나머지 배치 테스트의 시계 고정 | **범위 밖 — 고칠 것이 없다(CY-718 에서 실측).** 잡을 돌리면서 시계를 안 고정한 열하나를 전수로 봤다: `VerifyJobConfig` 은 `timeProvider.now()` 를 **0회** 읽어 `VerifyJob*` 여덟이 잡 경로로 시계에 안 닿고, `ExpireJobHistoryGuardTest`·`ExpireJobIsolationTest` 는 **시각 단언이 0건**이다. `VerifyJobExpireGuardTest` 의 `now()` 둘은 **일부러**다 — `RunningJobFixture` 는 조회 창이 DB 의 `NOW()` 를 기준으로 자르므로 심는 값이 그 시계와 같은 흐름 위에 있어야 한다(`CleanupJobTest` javadoc 이 그 예외를 적어 뒀다). 고정하면 안 바뀌거나 픽스처가 깨진다 |
| JVM 기본 존을 UTC 로 강제하는 기동 가드 | **닫혔다(CY-718).** `DefaultZoneGuard` 가 고정 오프셋 0 이 아니면 기동을 거절한다. **근거를 두 번 틀렸다가 실측으로 고쳤다.** 처음엔 `RunningJobProbe` 를 근거로 들었는데 그것은 쓰기·읽기가 같은 `Timestamp` 축을 대칭으로 지나 **안 어긋난다**(KST 에서 `16:42:55` 로 심고 `jobRepository` 로 읽으면 그대로다). 다음엔 *"원시 `LocalDateTime` 바인딩은 다 깨진다"* 로 적었는데 그것도 절반만 맞았다 — `TimestampBindingAxisTest` 로 **서버가 실제로 본 값**을 재 보니 원시 바인딩은 벽시계를 **그대로**(`16:42:55`) 보내고 `Timestamp.valueOf` 는 세션 존으로 정규화해(`07:42:55`) 보낸다. 즉 갈리는 것은 바인딩 방식이 아니라 **자바 쪽 값이 어느 존인가**다. 값이 배치 메타에서 온 선점문의 `:stuckBefore` 는 어긋났다 — **부호를 따라 갈린다**: UTC 동쪽이면 진도 조건이 **항상 참**이 되어 살아 있는 실행을 닫고, 서쪽이면 반대로 창이 넓어져 진짜 시체도 오프셋만큼 늦게 걷힌다. `StuckRunClaim.claim` 이 **바인딩까지 감쌌고**, 그것만으로는 SQL 상수가 패키지 공개라 우회가 되므로 `StuckBeforeBindingIsCentralizedTest` 가 소스를 훑어 바인딩이 한 곳뿐인지 센다(우회·변환 제거 돌연변이 둘 다 사망). 반대로 `cleanupJob` 의 메타 컷오프는 값이 `TimeProvider`(UTC)라 **원래부터 같은 축**이고, 한때 "여기도 깨진다" 고 적혀 있던 그 문장을 믿고 `Timestamp.valueOf` 를 씌웠으면 **멀쩡하던 자리를 아홉 시간 밀 뻔했다**. 이름이 아니라 **오프셋**으로 보고(`UTC`·`Etc/UTC`·`Z`·`GMT` 가 같은 좌표계다), `Europe/London` 처럼 전이가 있는 존은 지금 0 이어도 거절한다. 테스트 JVM 이 일부러 KST 라는 충돌은 `batch/src/test/resources/application.yml` 이 **한 곳에서** 끄는 것으로 갈랐다 |
| ~~`verification_runs.started_at` 이 `as_of` 와 다른 축~~ **닫혔다 · CY-743** | `as_of`·`from_ts` 는 `TimeProvider`(UTC) 축인데 `started_at`·`finished_at` 은 스프링 배치가 **인자 없는 `LocalDateTime.now()`** 로 찍은 **JVM 기본 존** 벽시계라, 원시 바인딩으로 저장되면 두 축이 오프셋만큼 벌어졌다. **값의 뜻은 안 바꿨다** — `.coderabbit.yaml` 이 출처를 `JobExecution.getStartTime()`·판정 Step 의 `StepExecution.getStartTime()` 으로 못 박았고 `TimeProvider` 로 갈아타는 것은 규약 위반이다. `BatchTimeAxis#onDomainAxis` 가 **호출부에서** 같은 순간의 UTC 벽시계로 옮긴다. ⚠️ **저장 계층에서 고치려다 되돌렸다** — 어댑터 바인딩을 `Timestamp.valueOf` 로 감쌌더니 도메인 불변식(`asOf <= startedAt <= finishedAt`)이 두 값을 같은 축으로 전제해서 **테스트 21개가 죽었다**. 어댑터는 값의 출처를 모르므로 존이 섞이는 지점(호출부) 하나에서 닫는 것이 맞다. **읽는 쪽 셋을 전수로 확인했다**: ⑴ `CleanupJdbcAdapter` 의 `purgeableRunIds`·`abandonedRunIds` 가 `started_at` 을 `TimeProvider`(UTC) 컷오프와 **비교**한다 — 이 변경이 그 비교를 **처음으로 같은 축에** 세운다(UTC 서쪽 존이었다면 살아 있는 검증까지 삭제 대상으로 끌어들였다). ⑵ `VerifyReportView` 는 DB 행을 그대로 싣고 `scripts/dump-verify-report.sh` 가 `date -u` 로 파싱하므로 UTC 가 정답이다. ⑶ `VerifyRunView`·`StuckRunView` 는 배치 메타에서 직접 읽어 **축이 안 맞았고** 같은 커밋에서 함께 옮겼다. ⚠️ `startedAt` 은 이제 두 API 가 같은 값이지만 **`finishedAt` 은 축만 같아지고 값은 다르다** — `/runs` 는 잡 종료 시각, `/report` 는 판정 Step 시각이라 그 사이에 통계 집계가 있다. 검사는 **넷**이다 — `BatchTimeAxisTest`(변환 자체 · 동쪽·서쪽·UTC·DST 겹침), `VerifyRunAxisTest`(잡을 돌려 DB 벽시계), `VerifyRunViewAxisTest`(조회 응답 · 돌연변이 4종 사망), `BatchMetaTimeBudgetTest`(배치 메타 시각을 읽는 자리를 세어 **새 자리가 변환 없이 새는 것**을 막는다). ⚠️ **백필은 안 했다** — 배포 JVM 이 처음부터 UTC 라 기존 행도 이미 UTC 라고 보지만, **확인한 것이 아니라 추론이다** |
| admin API 에 읽기 타임아웃이 없다 | **닫혔다(CY-697).** 조회·중단 경로에 `@Transactional(readOnly, timeoutString="${batch.admin.timeout-seconds:5}")` 를 건다 — **트랜잭션 밖이면 `DataSourceUtils` 가 `queryTimeout` 을 안 붙여 끊을 수단이 아예 없다**(형제 `BatchRunMetricsRefresher` 가 같은 근거로 같은 값을 쓴다). 트리거만 뺐다 — 감싸면 새 실행의 메타 쓰기가 이 트랜잭션에 들어와 롤백 시 행이 사라진다. JDBC URL 에도 `connectTimeout`·`socketTimeout` 을 걸고 `DataSourceTimeoutGuard` 가 **가장 긴 Step 데드라인보다 큰지** 기동 때 검사한다 |
| `cleanupJob` 시체를 걷을 API | **닫혔다(CY-697).** `CleanupAdminController` + `CleanupRecoveryService` 로 `/api/v1/admin/cleanup/runs/stuck`·`/recover` 를 연다. **만료식 한 방**이다 — 검증식 2단계가 아닌 근거 셋을 실측했다: 지킬 살아 있는 입력이 없고, 업무 데이터를 안 쓰고, 아무도 `cleanupJob` 이 도는지 안 봐서 막고 있는 것도 없다. 선점 문장은 `StuckRunClaim` 에 모아 만료와 한 곳에서 쓴다 |
| `verifyJob` 의 `stop` 에 시체 판정이 없다 | **닫혔다(CY-678).** 기본은 진도가 멈춘 실행만 받고(409 `VERIFICATION-019`), 판정과 쓰기를 `VerifyStopService` 의 선점 `UPDATE` 로 한 트랜잭션에 묶었다. 도는 검증을 정말 세워야 하면 **컨테이너를 내린다**(`docs/11` 의 결정). 만료와 검증이 같은 컨테이너라 내리면 만료 크론도 안 떠서 `updated_at` 오염이 안 일어난다 — 인증 없는 API 에 강제 중단 파라미터를 여는 것보다 낫다 |
| 리뷰 반복 결함의 기계적 검사 | **부분 완료.** 떠 있는 javadoc 은 CY-686 이 `NoOrphanJavadocTest` 로 기계화했고 CY-697 에서 곧바로 한 건 잡았다. **나머지 셋은 근거가 없어 안 만든다(CY-718 에서 실측)** — 개명 뒤 끊긴 참조는 후보 11건 중 진짜가 **0건**이고(프레임워크 타입·아직 없는 영역①② 클래스·중첩 클래스·"예전 이름은…" 역사 서술), 개수 주장은 "넷" 이 무엇의 넷인지를 문장이 정해 기계로 못 잡으며, 지표 단언은 등록 31개 중 고아가 **0건**이다(CY-718 에서 다시 셌다 — 15 는 옛 숫자였다). 만들면 아무것도 안 잡으면서 오탐만 낸다 |
| ~~`README` 의 batch 패키지 트리~~ | **해결됐다** — README 에 `api`/`config`/`job`/`replay`/`schedule` 트리가 들어갔다 |

### `stop` 이 실제로 무엇을 하는가 — 철회했다가 되돌린 기록 (CY-661)

**CY-661 이 이 항목을 "하면 안 된다" 로 철회했다가 실측에서 깨져 되돌렸다.**
철회 근거는 `VerifyTriggerController` 의 javadoc 이었다:

> `stop` 으로는 못 푼다 — 상태가 `STOPPING` 이 되어 위 목록에 그대로 있다.
> 살아 있는 프로세스가 그 신호를 받아 종료시켜 줘야 하는데, 그 프로세스가 이미 없다.

**그 문장이 틀렸다.** Spring Batch 6.0.4 바이트코드로 확인했다.

```
SimpleJobOperator.stop()    setStatus(STOPPING) · setExitStatus · setEndTime(now()) · update()
SimpleJobRepository.update  if (status == STOPPING && getEndTime() != null)
                                "Upgrading job execution status from STOPPING to STOPPED
                                 since it has already ended."
                                upgradeStatus(STOPPED)
```

**`stop()` 이 항상 `endTime` 을 채워 넘기므로 그 조건은 항상 참이다.** `STOPPING` 에 굳지
않고 **즉시 `STOPPED`** 가 된다 — 시체든 살아 있든 똑같다. `findRunningJobExecutions` 는
`STATUS IN ('STARTING','STARTED','STOPPING')` 을 보므로 `STOPPED` 는 그 목록에서 빠지고
**429 도 그 자리에서 풀린다.**

**그래서 시체 판정을 걸어도 복구 경로가 안 막힌다** — 다만 **`batch.stuck-job-after-ms`
(기본 30분) 뒤부터** 통과한다(`RunningJobProbe.isStuck`). 복구가 막히는 것이 아니라
**30분 늦어진다.** 만료 쪽(`ExpireRecoveryService`)이 이미 같은 대가를 치르고 있으므로 축은
같다. **구현할 때 거절 메시지에 그 임계와 남은 시간을 실어야 한다** — 안 실으면 사람이
API 가 깨진 줄 안다. 어쨌든 철회의 전제는 무너졌다.

**오히려 원래 우려가 더 크다.** 결과를 둘로 갈라야 한다.

**`stop` 단독으로 도는 검증이 죽는다.** `SimpleJobRepository.update(StepExecution)` 이
`isStopped() || isStopping()` 일 때 `setTerminateOnly()` 를 세우고, 다음 Step 경계에서
`JobInterruptedException` 이 난다 — `finalizeRunStep` 에 못 가므로 `verdict` 가 애초에
안 남는다. **472초가 버려지는 것은 여기다. `abandon` 이 오든 안 오든 그렇다.**

**`abandon` 이 겹치면 종단이 `ABANDONED` 가 된다.** `upgradeStatus` 가 max 를 취하므로
나중에 돌아온 스레드가 못 되돌린다. 그 대가는 §7 이 적은 것 — **그 `JobInstance` 를 같은
`asOf` 로 영원히 못 돌린다.**

**그리고 `stop` 이 푸는 것은 429 하나가 아니다.** `STOPPED` 는 `findRunningJobExecutions` 의
`STATUS IN ('STARTING','STARTED','STOPPING')` 에서 빠지고, **그 목록을 보는 자리가 셋**이다 —
`VerifyTriggerController.runningExecutions()`(429 판정) ·
`ExpireScheduler`(만료 슬롯 건너뛰기, `blockingExecutions(verifyJob)`) ·
`CleanupJobConfig`(정리가 청크마다 물러나기, 같은 호출). `STOPPED` 가 박히는 순간 **셋이 함께
빈 목록을 본다** — 스레드가 아직 도는 동안 **만료·정리와의 상호 배제가 꺼진다.** `CleanupJobConfig` 가
그 보호의 값을 적어 뒀다: *"도는 검증의 입력이 걷힌다. 그때 V1·V3·V5 는 빈 상태를 읽고 예외
없이 조용히 틀린 답을 낸다."*

**그래서 이 항목은 `stop` 에 시체 판정을 거는 것이지 429 만 되돌리는 것이 아니다.**

> ⚠️ **`VerifyTriggerController` 의 그 javadoc 도 같이 틀렸다.** CY-661 은 문서만 만졌으므로
> 코드 주석은 안 고쳤다 — **이 항목을 구현하는 티켓이 함께 고친다.**

> **내가 실측 없이 믿은 자리다.** 그 javadoc 이 *"그럴 것이다"* 였고, 그것을 근거로 백로그
> 항목을 철회했다. 리뷰가 재현으로 잡았다. `docs/00` 의 원칙이 금지하는 그것을 했다.

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

### 2b. ~~만료 누락~~ **완료 · CY-347**

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

### 2c. ~~처리량 — "돌기는 도는데 안 줄어든다"~~ **완료 · CY-651**

`writeCount` 는 `BATCH_STEP_EXECUTION` 에만 남고 **알림이 SQL 을 못 읽었다.** Micrometer 가
자동 등록하는 것은 실행 횟수와 진행 중 시간뿐이라 처리량 축이 통째로 없었다.

```
cy_expire_processed_total   카운터. 청크마다 실제로 넘긴 건수를 더한다
```

**후보 수가 아니라 실제 만료 건수다.** 후보를 읽은 뒤 재고를 잠그기 전까지 사이에
사용·취소된 건은 안 들어온다 — 그것이 *"배치가 한 일"* 의 정의다. 충전율
(`cy_expire_chunk_fill`)은 **후보** 기준이라 둘이 갈릴 수 있고, 갈리는 것이 정상이다.

**커밋된 뒤에 센다**(`afterCommit`). 가드를 지나는 것만으로는 부족하다 — 태스클릿이 반환한
**뒤**에 커밋되므로, 커밋 자체가 실패하면(트랜잭션 타임아웃 · 커밋 시점 1213) DB 는 롤백되는데
카운터는 트랜잭션에 참여하지 않아 **되돌아가지 않는다.** 그리고 `ExpireMakingNoProgress` 가
이 값의 증가를 *"진도가 나갔다"* 로 읽으므로, **롤백된 청크가 알림을 침묵시킨다.**

형제인 `cy_expire_chunk_fill` 은 안 옮겼다. 분포라 롤백된 표본이 섞여도 평균이 조금 흔들릴
뿐이고, 알림이 그것으로 판정하지 않는다.

```yaml
- alert: ExpireMakingNoProgress
  expr: >-
    cy_expire_unexplained_pending > 0
    and increase(cy_expire_processed_total[26h]) == 0
  for: 30m
```

두 조건을 **AND 로 묶는 것이 핵심**이다. 처리량 0 자체는 정상이다 — 만료할 게 없는 날이
대부분이다. **남은 대상이 있는데 0** 인 것이 사건이다.

> **`cy_expire_pending` 이 아니라 `cy_expire_unexplained_pending` 이다.** 막힌 회차의 대기는
> 배치가 일부러 건너뛴 몫이라, 그것으로 묶으면 오염 회차 하나가 남아 있는 동안 영구히 울린다.

> **창이 만료 주기(24h)보다 넓어야 한다.** 처음에 30분으로 뒀는데, 만료가 하루 한 번 04:10 이라
> **하루 1,440분 중 1,410분 동안 좌변이 항상 참**이었다 — 남는 것은 `unexplained > 0` 하나이고
> 그것이 곧 `ExpireLeavesWorkBehind` 의 식 전체다. `repeat_interval` 이 1시간이라 그 중복이
> 시간당 한 번씩 Slack 으로 나간다. **§2 를 닫으려는 티켓이 아무도 안 보게 만드는 알림을 하나
> 더 붙일 뻔했다.**

> **게이지가 좌변이어야 한다.** `and` 는 **좌변의 값**을 낸다. `increase(...) == 0` 을 좌변에
> 두면 `$value` 가 **항상 0** 이라 summary 가 *"아무것도 안 넘기고 있는데 남은 대기가 0건"* 이라는
> 자기모순이 된다. `promtool` 이 실제로 그렇게 렌더링했다.

> **`offset 26h` 가 배포 직후 오탐을 막는다.** 새로 뜬 프로세스의 시계열은 창을 다 못 채우는데
> `increase()` 는 그것을 **0** 으로 낸다 — 재현: 90분치 시계열에 `unexplained=5` 만 있어도
> 85분에 떴다. `offset 26h` 항은 26시간 전에 표본이 있어야 매칭되므로 그때까지 조용하다.
> 값 비교가 아니라 **존재 검사**다.

> **셋 다 `promtool` 유닛 테스트로 못 박았다**(`infra/prometheus/tests/batch-alerts_test.yml`).
> 그 파일이 스스로 적어 둔 이유가 *"문법은 맞는데 영원히 안 뜨는 상태로 바뀌어도 CI 가 초록"* 인데,
> 이번에 그 파일을 안 건드릴 뻔했다. `exp_annotations` 에 `$value` 가 든 문장을 넣는 것이
> 그 버그를 잡는 유일한 단언이다.

### 2d. ~~실패 원인을 알림이 못 가른다~~ **완료 · CY-651**

`spring_batch_job_seconds_count` 에는 **에러코드 라벨이 없고 붙일 수도 없다** — 그 미터는
Spring Batch 가 만들고 태그 집합이 고정이다. 그래서 만료의 실패 자리 다섯이 **한 시계열로
뭉쳐** 나왔고, 어느 자리였는지는 배치 로그의 `EXPIRATION-00N` 으로만 갈렸다.

새 카운터를 `JobExecutionListener` 로 붙였다(`ExpireFailureMetrics`).

```
cy_expire_failures_total{error_code}   만료 실패 횟수. 에러코드로 가른다
```

| 코드 | 봐야 할 곳 |
|---|---|
| `EXPIRATION-001` 이력 수 불일치 · `002` 재고 행 없음 · `003` 재고 언더플로 | **코드** |
| `EXPIRATION-004` `asOf` 가 미래 | **넘긴 파라미터** |
| `EXPIRATION-005` 오염 스키마 | **접속 URL** |
| `UNCLASSIFIED` | **`BusinessException` 이 아닌 실패.** 갈래 셋 — `BinlogFormatGuard`(`binlog_format` 이 `STATEMENT`) · `asOf` 파라미터 누락 · DB 예외(1213·1205·트랜잭션 타임아웃) |

**실패 전에 여섯을 전부 0 으로 등록한다**(잡 실패 다섯 + `UNCLASSIFIED`). 이유가 둘이다.
⑴ 시계열이 실패 순간에 처음 생기면 그 창 안에 표본이 하나뿐이라 `increase()` 가 증가분을
못 낸다 — **첫 실패가 조용히 넘어가고** 두 번째부터 울린다.

> **잔여 간격이 있다.** 레지스트리에 0 을 만드는 것과 Prometheus 가 그 0 을 **긁는 것**은
> 다르다. 기동 직후 첫 스크레이프(15초) 전에 실패하면 첫 표본이 이미 1 이라 같은 문제가 남는다.
> 다만 그 창은 **15초**이고, 만료는 크론(04:10)이라 기동 몇 시간 뒤에 돈다 — 사전 등록이
> **모든 첫 실패를 놓치던 것**을 그 15초로 줄인 것이다. 완전히 없애려면 실패 시각 게이지가
> 필요한데(첫 표본만으로 판정된다) 그건 별도 축이라 여기서 안 넓혔다.
⑵ `BatchMetricExposureTest` 가 규칙 파일과 실제 노출을 잇는데, 지연 등록이면 그 테스트가
잡을 수 없다. 규칙이 쓰는 이름이 안 나오면 **그 알림은 영원히 안 울리는데 아무도 모른다.**

**원인 사슬을 따라간다.** Spring Batch 가 `BusinessException` 을 `UncategorizedSQLException`
같은 것으로 감싸는 자리가 있어, 맨 위만 보면 우리가 낸 코드가 전부 `UNCLASSIFIED` 로 샌다.

**006·007 은 뺀다.** `ExpireAdminController`(CY-429)의 거절 사유이지 잡 실패가 아니다 —
열거형의 클래스 주석이 *"잡이 낼 수 있는 코드를 순회하는 쪽(예: 지표 라벨)은 그 둘을 빼야
한다"* 고 명시했는데 처음에 `values()` 를 통째로 돌아 어겼다. 그 판정을 읽는 쪽이 손으로
하게 두면 매번 틀리므로 `ExpirationErrorCode.isJobFailure()` 로 옮겼다.

**라벨은 `getCode()` 하나뿐이다.** 메시지나 `detail` 은 안 넣는다 — 회차 id 가 섞여 시계열이
폭발하고, `detail` 은 로그용이라 PII 가 들어갈 수 있다(`PRD:2143`).

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

### 2f. ~~알림이 stdout 에만 남는다~~ **완료 · CY-651**

**이 절의 제목이 마지막까지 참이었다.** 규칙 서른여덟이 다 붙고 Alertmanager 가 축까지
갈라 라우팅하는데, 리시버가 `print()` 만 하는 스텁이라 **`docker compose logs alert-sink` 를
쳐야만 보였다.** 실측 — CY-651 을 시작할 때 이미 셋이 발화 중이었다:

```
[server] firing ExpireNeverSucceeded    severity=critical
[server] firing ExpireMetricsUnknown    severity=warning
[server] firing CleanupNeverSucceeded   severity=warning
```

`critical` 이 하나 떠 있는데 아무도 몰랐다.

`alert-sink.py` 가 받은 알림을 Slack 으로도 넘긴다.

| | |
|---|---|
| **발화만 보낸다** | `resolved` 까지 보내면 한 사건이 두 줄이 되고, 알림 서른여덟에서는 곧 아무도 안 읽는 채널이 된다 |
| **URL 이 없으면 stdout 만 한다** | 없다고 죽으면 리시버가 사라져 Alertmanager 가 실패하고, **그 실패를 알릴 경로도 같이 없어진다** |
| **보내다 실패해도 요청을 안 죽인다** | 200 을 이미 보낸 뒤라 Alertmanager 는 성공으로 알고, 같은 payload 의 **뒤 알림들이 통째로 유실된다** |
| **Slack 타임아웃 5초** | 이 리시버가 밀리면 그동안 들어온 알림이 전부 밀린다 |
| **실패하면 한 번만 다시 보낸다** | 우리가 이미 200 을 냈으므로 Alertmanager 가 재시도하지 않는다. 순간 실패를 여기서 안 흡수하면 **`repeat_interval`(1시간) 안에 해소되는 알림은 영영 한 번도 안 간다**(`resolved` 는 안 보내므로). 최악 지연은 5+1+5=11초 — **마지막 실패 뒤에는 안 기다린다**(기다리면 12초이고, 그 1초가 같은 payload 의 뒤 알림들을 건마다 민다) |
| **429·5xx 만 다시 보낸다** | 400·403·404 는 다시 보내도 **같은 답**이다 — 재시도가 지연만 늘리고 그 지연이 같은 payload 의 뒤 알림을 민다. 예외도 같은 기준이다: 연결 실패·타임아웃은 다시, 설정·직렬화 오류는 안 한다. **실측**: `200·403·404·204` 1회 / `429·500·503` 2회 / `ValueError` 1회 · `URLError` 2회 |
| **429 의 `Retry-After` 를 본다** | Slack 이 얼마를 기다리라고 말해 주는데 그걸 무시하고 고정 1초 뒤에 다시 치면 **재시도도 429 다** — 한 번뿐인 재시도를 헛되이 쓴다. 값이 3초를 넘으면 **포기한다**(그만큼 붙잡히면 그동안 들어온 알림이 전부 밀린다). 실측: 헤더 없음 → 1초 · `2` → 2초 · `30` → 포기 · 파싱 불가 → 1초 |
| **로그가 거짓이면 안 된다** | 마지막 시도의 429·5xx 에도 *"다시 보낸다"* 라 적으면 운영자가 **Slack 장애 중에 재전송을 기다린다.** 실제로 다시 보낼 때만 그렇게 적고, 마지막이면 *"마지막 시도였다"* 다 |
| **HTTP 상태 코드를 남긴다** | `urlopen` 은 4xx·5xx 를 **`HTTPError` 로 던지므로** 상태 코드 분기에 도달하지 않는다. 타입 이름만 남기면 **403(웹훅 폐기)과 500(Slack 장애)이 같은 줄**이 된다 — 전자는 웹훅을 다시 발급해야 하고 후자는 기다리면 된다. `e.code` 에는 URL 이 안 들어간다(실측) |
| **URL 을 로그에 안 싣는다** | 웹훅 URL 자체가 자격증명이다 |

**URL 은 환경변수로만 받는다.** 저장소에 안 적는다 — `db.env` 처럼 추적 안 되는 파일에 두고
compose 가 넘긴다(`.gitignore` 가 `*.env` 를 막는다, CY-621). 이 컨테이너는 호스트 포트
매핑이 없고 `read_only`·`cap_drop: [ALL]`·비루트(65534)로 돈다.

> **남은 것 — 웹훅 URL 을 컨테이너에 넣어야 한다.**
> **PR 리뷰 알림과 같은 웹훅을 쓴다**(결정). 채널을 나누면 볼 곳이 둘이 되고, 이 팀 규모에서는
> 그게 곧 한쪽을 안 보는 것이다.
>
> 다만 **값을 코드로 못 가져온다.** `SLACK_WEBHOOK_URL` 저장소 시크릿은 GitHub Actions
> 전용이고(`ai-review-slack.yml`), GitHub 은 시크릿 값 **조회 API 자체를 안 준다.**
> Slack 앱 설정(`api.slack.com/apps` → Incoming Webhooks)에서 다시 복사해야 한다.
>
> ```bash
> echo 'SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...' >> db.env
> set -a; . ./db.env; set +a
> docker compose -f base.yml up -d --force-recreate alert-sink
> docker logs cy-alert-sink-1 | head -1     # "slack=연결됨" 이 떠야 한다
> ```
>
> `.gitignore` 가 `*.env` 를 막는다. 그때까지는 URL 없이 stdout 만 하는 상태로 안전하게 돈다.
>
> **배선을 실측으로 닫았다**(2026-08-27). 기동 로그가 `slack=연결됨` 으로 바뀌고,
> `VerificationVerdictFailed`(critical) 를 흘렸더니 Slack 에 도착했다.
>
> **보내는 이름·아이콘은 payload 로 못 바꾼다.** `username`·`icon_url` 을 실어 봤는데
> 무시됐다 — 앱 이름(`yaho-batch`)으로 도착했다. **앱 기반 Incoming Webhook 은 그 둘을
> 아예 못 바꾼다** — 항상 앱의 것으로 나간다. 안 먹는 필드를 남기면 다음 사람이
> "왜 안 바뀌나" 를 다시 조사하므로 **뺐다.**
>
> ⚠️ 한때 여기 *"`chat:write.customize` 스코프가 없어서"* 라고 적었는데 **틀렸다.**
> 그 스코프는 **Web API**(`chat.postMessage`)용이고 Webhook 경로와 무관하다. 그 안내를
> 따라가면 운영자가 필요 없는 권한을 더하고 엉뚱한 곳을 조사한다.
> 이름은 `yaho-batch` 가 맞고, **아이콘은 앱 설정에서** 바꾼다
> (Basic Information > Display Information).

## 3. 만료 배치 — 실측으로 대가를 남긴 것들

수치는 전부 `docs/12` 에 있다. 여기는 **언제 손대는지**만 적는다.

| 무엇 | 언제 | 왜 그때인가 |
|---|---|---|
| `EXPIRE_BATCH` 의 `ORDER BY`·`LIMIT` → 상한 방식 | **취소·사용 API 티켓** | 후보 ≫ `LIMIT` 이면 후보 전부를 X 락한다(5,000건 실측). 막히는 것은 취소·사용뿐인데 그 경로가 아직 없다 |
| 표식 → `run_id` 컬럼 | **배치 다중화 직전(차단 조건)** | 인스턴스가 하나면 닿을 수 없다. 두 대가 되면 `APPEND_HISTORIES` 의 표식(`updated_at = :committedAt`)에 남의 행이 섞인다 |
| ~~청크 실행 시간 실측~~ **부분 완료 · CY-742** | ~~300만 건 적재 직후~~ | 300만 발급·620만 이력에서 **잡 전체가 9.64초 / 358청크**, 평균 27ms 다. 어떤 청크도 잡보다 길 수 없으므로 **락 보유 상한이 9.64초**이고 `innodb_lock_wait_timeout`(50초)의 **1/5 미만**이다 — "1205 로 실패한다" 쪽으로 안 넘어간다. ⚠️ **동시 부하 형상은 여전히 안 쟀다** — 위에서 물은 것은 후보 ≫ `LIMIT` 이면서 **취소·사용이 함께 도는** 모양인데, 그 경로가 아직 없어 재현할 대상이 없다 |
| 인덱스 둘의 쓰기 비용 | **세 번째 인덱스 얘기가 나올 때** | 지금 둘은 가용성과 5분 주기로 정당화됐고 쓰기 축은 본 적이 없다 |

---

## 4. 운영

| 무엇 | 언제 |
|---|---|
| ~~`BATCH_*` 정리~~ | **완료 · CY-436.** `cleanupJob` 의 `purgeBatchMetadataStep` 이 `batch.cleanup.metadata-keep-days`(기본 30, **최소 8 = 되읽기 창 7일 초과, 기동 거절**)로 걷는다. 딸린 행을 FK 역순으로, 고아 `JobInstance` 를 같은 트랜잭션에서 함께 지운다. `END_TIME` 이 비어 있는 행(시체)은 대상이 아니다 — 그 축은 §6 시체 절이 지고 CY-429 의 복구 API 가 닫는다. 청크는 `batch.cleanup.metadata-chunk-size`(기본 500, **잡 실행 수**, 1..5000)다. **삭제는 `IN` 목록이 아니라 id 하나씩** — `IN` 목록이 테이블 행 수 대비 커지면 옵티마이저가 풀스캔을 골라 대상이 아닌 행까지 잠근다. 그러면 양방향이 다 깨진다(실측): RR 에서는 메타 테이블 전체 + 갭 + supremum 에 X 락이 걸려 **다른 잡 기동과 도는 잡의 하트비트 커밋이 막히고**, `READ COMMITTED` 로 내려도 풀스캔은 **남이 잡은 행에서 대기**해 청크가 `ERROR 1205` 로 죽는다. id 하나씩이면 여섯 문장이 전부 `rows=1` 이라 RR·RC 양쪽에서 네 프로브가 다 통과한다 — **격리수준은 기본값 그대로**다. 대가는 5,000 실행 기준 680ms → 1,980ms(약 2.9배, 청크당 200ms 수준). 만료가 일 1회로 옮겨 인스턴스 순증은 하루 288 → **1** |
| 정리 Step 1 이 `expireJob` 을 안 본다 | **범위 밖 — 지킬 대상이 없다(CY-678 에서 판정).** 두 잡은 **테이블 교집합이 공집합**이다: 정리 Step 1 은 `asof_state`·`verification_findings`·`coupon_stats` 를 지우고, 만료의 쓰기는 `issuances`·`issuance_histories`·`coupon_stocks` 셋뿐이다(`ExpirationJdbcAdapter`). 프로브를 붙여도 물러나서 지킬 행이 없다. **실제 접촉면은 Step 2 의 `BATCH_*` 이고 그것은 CY-436 이 id 단건 삭제로 닫았다** — 이 항목은 틀린 Step 을 가리킨다. 그리고 겹치려면 만료가 1,200초 넘게 돌아야 하는데 `BatchJobRunningTooLong` 이 600초에서 이미 운다(실측 부하는 하루 약 6,300건). 반대로 물러나게 만들면 손해가 난다 — yield 에 상한이 없고, 겹치는 밤은 곧 `asof_state` 백로그가 큰 밤이라 `CleanupNotSucceeded` 가 매일 울면서 디스크만 자란다 |
| 버려진 실행 컷오프가 안 얼어 있다 | **닫혔다(CY-686).** Step 1 의 `abandonedBefore` 를 `cleanup.abandonedCutoff` 로 첫 호출 값에 얼렸다 — Step 2 의 `cleanup.metaCutoff` 와 같은 모양이다. 고정 시계 하네스로는 언 것과 안 언 것이 구분되지 않아(그래서 Step 2 의 분기는 지워도 초록이었다) **컷오프가 문맥에서 온다는 불변식 자체**를 잰다 — 두 Step 을 함께 못 박았다 |
| ~~회차 상태 전이 스케줄러~~ | **완료 · CY-446.** `CouponRoundScheduler` 가 1분마다 `open_at` 도달 회차를 열고 `close_at` 도달 회차를 닫는다. **Spring Batch 잡이 아니다** — 1분 주기로 배치 메타를 쓰면 하루 1,440 인스턴스가 되어 CY-436 이 정리한 축이 되살아난다. **대상을 고르고 id 하나씩 조건부 UPDATE** 로 바꾸고, **어댑터를 `READ COMMITTED` 로 연다**. ⚠️ **발급을 살리는 것은 격리수준이다** — 기본(`REPEATABLE READ`)에서는 이 테이블을 훑는 `UPDATE` 가 X 락 151(전부 + supremum)을 잡아 재고 소진 `CLOSED` 와 발급 전 `FOR SHARE` 가 둘 다 `ERROR 1205` 였고, RC 에서는 `X,REC_NOT_GAP` 10 만 잡고 둘 다 통과했다. **id 단건은 그것과 별개**이고 격리수준이 되돌아가는 날의 두 번째 겹이다(돌연변이 확인: RC 에서는 집합 `UPDATE` 로 되돌려도 락 테스트가 전부 초록이었다). `close_at` 은 갱신하지 않고(docs/02 F5) `coupon_stocks` 도 안 건드린다. 관측은 **결과 축**이다 — 게이지 **다섯**: 대기 넷(`cy_coupon_round_pending_open`·`_pending_close`·`_missed_window`·`_blocked_no_stock`)을 **한 문장으로** 되읽고(문장을 나누면 RC 에서 read view 가 갈려 회차가 어느 게이지에도 안 잡히거나 이중 계상된다), `_scheduling_enabled` 가 **끈 구간을 알림 갈래에서 빼는 축**이다(이름은 회차 전용처럼 보이지만 스위치가 하나라 만료·검증 알림 셋도 같은 축을 쓴다. 콜드 스타트와 구분이 안 되는 넷(`ExpireNeverSucceeded`·`CleanupNeverSucceeded`·`VerifyNeverSucceeded`·`ExpireMetricsUnknown`)만 사람이 silence 를 건다 — docs/14), 카운터 넷(`_ticks_total`·`_select_failures_total`·`_transition_failures_total`·`_refresh_failures_total`)이 진단을 진다. 알림은 **열**이고 데이터 축 셋(`BlockedByMissingStock`·`MissedWindow`·`DataMetricsUnknown`)은 `channel: data` 로 갈랐다 |
| ~~회차 생성 스케줄러~~ | **범위 밖 · CY-503.** 그때는 회차를 만드는 경로가 시드뿐이라 batch 가 그 축을 맡는 것이 자연스러웠는데, 지금은 **관리자 API 가 그 일을 한다**(`POST /api/v1/admin/coupon-templates/{id}/rounds` · CY-5). 배치가 매일 새벽에 하나 더 만들면 같은 테이블에 회차를 만드는 경로가 둘이 된다. 자리표시였던 `batch.schedule.coupon-create-cron` 을 걷었다 — 남겨 두면 다음 사람이 그것을 "하기로 되어 있는 일" 로 읽는다. **전이가 지는 전제는 그대로다** — 재고 행 없는 회차를 일부러 안 연다(발급 경로가 죽는다). 자동 생성이 필요해지면 그때 어느 쪽에 둘지 다시 정한다 |
| 기동 가드가 배치 메타 **인덱스**를 안 본다 | **닫혔다(CY-686).** `SchemaPresenceGuard` 에 셋째 축을 더했다 — `information_schema.statistics` 로 `IX_JOB_EXEC_STATUS_END`·`IX_JOB_EXEC_CREATE_TIME` 을 묻고 없으면 거절한다. 앞 둘과 달리 이 축은 없어도 기동과 동작이 통과해서 조용히 느려질 뿐이라 늦게, 원인을 안 가리키며 드러났다. `coupon_clean`·`coupon_corrupt`·`app` 셋 다 이미 인덱스가 있어 기존 환경은 안 깨진다(실측) |
| 업무 포트 노출 | ~~compose 티켓~~ ~~CY-359~~ **CY-368 에서 다시 정했다.** 그 포트에 인증 없는 admin 트리거가 열려 `batch.yml` 은 업무 포트를 **아예 안 내보낸다** — 필요할 때만 `batch-expose.yml` 을 얹어 `127.0.0.1:${BATCH_HOST_PORT:-9091}:9091` 으로 연다. 관리 포트(9092)는 어느 경우에도 안 올린다 |

---

## 4a. 검증용 셋에 Spring Batch 메타 테이블이 없다 (CY-359 가 발견)

`coupon_clean`·`coupon_corrupt` 는 cy-seed 의 `ddl/00_schema.sql` 로 만들어지는데
`CREATE TABLE` 17개 중 **`BATCH_*` 는 0개**다. cy-be 의 `V11__batch_metadata.sql`(과 인덱스 둘 `V2026082513`·`V2026082514`)은
Flyway 소유자인 `api` 만 돌리고, 검증용 셋은 그 Flyway 가 닿는 DB 가 아니다.

그런데 그 DB 를 보게 배치를 띄우는 것이 `application.yml.example` 이 문서화한 정상 절차다.
**데이터 테이블은 다 있고 메타만 없는 상태가 정상 절차에서 생긴다.** 그러면 기동은 통과하고
첫 잡 실행에서 `Table 'BATCH_JOB_INSTANCE' doesn't exist` 로 죽는다.

CY-359 는 `SchemaPresenceGuard` 로 그것을 **기동 시점에 드러내고 메시지로 조치를 가르는**
데까지만 했다. 남은 결정은 **누가 언제 붓는가** 다.

| 후보 | 대가 |
|---|---|
| 시드 생성 절차에 `V11__batch_metadata.sql` 을 넣는다 (cy-seed 쪽) | 시드 저장소가 cy-be 의 마이그레이션 파일을 알아야 한다 — 지금은 스키마 주인이 cy-be 라는 규율과 맞물려 사본 관리가 하나 더 는다 |
| compose 에 마이그레이션 원샷 서비스를 넣는다 | `api` 이미지를 `--spring.batch.job.enabled=false` 로 한 번 돌린다. `base.yml` 이 `api` 를 알아야 한다 |
| 문서화된 수동 절차로 둔다 (현재) | `docs/14` 시연 절차에 `V11`·`V2026082513`·`V2026082514` 주입 명령을 박아 뒀다. **테이블·컬럼·인덱스 셋을 다 가드가 잡는다**(CY-686). 인덱스는 선두 컬럼까지 대조하고, 그 축만 `batch.schema-guard.require-batch-indexes=false` 로 끌 수 있다 |

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

## 4c. ~~전체 빌드가 5분이다~~ **97초였고, 76초가 됐다** (CY-368 실측 → CY-661 정정)

> ⚠️ **"5분" 은 낡은 수치다.** CY-368 이 쟀을 때는 맞았는데 **CY-621 이 그 대부분을
> 없앴다**(컨테이너를 스프링 컨텍스트가 아니라 JVM 이 소유하게 함 — §7). CY-661 이
> 다시 재니 **97초**였다. 이 절을 근거로 "5분을 줄인다" 는 티켓을 세우지 마라.

**남은 것은 거의 전부 테스트 본체다.** 실측(M-series 11코어 · Docker VM 7.65GB, 2026-08-27):

| | 전 | 후 |
|---|---|---|
| 전체 `clean build` | 97초 | **73~76초** |
| 변경 없는 재빌드 | 6초 | **2~3초** |

**테스트를 뺀 빌드는 8초다.** 즉 76초 중 68초가 테스트이고, 그 안은 Testcontainers MySQL
기동과 스프링 컨텍스트 로딩이다. **여기서 더 줄이려면 그 둘을 건드려야 하고, 아래 두 줄이
그 후보다.**

| 무엇 | 지금 | 왜 |
|---|---|---|
| ~~`gradle.properties`~~ | **완료 · CY-661.** `caching`·`configuration-cache` **둘**을 켰다. `parallel` 은 실측에서 효과가 없어 뺐다(아래) |
| Testcontainers `withReuse` | 안 켬 | 클래스마다 컨테이너를 새로 띄운다 |
| Spring 컨텍스트 | 배치만 6벌 | `@SpringBootTest` 의 `properties` 조합이 다르면 캐시가 안 걸린다 |

**이 문단이 맞았다 — 병렬은 효과가 없다.** CY-661 이 한 번 *"94초 → 68초(−28%)"* 로 뒤집었다가
다시 재서 되돌렸다. **한 번의 측정으로 축 셋을 동시에 켜 놓고 그 차이를 병렬에 귀속시킨 것이
잘못이었다.** 축을 하나만 움직여 세 회차를 쟀다(`clean test --rerun-tasks`, 매 회차 빌드
캐시를 지워 콜드로 맞춤):

```
parallel=false   72 · 69 · 71초   평균 70.7
parallel=true    76 · 74 · 71초   평균 73.7
```

**빠르지 않고 오히려 느리다.** 이 문단이 적은 이유 그대로다 — `core → storage → batch` 가
의존으로 줄 서 있고 `batch` 가 `storage` 의 `testFixtures` 를 쓴다.

**대가는 실재했다.** 병렬을 켜면 모듈이 겹쳐 돌아 동시 mysql 컨테이너가 **3개 → 5개**가 된다.
CY-392 를 깨뜨린 18개(약 8GB)와는 멀지만 **얻는 것이 없는데 치를 이유가 없다.** 그래서 뺐다.
CI(ubuntu-latest)는 컨테이너 전용 VM 없이 7GB 를 JVM 과 나눠 쓰므로 그쪽 마진은 로컬보다
좁고, **거기서는 아예 안 쟀다.**

**테스트 병렬(`maxParallelForks`)은 애초에 후보가 아니다.** `build.gradle` 이 경고한 대로
**JVM 수만큼 컨테이너가 곱해진다.**

**캐시가 문서 검사를 건너뛰던 구멍도 함께 막았다.** `AlertChannelRegistryTest` 와
`BatchMetricExposureTest` 가 `Path.of("..")` 로 모듈 밖 파일(규칙 파일·`docs/14`)을 읽는데
태스크 입력에 없었다 — **그 둘만 바뀐 라운드는 `:batch:test` 가 UP-TO-DATE 로 건너뛰었다**
(실측). 캐시를 켜기 전부터 있던 구멍이지만(up-to-date 검사와 캐시는 별개다) 캐시가 그 판정을
CI 러너 사이로도 옮기므로 여기서 막았다 — `batch/build.gradle` 의 `inputs.files(...)`.

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
curl -s localhost:9091/api/v1/admin/expire/runs/stuck | jq .data
#   [{ "executionId": 41, "status": "STARTED", "createTime": "...", "startTime": "...",
#      "lastProgress": "...", "stalledSeconds": 7412 }]

# ② 한 번이면 된다. 재시도해도 안전하다(FAILED + END_TIME 으로 판정한다).
curl -s -XPOST localhost:9091/api/v1/admin/expire/runs/41/recover | jq '.data, .error'
#   409 / EXPIRATION-007 이면 걷어낼 대상이 아니다 — ① 을 다시 본다.
#   404 / EXPIRATION-006 이면 만료 실행이 아니거나 없는 번호다.
```

`recover` 는 돌던 Step 까지 `FAILED` 로 닫는다. **`ABANDONED` 로 만들지 않는 것이 계약이다** —
그 상태는 `COMPLETED` 와 같은 취급이라 그 `JobInstance` 를 같은 `asOf` 로 영원히 못 돌린다.

업무 포트가 안 열려 있으면 `batch-expose.yml` 을 얹는다(§4). `verifyJob` 쪽은
`/api/v1/admin/verify` 에 `stop → abandon` 이 있다 — **그쪽 `stop` 은 시체 판정을 안 지난다**
(그것이 남은 항목이다, 위 절). **부르기 전에 프로세스 부재를 사람이 확인해야 한다** —
`stop` 은 살아 있는 실행에도 먹고, DB 를 즉시 `STOPPED` 로 올린다. 그 뒤 `abandon` 이
스레드보다 먼저 도착하면 **도는 검증이 `ABANDONED` 로 굳는다.** `cleanupJob` 도 `POST /api/v1/admin/cleanup/runs/{id}/recover` 가 있다(CY-697). **손 SQL 은 배치가 안 떠 있을 때만이다.**

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
   `cleanupJob` 도 API 가 있다(CY-697) — 아래 SQL 은 배치가 안 떠 있을 때만이다.
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

## 7. ~~테스트 컨테이너를 스프링 컨텍스트마다 띄우고 있다~~ 해결됨 (CY-621)

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

**당시의 대응은 `spring.test.context.cache.maxSize=4` 였다**(루트 `build.gradle`).
밀려난 컨텍스트가 닫히면 그 컨테이너도 내려간다 — 18개가 4개가 되는 것을 확인했다.
대가는 재생성 비용이었고, 빌드가 5분대였다. **CY-621 이 그 상한을 기본값 32로 되돌렸다** —
아래 블록 참고.

**근본 해결은 컨테이너를 컨텍스트마다 안 띄우는 것이다.** 다만 단순 싱글턴이 안 된다 —
이 저장소는 **정상 스키마와 오염 스키마를 나눠** 써야 하고(`CorruptRepositoryTest` 가 그것을
명시한다), 락 측정 테스트는 `performance_schema` 를 요구한다. 그래서 **스키마 종류별로 갈린
JVM 싱글턴**이 필요하다.

> ### ✅ CY-621 이 했다
>
> **컨테이너를 컨텍스트가 아니라 JVM 이 소유한다.** `MySqlContainerConfig` 가 `static` 싱글턴을
> 들고, `stop()` 을 받지 않는 서브클래스로 스프링이 수명에 손대지 못하게 한다. 컨텍스트가
> 밀려나도 mysqld 는 그대로 산다.
>
> | | 전 | 후 |
> |---|---|---|
> | `:batch:test` 컨테이너 기동 | **44회** | **4회** |
> | 동시에 살아 있는 컨테이너 | **18개** (≈8GB) | **3개** |
> | `:batch:test` | 294초 | **59초** |
> | `./gradlew build` (clean) | 약 300초 | **97초** |
>
> 4회의 내역: CLEAN 1 + CORRUPT 1 + `BinlogFormatGuardTest` 가 직접 띄우는 STATEMENT·ROW 둘.
>
> **위가 예고한 "스키마 종류별로 갈린 JVM 싱글턴" 이 맞았다.** 같은 컨테이너의 다른
> 데이터베이스로 가르는 방법을 먼저 해 봤는데 안 된다 — `@ServiceConnection` 이 컨테이너에서
> 읽은 접속 정보가 **인라인 프로퍼티를 이겨서**, CORRUPT 마이그레이션이 CLEAN DB 에 떨어졌다
> (`CleanSchemaGuard` 가 13건 울었다).
>
> **`maxSize` 를 4에서 기본값 32로 되돌렸다.** 그 값의 근거였던 메모리 폭발이 사라졌다.
>
> **대가는 격리다.** 컨텍스트가 곧 빈 DB 이던 성질이 없어져, 데이터를 읽는 테스트는 스스로
> 비워야 한다.
>
> **`removeJobExecutions()` 만으로는 안 비워진다** — 실행이 없는 인스턴스를 남긴다(바이트코드로
> 확인). 그래서 `BatchMetadata` 를 만들어 `BATCH_*` 여섯을 FK 역순으로 지운다.
>
> ⚠️ **처음에는 그것을 두 클래스에만 붙이고, 나머지를 이렇게 면제했다 — 그 근거가 거짓이었다.**
>
> > *"나머지는 이미 `VerificationSeed.clear()` 로 비우고 있었거나, 배선·설정 검사라 행을 안 읽는다."*
>
> **그 픽스처의 `TABLES_IN_DELETE_ORDER` 에는 `BATCH_*` 가 하나도 없다.** 실제 방어선은
> `removeJobExecutions()` 하나였고, 그것이 못 지우는 바로 그 행이 문제였다. 잡을 돌리는 배치
> 테스트를 전수로 세어 보니 **29개 중 25개가 같은 `asOf`(2026-01-15T09:00)** 를 쓴다 —
> `BinlogFormatGuardWiringTest` 가 그렇게 깨졌고 같은 조건이 24개 더 있었다.
>
> ```bash
> grep -rl "jobOperator\|launch(" --include='*.java' batch/src/test | wc -l   # 29
> ```
>
> **지금은 `VerificationSeed.clear()` 안에서 함께 부른다.** 이미 데이터를 비우는 자리라
> 부르는 쪽이 늘 필요가 없고, 강도가 한 벌로 통일된다. 두 벌이 병존하면
> *"왜 저기만 지우지"* 를 다음 사람이 판단해야 하고, 판단이 틀리면 순서 의존 초록이 다시 생긴다.
>
> ### ⚠️ 그리고 이 변경이 blocker 를 하나 만들었다 — 머지 뒤에 잡혔다
>
> `stop()` 을 **무조건** 비웠는데, 그것은 스프링만 부르는 게 아니다 —
> **Testcontainers 가 기동 실패를 치울 때도 부른다.** 바이트코드로 확인했다:
>
> | | |
> |---|---|
> | `GenericContainer.start()` | 첫 줄이 `if (containerId != null) return` |
> | `GenericContainer.stop()` | `containerId`·`containerInfo` 를 `null` 로 되돌리는 **유일한 자리** |
> | `GenericContainer.tryStart()` | 기동 실패 시 `stop()` 을 부른 뒤 `ContainerLaunchException` |
>
> 그래서 CI 에서 첫 기동이 실패하면(이미지 pull 타임아웃 · 메모리 부족 · 포트 고갈)
> **죽은 컨테이너 id 가 고정되고, 이후 모든 컨텍스트가 그것을 받는다.** 실패 수백 건에
> 원인 스택은 첫 하나뿐이다 — 위에서 정적 초기화자를 피한 **바로 그 실패 모양**을
> 뒷문으로 되살린 것이다. 컨텍스트마다 컨테이너를 새로 만들던 시절에는 다음 컨텍스트가
> 새 객체로 회복했는데, 공유가 그 경로를 없앴다.
>
> **지금은 `doStart()` 가 반환한 뒤에만 막는다.** `tryStart` 의 정리 `stop()` 은 그 반환
> **전에** 불리므로 그 시점 `startedOnce` 가 false 다. `close()` 오버라이드는 지웠다 —
> `Startable.close()` 가 `stop()` 한 줄이라 조건을 그대로 타는데, 따로 비우면
> **조건을 건너뛰어** 정리가 다시 죽는다.
>
> **가드도 표기법 하나에만 성립했다.** `spring.flyway.locations` 를 평문으로 읽었는데
> 그 프로퍼티는 `List<String>` 이라 인덱스 표기(`[0]=…`)로도 바인딩되고, 그때
> `getProperty` 가 `null` 을 줘 **CLEAN 가드가 뚫린다.** `Binder` 로 바꾸고 판정을
> `CorruptSchema.isCorrupt()` 한 곳에 모았다 — 두 가드가 **같은 함수의 부정**을 쓴다.
>
> **이 blocker 를 CodeRabbit 은 세 라운드 동안 못 잡았고, 로컬 코어 리뷰가 바이트코드를
> 읽어 잡았다.** 봇 하나로는 이 깊이가 안 나온다는 기록으로 남긴다.
>
> **제약이 메모리에서 연결 수로 옮겨 갔다.** 캐시된 컨텍스트가 각자 풀을 들고 한 서버에
> 붙어 `Too many connections` 가 282번 났다. `--max-connections` 를 **`build.gradle` 이 캐시
> 상한에서 계산해** 넘긴다 — 두 값이 서로를 모르면, 한쪽만 올렸을 때 나오는 실패가 Hikari
> 문제로 오독된다.
>
> **규약을 기계가 막는다.** CORRUPT 는 `@Import` 와 `CorruptSchema.FLYWAY_LOCATIONS` 를 **둘 다**
> 줘야 하는데, 하나만 주면 *"검출 0건"* 이라는 **초록**으로 실패했다. 지금은 두 설정이 각자
> 기동 시점에 로케이션을 보고 죽인다 — 조건이 서로의 부정이라 둘을 함께 임포트하는 것도 막힌다.
> `SharedMySqlContainerTest` 가 컨테이너 정체성 둘(수명·분리)을 따로 잰다.

---

## 이 문서를 쓰는 법

티켓으로 옮길 때 **"언제"** 를 그대로 선행 조건에 적는다. 조건이 안 열린 항목을 지금 하면
검증할 방법이 없는 채로 코드만 늘어난다 — 3번의 앞 두 줄이 정확히 그 상태다.
