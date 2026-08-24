# 배치 서버 구현 방향

> 무엇을 어떻게 만드는가. 왜 그렇게 정했는지의 배경은 `10-batch-design.md`,
> 시드와 맞춰야 하는 계약은 시드 저장소의 `contract.json` 이 원본이다.

---

## 1. 배치는 별도 프로세스다

`settings.gradle` 이 이미 그렇게 잡혀 있다. `batch` 가 독립 Gradle 모듈이고
`BatchApplication` 이 별도 부트 앱이며 `spring-boot-starter-batch` 는 여기에만 붙는다.

```
api     ①②③ 런타임 · 부하 측정 대상
batch   ④    별도 JAR · 별도 프로세스
core    도메인 (상태머신 공유)
storage JPA · 어댑터 · Flyway
```

프로세스가 갈리므로 **Hikari 풀이 자동으로 분리**된다. `HikariCP` 사용률이 v1 병목의 증거인데
배치가 나눠 쓰면 그 증거가 흐려진다 — 이 분리가 그것을 구조적으로 막는다.

### 스케줄러는 전부 batch 에 있고, 부하 중에는 하나도 돌지 않는다

회차 생성과 상태 전이까지 batch 가 가져간다. 상태를 바꾸는 배치가 한 프로세스에 모여야
정지 스위치가 하나로 끝나기 때문이다.

| 어디에 | 무엇이 | 부하 중 |
|---|---|---|
| `api` | 발급 · 사용 · 취소 · 대기열 · 드리프트 감시 | 유지 |
| `api` | **재고 소진 시 회차를 `CLOSED` 로** — 발급 경로가 인라인으로 | 유지 |
| `batch` | 회차 생성 · 회차 상태 전이 스케줄러 | **정지** |
| `batch` | `expireJob` · `verifyJob` | **정지** |

**부하 중에 스케줄러가 하나도 안 돌아도 되는 이유** — 부하 테스트는 이미 `OPEN` 인 회차 하나에
트래픽을 몰아넣는 것이다. 테스트 시작 전에 대상 회차를 `OPEN` 으로 준비해 두면 되고,
테스트 도중에 새 회차가 열릴 일은 없다.

재고 소진으로 `CLOSED` 가 되는 것만 부하 중에 실제로 일어나는데, 이건 **발급 경로가 그 자리에서**
바꾼다. 스케줄러를 기다리면 재고가 0인데 `OPEN` 인 구간이 생긴다.

그래서 **부하 중 정지 수단이 설정이 아니라 컨테이너다.**

```bash
docker compose -f base.yml up                     # batch 를 아예 안 띄운다
docker compose -f base.yml -f batch.yml up batch  # 부하 종료 후 겹쳐 올린다
```

`base.yml` 은 한 글자도 안 바뀌므로 비교표의 *"동일 Docker Compose 리소스 limit"* 이 유지된다.

### 판정 시점에는 스위치 하나로 멈춘다

`batch` 를 띄운 채로 `FULL` 을 두 번 돌려야 하는데, 그때 같이 뜬 스케줄러가 회차를 열거나
만료를 돌리면 **회차 하나만 `CLOSED` 로 바뀌어도** `sum(coupon_stocks.active_count)` 나
`max(issuances.updated_at)` 이 움직여 지문이 달라지고 결정론 증명이 실패한다.

```yaml
batch.scheduling.enabled: false   # 전 스케줄러를 @ConditionalOnProperty 로 이 하나에 묶는다
```

스케줄러가 전부 batch 에 있으므로 끌 것이 하나다. 두 앱에 흩어져 있으면 하나는 반드시 빠뜨린다.

---

## 2. 검증·리플레이는 JPA 를 쓰지 않는다

대용량 검증·리플레이 경로는 JPA 영속성 컨텍스트를 사용하지 않고 JDBC reader/writer 로 처리한다.
회차 생성·상태 전이·만료 도메인 경로는 공용 storage JPA 어댑터를 사용한다.

**근거.** 검증 규칙은 전부 집계 SQL 이고 리플레이는 이력 순회다 — 거기엔 **JPA 가 할 일이 없다.**
300만~534만 행에 영속성 컨텍스트를 얹지 않는다는 것이 이 장의 결정이고, 그건 그대로다.

**바뀐 것.** 예전에는 "배치가 JPA 를 아예 안 쓴다" 였고 근거는 저장소에 엔티티가 0개라는
사실이었다. CY-245 계보가 들어오면서 그 전제가 사라졌다 — 만료 스케줄러가 도메인 포트를 타고
`IssuanceRepository` · `IssuanceHistoryRepository` · `CouponStockRepository` 를 부르는데 셋 다
storage 의 JPA 구현이다. 그 세 곳의 잠금·조건부 갱신·예외 변환을 JDBC 로 다시 만드는 것보다,
경계를 **경로별로** 긋는 것이 작고 일관된다.

`batch → storage` 는 여전히 `runtimeOnly` 다. batch 본 코드는 Entity·JpaRepository 타입을
컴파일 타임에 못 보고, 도메인 포트로만 부른다 — 그 경계는 그대로 살아 있다.

**⚠️ 반쪽만 켜지 않는다.** `spring.autoconfigure.exclude` 의 JPA 두 줄과
`storage.jpa.auditing.enabled` 는 한 쌍이다. storage 의 `@EnableJpaRepositories` 는 자동설정이
아니라 `exclude` 로 막히지 않으므로, 자동설정만 빼면 리포지토리는 만들어지는데
`EntityManagerFactory` 가 없어 기동이 죽는다. 반대로 auditing 만 끄면 기동은 되고 쓰기 시점에
`created_at` 이 비어 실패한다 — 증상이 서로 다른 자리에서 나온다.
`DomainGaugeConfigContractTest` 가 이 쌍을 지킨다.

```
V1 V2 V6      tasklet + 단일 SQL
V3 V5         tasklet + 단일 SQL (asof_state 조인)
V4 · Step 0   JdbcPagingItemReader (keyset) → Processor → JdbcBatchItemWriter
시드 적재      JdbcBatchItemWriter + rewriteBatchedStatements=true
```

검증 전용 엔티티와 어댑터는 만들지 않는다. 위 표의 경로는 전부 JDBC 다.

---

## 3. Spring Batch 를 쓸 조건

Spring Batch 는 공짜가 아니다. Job 하나마다 `BATCH_JOB_INSTANCE` · `EXECUTION` ·
`STEP_EXECUTION` · `EXECUTION_CONTEXT` 에 쓰기가 생긴다.

```
1  청크 재시작이 필요한가        300만 스캔 중 죽었을 때 이어서 돌아야 하나
2  실행 이력이 판정 근거인가      BATCH_STEP_EXECUTION duration 을 증거로 쓰나
3  파라미터 재실행을 증명하나     같은 asOf 두 번 → attempt
```

하나라도 해당하면 Spring Batch, 아니면 `@Scheduled`.
템플릿 12행을 스캔하는 회차 생성을 Batch 로 만들면 **배치 메타 쓰기가 검증 대상 DB 를 때린다.**

`@EnableBatchProcessing` 은 붙이지 않는다 — 붙이면 `BatchAutoConfiguration` 이 물러나
`JobRepository` · `JobLauncher` 를 직접 정의해야 한다.

메타 테이블은 `V11__batch_metadata.sql` 이 만든다. `spring-batch-core` 6.0.4 원본 그대로이고,
`spring.batch.jdbc.initialize-schema: never` 라 이 파일이 없으면 `JobRepository` 초기화가 즉시 실패한다.

---

## 4. 배치 3계층 — 쓰기 대상으로 가른다

주기로 나누면 `expireJob`(재고를 쓴다)과 `verifyJob(INCREMENTAL)`(읽기만 한다)이 같은 칸에 들어간다.
위험도가 정반대인데도.

```
계층 1 · 상태를 바꾼다
  회차 생성            @Scheduled    매일 새벽   total_quantity 만 세팅
  회차 상태 전이        @Scheduled    분 단위     SCHEDULED→OPEN→CLOSED
  expireJob           Spring Batch  5분        ★ 재고를 쓰는 유일한 잡

계층 2 · 진실을 판정한다   원본은 읽기만 한다
  verifyJob           Spring Batch  FULL 온디맨드 / INCREMENTAL 10분
    └ statsAggregate                Step 7. CLEAN 만
  reportDump          관리 API      최종 1회

계층 3 · 지운다
  cleanupJob          @Scheduled    1시간   멱등 · 토큰 · 스냅샷 · asof_state
  dltReprocessJob     수동          ③ Kafka 계약 대기

일회성
  seedLoadJob · corruptInjectJob    Spring Batch

계층 밖 · 관측
  드리프트 감시         @Scheduled    1초   아무것도 쓰지 않는다
```

**계층 1만 불변식을 깰 수 있고 셋뿐이다.** 동시성 테스트도 부하 중 정지도 락 순서도 전부 여기서만 필요하다.
**계층 2는 아무리 느려도 아무것도 안 깬다** — 그래서 시간 상한을 두지 않는 결정이 성립한다.
**계층 3은 통째로 컷 가능하다.**

### 못 박는 규칙 셋

1. **재고(`active_count`)를 쓰는 배치는 `expireJob` 하나.** 회차 생성은 `total_quantity` 만 채운다.
   이게 무너지면 경합 축이 하나에서 셋으로 늘고 동시성 조합이 폭발한다.
2. **계층 2는 원본 테이블에 쓰지 않는다.** 검증기가 데이터를 고치면 다음 실행이 무엇을 검증하는지 알 수 없어진다.
3. **`asof_state` 를 읽는 규칙은 전부 한 잡에.** 나누면 Step 0(실측 57초)를 잡마다 다시 돌린다.
   규칙 6개 중 4개가 이것을 읽으므로 검증은 사실상 하나로 묶인다.

---

## 5. 준실시간은 새 파이프라인이 아니다

```
실시간 1초      드리프트 감시          Redis ↔ DB 합계 비교. 유형을 식별하지 못한다
준실시간 10분   verifyJob INCREMENTAL  절대 구간 (from_ts, as_of]. 유형까지 식별
온디맨드        verifyJob FULL         전수. 합격 판정은 여기서만
```

**같은 Job 의 `scope` 파라미터 차이**일 뿐 규칙도 코드도 하나다.
잡을 둘로 나누면 규칙 구현이 두 벌이 되어, 두 결과가 갈릴 때 진실을 판단할 근거가 사라진다.

증분은 판정하지 않는다. 집계 규칙(V1·V2)은 윈도우 안의 *행*이 아니라
**윈도우에 등장한 키의 전체 이력**을 스캔해야 한다 — 행 윈도우로 자르면 경계 밖 짝을 놓친다.

---

## 6. 모듈 구조

```
batch/src/main/java/com/kafkick/batch/
  job/        계층 2 — Spring Batch 인 것만
  schedule/   계층 1·3 — @Scheduled (안에서 Job 을 실행한다)
  rule/       VerificationRule 과 V1~V6
  replay/     이력 접기 · aggregating Reader
  seed/       생성기 · PII 암호화 · 분포
  corrupt/    유형별 주입 · expected_findings
  support/    지문 · 체크섬
  api/        내부 업무 포트 verify 트리거

core/src/main/java/com/kafkick/core/
  coupon/       IssuanceStatus · IssuanceEventType · CouponStatus · CouponStateMachine
  verification/ FindingType · TargetKey · ScopeType · DatasetType · VerdictType · StatsStatus
```

`job/` 과 `schedule/` 을 가르면 *"이건 Batch 인가 Scheduled 인가"* 를 폴더가 답한다.
한 폴더에 몰면 다음 사람이 `cleanupJob` 도 Spring Batch 로 만든다.

`CouponStateMachine` 이 `core/coupon` 에 있는 이유는 **런타임도 같은 클래스를 써야 하기 때문**이다.
검증 전용 패키지에 두면 두 벌로 갈라져 같은 버그를 양쪽이 재현한다.

---

## 7. 글자 단위로 맞춰야 하는 계약

한쪽만 바뀌면 검출은 정상인데 판정이 전부 뒤집힌다.

```
target_key
  V1        COUPON:{coupons.id}
  V2        COUPON:{coupons.id}|MEMBER:{members.id}
  V3 V5 V6  ISSUANCE:{issuances.id}
  V4        HISTORY:{issuance_histories.id}

  집합 비교 · UNIQUE · checksum 은 전부 이 문자열로만 한다.
  다형 FK 컬럼으로 조인하면 NULL = NULL 이 UNKNOWN 이라
  정확히 검출한 finding 이 전부 "누락"으로 잡힌다.

리플레이
  상태      created_at <= asOf 이력을 (created_at, id) 오름차순 정렬한 마지막 to_status
  활성 사용  used_at <= asOf AND (canceled_at IS NULL OR canceled_at > asOf)
  from_status 를 믿지 않는다. 추적 상태가 진실이고 다르면 그 자체가 finding 이다.
  불법 전이를 만나면 기록하고 계속한다 — 중단하면 나머지 이력이 검증되지 않는다.

findings_checksum
  정렬된 (finding_type, target_key) 만.
  finding_type + U+001F + target_key + U+001E 반복 후 SHA-256.
  expected/actual 같은 자유 문자열을 섞으면 포맷 한 글자에 거짓 실패가 난다.

dataset_fingerprint
  SHA256( max(issuance_histories.id) | count(issuance_histories) | count(issuances)
          | sum(coupon_stocks.active_count) | max(issuances.updated_at) )
  구분자 "|"   타임스탬프 "%Y-%m-%d %H:%M:%S.%f"   이력 필터 created_at <= asOf

오염
  주입 700건 · 정답 800행. 유형 3이 V1 과 V4 를 동시에 울린다.
  규칙별 기대  V1 200 · V2 200 · V3 100 · V4 200 · V5 100 · V6 0

asof_state
  PK (run_id, coupon_id) — run 마다 재생성한다. 컬럼명은 state 이지 status 가 아니다.
  FK run_id → verification_runs.id 이므로 run 행을 먼저 INSERT 해야 한다.
  active_usage_count 를 Step 0 가 같이 채워 V5 가 실행 시점에 usages 를 조인하지 않는다.
```

`attempt` 를 JobParameters 의 식별 파라미터에 넣지 않으면 같은 `asOf` 재실행이 차단되어
**결정론 증명 자체가 불가능**해진다. `uk_run_params(as_of, dataset, scope, attempt)` 도 같은 방향이다.

---

## 8. 검증 규칙은 6종이다

| 규칙 | finding_type | 그레인 | 결정론 |
|---|---|---|---|
| V1 | `STOCK_MISMATCH` | 회차 | 현재 행 |
| V2 | `DUP_PER_MEMBER` | (회차, 회원) | ✅ |
| V3 | `REPLAY_MISMATCH` | 발급건 | 현재 행 |
| V4 | `ILLEGAL_TRANSITION` | 이력 행 | ✅ |
| V5 | `USAGE_MISMATCH` | 발급건 | ✅ |
| V6 | `GRADE_VIOLATION` | 발급건 | 스냅샷 |

**`V7` 같은 규칙을 새로 만들지 않는다.** 발급코드 중복은 `V2` 의 두 번째 케이스이고
(`GROUP BY coupon_id, code`, `MIN(id)` 제외), 고아 이력은 `V4` 가 전이 연쇄로 잡는다.
별도 규칙을 만들면 같은 행이 두 규칙에 잡혀 집합 비교가 어긋난다.

**`V6` 는 `issuances.issued_grade` 스냅샷을 조인한다.** `members.membership_grade` 는 현재값이라
시드가 일부러 심어 둔 *"현재는 부적격 · 스냅샷은 적격"* 3% 가 통째로 오탐이 된다.

Step 순서는 결정론 규칙이 먼저다 — 폭주로 중단돼도 결정론적 부분은 이미 확보된다.

```
Step 0 asof_state 생성
────── 완전 결정론 ──────
Step 1 V4   Step 2 V2   Step 3 V5
────── 현재 행을 읽음 ──────
Step 4 V3   Step 5 V1   Step 6 V6
Step 7 통계(CLEAN 만)   Step 8 finalize
```

`V2` 가 결정론 구간에 있는 이유 — `asof_state` 에 `member_id` 가 없어 `issuances` 를 읽지만,
**세는 대상이 "행의 존재"와 `code` 라 둘 다 변하지 않는다.**

---

## 9. 검증하지 않는 것

여기 있는 것을 규칙으로 추가하면 **정상셋 0건이 원천적으로 불가능해진다.**

| 대상 | 왜 |
|---|---|
| `stock_per_occurrence` ↔ `total_quantity` | 회차별 재고 조정이 정상. 과거 회차 144개가 전부 걸린다 |
| 만료 누락 (`expires_at < asOf` 인데 `ISSUED`) | 리플레이 결과도 `ISSUED` 라 자동 일치. 지연은 배치 주기의 함수다 |
| 고아 이력 | `V4` 가 잡는다. 별도 규칙은 이중 검출 |
| `close_at` 미갱신 | 갱신하면 *언제 닫힐 예정이었나* 가 소실된다 |
| CLOSED 회차의 잔여재고 증가 | `active_count` 는 누적이 아니다 |
| 스냅샷 컬럼 전부 (`issued_grade` 포함) | 시점 고정이라 불일치가 곧 정상이다 |

---

## 10. 테스트

`PRD:1108` — *"`교과 내용 반영 수준`이 기술성 30점의 명시 세부항목이고 **테스트 전략 자체가 득점 요소**"*.
그리고 배치는 **부하 테스트로 커버되지 않는 유일한 영역**이다.

| 종류 | 대상 | 컷 |
|---|---|---|
| 규칙 단위 | 6종 각각. 오염 1건 심고 **정확히 1건** 잡는지 | 🔴 컷 불가 |
| 상태머신 | 전이표 전수. 종단 상태·역방향 전이 | 🔴 컷 불가 |
| 계약 포맷 | 지문·체크섬·`target_key` 가 **바이트 단위로** 일치하는지 | 🔴 컷 불가 |
| 동시성 | 만료 × 취소 → 재고 1회만 복원 | 🔴 컷 불가 |
| 교차 검증 | 같은 스키마에 독립 구현 두 개를 돌려 집합 비교 | 권장 |
| 통합 | 축소 시드 → 오염 → verify → `expected` 집합 일치 왕복 | 권장 |

**규칙 단위가 컷 불가인 이유** — `V6` 는 오염 유형이 없고 `V1`·`V2` 는 두 유형이 겹쳐 들어온다.
규칙 하나가 맞는지 확인하는 유일한 수단이다.

기반은 이미 있다 — `@RepositoryTest`(Testcontainers MySQL + Flyway)와 `MySqlContainerConfig` 가
`testFixtures` 로 배선돼 있고, 배치가 그 첫 사용자다.

> 테스트 컨테이너 이미지가 `mysql:latest` 다. 코드 주석이 스스로
> *"커밋이 그대로여도 테스트 결과가 달라진다"* 고 경고한다. 검증 결정론이 계약인 프로젝트이므로
> 태그 고정이 필요하다.

---

## 11. 인증은 헤더로 사용자를 구분한다

**회원가입·로그인은 과제 범위 밖이다.** 가상 회원 100만 명 중 지금 누가 요청하는지를 가려야 하므로
**회원과 권한을 요청 헤더로 받는다.** 인증 체계가 아니라 사용자 구분 수단이다.

서명이 없으므로 클라이언트가 무엇이든 주장할 수 있다. 네트워크 경계는 다음처럼 나눈다.

| 무엇 | 어떻게 |
|---|---|
| 관리 화면 `/api/v1/admin/**` | 브라우저는 API 8080만 호출하고, API가 Batch 9091을 내부 호출한다 |
| Actuator | API 9090·Batch 9092를 호스트에 노출하지 않고 Prometheus만 내부 접근한다 |
| 사용자 경로 | 서버가 헤더 등급을 회차의 `eligible_grades_mask` 와 **대조**한다 |

서명 없는 역할 클레임(`hasRole`)은 방어가 아니라 장식이므로 넣지 않는다.
JWT · 세션 · Spring Security 도 도입하지 않는다.
`X-User-Role: ADMIN` 문자열 검사도 호출자를 인증하지 않으므로 관리자 API의 보안 방어선으로
간주하지 않는다. 실제 관리자 인증 또는 신뢰된 게이트웨이 경계는 후속 작업이다.

> TODO(CY-209 배포 확인): 이 저장소에는 Compose가 없다. 배포 저장소에서
> API 9090·Batch 9091·9092가 호스트에 매핑되지 않음을 확인하기 전에는 이 경계가
> 보장된 것으로 간주하지 않는다. 브라우저가 Batch 9091을 직접 호출하도록
> 라우팅해도 이 경계는 무효다.

> 앱이 헤더 등급을 대조하지 않으면 부적격 등급이 발급되고 그 값이 `issuances.issued_grade`
> 스냅샷에 그대로 박힌다. **검증 배치 `V6` 가 그것을 잡는다** — 즉 `V6` 는 시드 데이터 검사가 아니라
> 런타임 결함 검사다.

---

## 12. 아직 정하지 못한 것

**`CouponPolicyType` 에 `DATA_GRANT` 가 없다.** `V1__init_schema.sql` 의 `coupon_templates` 에는
`data_grant_mb` 컬럼이 있고 `policy_type` 주석이 `PERCENT_CAPPED / FIXED_AMOUNT / DATA_GRANT` 다.
시드가 `DATA_GRANT` 를 넣는 순간 `@Enumerated(STRING)` 역직렬화가 터진다.

**③ 답변 대기 둘.** Redis 선점 카운터 TTL(미영속 발급 검증의 전제)과 Kafka DLT 계약.
답이 오기 전까지 착수하지 않는다.
