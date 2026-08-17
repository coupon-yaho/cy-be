# 배치 서버 구현 방향

> 무엇을 어떻게 만드는가. 왜 그렇게 정했는지의 배경은 `10-batch-design.md`.

## 계약 원본

시드와 맞춰야 하는 계약의 **원본은 시드 저장소** `coupon-yaho/cy-seed-data-generator` 의
`contract.json` 이다. 이 저장소에는 읽기 전용 사본을 `docs/contract.json` 으로 둔다.

```
원본   cy-seed-data-generator @ 96b12f2  (2026-08-13)
사본   docs/contract.json                 바이트 동일
```

**사본을 손으로 고치지 않는다.**

검증과 갱신은 다른 일이다. 한 명령으로 겸하면 차이가 났을 때 *사본을 손댄 것*인지
*원본이 바뀐 것*인지 구별할 수 없다.

둘 다 **먼저 임시 파일에 받고, 성공했을 때만** 비교하거나 덮는다.
`> docs/contract.json` 은 명령이 돌기 전에 파일을 비우므로, 조회가 실패하면 사본이 0바이트가 된다.
검증도 마찬가지로 조회가 실패하면 빈 입력과 비교해 "사본을 손댔다"로 잘못 읽힌다.

```bash
# 검증 — 기록된 리비전과 바이트 동일한가. 차이가 나면 사본을 손댄 것이다
set -o pipefail
tmp=$(mktemp)
gh api "repos/coupon-yaho/cy-seed-data-generator/contents/contract.json?ref=96b12f2" \
  --jq '.content' | base64 -d > "$tmp" \
  && diff "$tmp" docs/contract.json && echo "사본이 원본과 같다"
rm -f "$tmp"
```

```bash
# 갱신 — 원본이 새 리비전으로 올라갔을 때만. 위 표의 SHA·날짜도 같이 고친다
set -o pipefail
tmp=$(mktemp)
gh api "repos/coupon-yaho/cy-seed-data-generator/contents/contract.json?ref=<새 SHA>" \
  --jq '.content' | base64 -d > "$tmp" \
  && mv "$tmp" docs/contract.json
rm -f "$tmp"
```

갱신은 **계약이 바뀌었다는 뜻**이라 배치 코드도 같이 봐야 한다. 조용히 덮지 않는다.

사본을 두는 이유는 **어긋남을 잡을 수 있게 하려는 것**이다. 사본이 없으면 배치 코드가
계약과 맞는지 확인하려고 매번 다른 저장소를 열어야 하고, 리뷰어(사람·CodeRabbit)는
아예 못 본다. 실제로 리뷰 설정이 계약을 자기 말로 다시 적었다가 두 번 어긋났다.

**계약과 이 문서가 다르면 계약이 이긴다.** 이 문서는 계약을 구현으로 옮긴 것이고,
계약은 시드가 실제로 심는 데이터를 규정한다.

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

## 2. JPA 를 쓰지 않는다

배치는 JPA 엔티티를 만들지 않고 `JdbcTemplate` 과 Spring Batch JDBC 리더·라이터로 간다.

**근거.** `batch → storage` 가 `runtimeOnly` 라 컴파일 타임에 Entity·JpaRepository 를 볼 수 없다.
그런데 `DataSource` 는 Boot 자동설정 빈이고 `spring-jdbc` 타입이라 이 제약과 무관하게 주입된다.
검증 규칙은 전부 집계 SQL 이고 리플레이는 이력 순회다 — **JPA 가 할 일이 없다.**

```
V1 V2 V6      tasklet + 단일 SQL
V3 V5         tasklet + 단일 SQL (asof_state 조인)
Step 0 · V4   발급건 식별자 창 기반 커스텀 ItemStreamReader → Processor → CompositeItemWriter
              (JdbcPagingItemReader 를 안 쓴다 — 창 경계가 발급건 단위여야
               한 발급건의 이력이 두 창에 걸치지 않는다)
시드 적재      JdbcBatchItemWriter + rewriteBatchedStatements=true
```

엔티티 17개와 어댑터를 만들지 않는다. 300만~534만 행에 영속성 컨텍스트를 얹지 않는 것도 부수 효과다.

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

메타 테이블은 `V2__batch_metadata.sql` 이 만든다. `spring-batch-core` 6.0.4 원본 그대로이고,
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
  api/        관리 포트 트리거

core/src/main/java/com/kafkick/core/
  coupon/       IssuanceStatus · IssuanceEventType · CouponStatus · CouponStateMachine
  verification/ FindingType · TargetKey · ScopeType · DatasetType · VerdictType · StatsStatus
```

`job/` 과 `schedule/` 을 가르면 *"이건 Batch 인가 Scheduled 인가"* 를 폴더가 답한다.
한 폴더에 몰면 다음 사람이 `cleanupJob` 도 Spring Batch 로 만든다.

`CouponStateMachine` 이 `core/coupon` 에 있는 이유는 **런타임도 같은 클래스를 써야 하기 때문**이다.
검증 전용 패키지에 두면 두 벌로 갈라져 같은 버그를 양쪽이 재현한다.

### `application.yml` 은 문서가 둘이다 — `---` 를 지우면 설정이 조용히 죽는다

`batch/application.yml` 은 `spring.config.import: classpath:storage.yml` 로 DB 설정을 가져온다.
**들어온 storage.yml 이 그것을 선언한 문서를 이긴다.** 직관과 반대다.

실측으로 드러난 상태다 — 관측 담당자가 batch 컨텍스트에서 `Environment` 를 직접 찍었다.

| 키 | batch 가 적은 값 | 실제로 뜬 값 | |
|---|---|---|---|
| `spring.flyway.enabled` | `false` | `true` | storage.yml 이 이김 |
| `spring.datasource.hikari.maximum-pool-size` | `4` | `10` | storage.yml 이 이김 |
| `spring.datasource.hikari.pool-name` | `batch-pool` | `batch-pool` | storage.yml 에 같은 키가 없음 |

**에러도 경고도 없다.** 기동은 되고 값만 다르다. 그래서 두 결정이 동시에 깨져 있었다 —
마이그레이션 소유자를 api 하나로 두기로 한 것과, 배치 풀을 런타임과 나누기로 한 것.

고치는 방법은 `---` 하나다. 같은 파일 안에서 **import 를 선언한 문서보다 뒤 문서가 이긴다.**

```yaml
spring:
  config:
    import: classpath:storage.yml
  # 여기 적은 값은 storage.yml 에 같은 키가 있으면 진다
---
# storage.yml 을 덮어쓸 값은 전부 이 아래
spring:
  flyway:
    enabled: false
```

덮어쓸 값만 `---` 아래 둔다. 무관한 키를 아래로 내리면 **충돌 검사 범위가 조용히 줄어든다** —
검사는 앞 문서만 훑기 때문에, 나중에 storage.yml 에 같은 키가 생겨도 아무도 모르게 된다.

Flyway 를 끄는 이유는 계층 2 의 불변식이다 — 검증 배치가 DDL 권한을 쥐면
*"원본 테이블에 쓰지 않는다"* 가 스키마 수준에서 깨진다.

**대가는 배포 순서 의존인데, 지금은 그 위반이 조용하다.** batch 에는 `@Entity` 가 없어
`ddl-auto: validate` 가 공허하게 통과하고 `initialize-schema: never` 라 메타 테이블도 안 본다.
`@Scheduled`·`@RestController` 도 아직 0건이다. 그래서 **빈 DB 에서도 기동이 그냥 성공한다.**
실패는 잡 실행 시점의 "테이블 없음" SQL 에러로 늦게 나타나고, 스택트레이스가 SQL 계층이라
*"배포 순서를 틀렸다"* 가 아니라 *"검증 배치가 깨졌다"* 로 읽힌다.

관측할 신호도 없다 — actuator 의존성이 저장소에 없어 **헬스 엔드포인트 자체가 없다.**
톰캣만 뜨고 매핑은 0개다. 프로세스가 살아 있다는 것 말고는 밖에서 알 수 있는 게 없다.

기동 시점 가드는 **compose 가 들어오는 티켓의 몫**으로 남긴다. 지금은 compose 파일 자체가 없어
강제할 순서도 없고, 배치를 띄우는 경로가 테스트 말고 없다. 넣을 때는 `ApplicationRunner` 로
둔다 — 컨텍스트 refresh 이후에 돌아 `FlywayMigrationInitializer` 보다 확실히 뒤에 온다.
`InitializingBean` 은 그 순서가 보장되지 않으니 쓰지 않는다.

### 풀 크기 손잡이가 바뀌었다

이 변경 전에는 storage.yml 이 이겨서 **batch 풀도 `DB_POOL_SIZE` 가 움직였다.**
이제 `BATCH_DB_POOL_SIZE` 다. `DB_POOL_SIZE` 만 주던 배포는 batch 쪽이 10 에서 4 로 바뀐다.

### 테스트가 지킨다 — 실패 원인이 갈리도록 나눴다

```
ConfigImportPrecedenceRuleTest       Spring 규칙 자체. 깨지면 Boot 가 규칙을 바꾼 것이다
ConfigImportPrecedenceOverlapTest    겹침 판정 규칙(storage). 깨지면 판정기가 헐거워진 것이다
BatchConfigPrecedenceTest            batch .example 파싱. 깨지면 우리가 키를 잘못 둔 것이다
ApiConfigPrecedenceTest              api 쪽 같은 검사. 판정기는 storage 픽스처로 공유한다
ResolvedBatchConfigTest              Boot 로 실제 로드해 값을 본다. 깨지면 결과가 틀린 것이다
ApiResolvedConfigTest                api 쪽 같은 계층. HermeticBoot 도 픽스처로 공유한다
```

**파싱만으로는 부족하다.** SnakeYAML 이 잡는 것은 문법과 중복 키뿐이고, **Boot 단계에서만 드러나는 것**은
전부 통과한다 — 기본값 없는 플레이스홀더, import 가 없는 파일을 가리키는 경우,
Hikari·Flyway 프로퍼티 **이름 오타**.

마지막이 가장 나쁘다. **운영 기동을 죽이지 않는다** — `@ConfigurationProperties` 의
`ignoreUnknownFields` 기본값이 `true` 라(Boot 4.1.0 바이트코드 확인) 모르는 키는 **예외 없이
무시되고 기본값이 뜬다.** 그래서 compose 가 들어와 기동 스모크 테스트가 생겨도 이 검사를 대체하지 못한다.
`ApiResolvedConfigTest` 가 `NoUnboundElementsBindHandler` 로 DB 없이 잡는 것이 **유일한 검출 지점**이다 —
기본 `Binder` 도 모르는 프로퍼티를 조용히 무시하므로 그 핸들러 없이는 검증하는 척만 한다.

**`batch.*` 는 다른 방식으로 죽는다.** storage.yml 과 겹칠 상대가 없어 위 검사들이 못 본다.
키 경로가 틀리면 `@Value` 가 기본값으로 폴백하고 에러도 경고도 없는데, 하필
`.example` 의 값과 `@Value` 기본값이 **글자까지 같아서** `Environment` 를 찍어 봐도
*"적힌 값이 떴다"* 로 보인다 — 이 절을 만든 사고보다 한 단계 더 조용하다.
그래서 `ResolvedBatchConfigTest` 가 **기본값과 다른 값**을 주입해 키 경로가 살아 있는지 본다.
크론 다섯은 읽는 코드가 아직 0건이라 `CronExpression.parse` 로 문법만 미리 잡아 둔다.

**겹침은 문자열 일치가 아니다.** Boot 는 프로퍼티 트리로 바인딩하므로 판정도 트리로 해야 한다.

```
a  vs  a[0]      같은 컬렉션 — 상위 소스가 잎을 주면 하위의 [0] 은 통째로 사라진다
a[0] vs a[1]     같은 컬렉션 — Boot 는 항목을 준 첫 소스에서 멈춘다
a-b  vs  aB      같은 프로퍼티 — relaxed binding
logging.level.*  예외 — Map 은 소스별 엔트리를 합친다. 조상 관계여도 둘 다 산다
```

마지막이 중요하다. Map 루트를 겹침으로 잡으면 **정당한 키를 실패시키면서 메시지가
`---` 아래로 내리라고 반대로 안내한다.** 그러면 그 키는 옮겨져도 효과가 없고 검사 밖에 영구히 남는다.

**키 위치만 보는 것은 `BatchConfigPrecedenceTest` 와 `ApiConfigPrecedenceTest` 둘이다.** 그래서 뒤 문서에
`spring.config.activate.on-profile` 이 붙어 **문서가 통째로 비활성화돼도** 키 위치는 그대로라 통과한다.
`ResolvedBatchConfigTest` 가 결과값으로 그 틈을 메우는데, **덮개가 전면적이지는 않다** —
하드코딩한 네 프로퍼티(`flyway.enabled` · `maximum-pool-size` · `pool-name` · `datasource.url`)만 본다.

**api 도 같은 두 층을 갖는다.** `HermeticBoot` 와 판정기를 storage 픽스처로 올려 두 모듈이 공유한다 —
`---` 를 기다리지 않은 이유는, api 의 `.example` 이 저장소 어디에서도 Boot 로 로드되지 않는 상태였고
하필 마이그레이션을 쥔 쪽이 그랬기 때문이다.

그 키를 예외로 허용하는 목록은 **두지 않았다.** 저장소에 프로파일이 0건이라
오늘 없는 문제에 구멍을 내는 셈이고, 예외로 뚫은 키는 그대로 검사 밖에 영구히 남는다.
프로파일을 쓰게 되는 날 이 검사가 빨개지고, 그때 전제부터 다시 보면 된다 — 실패 메시지에 적어 뒀다.

`ConfigImportPrecedenceRuleTest` 는 우리 파일을 보지 않는다. `batch/src/test/resources/precedence/`
의 합성 yml 세 벌로 **Spring 규칙만 재현한다.** 이것이 빨간 것은 우리 설정이 아니라
Boot 가 규칙을 바꿨다는 뜻이다 — `.example` 을 뒤지면 안 된다.

컨텍스트를 띄우는 둘은 `HermeticBoot` 를 쓴다. **`systemProperties` 와 `systemEnvironment` 를
아예 붙이지 않는다.** 두 소스는 설정 파일보다 높아서, 셸에 `SPRING_FLYWAY_ENABLED` 나
`SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE` 가 있으면 파일과 무관하게 값이 바뀐다.
커맨드라인 인자로 로케이션만 고정하는 것으로는 못 막는다 — **단언 대상 프로퍼티 자체**의
relaxed 표기가 그대로 들어오기 때문이다.

더 나쁜 쪽은 거짓 빨강이 아니라 **거짓 초록**이다. Compose 표기를 셸에 export 해 둔 사람 앞에서는
`---` 를 지워도 통과한다 — *조용히 죽는 설정*이 그것을 잡으라고 만든 테스트를 통과하는 셈이다.
밀폐가 깨졌는지는 테스트가 스스로 확인한다. 부팅 전에 `spring.flyway.enabled=true` 를
시스템 프로퍼티로 심어 두고, 그 값이 결과에 나오면 실패한다.

`.example` 을 직접 읽는 것은 `BatchConfigPrecedenceTest`·`ApiConfigPrecedenceTest` 둘이고, `ResolvedBatchConfigTest` 는 빌드가 만든 그 사본을 읽는다. 실제 `application.yml` 은 gitignore 대상이라 사람마다 다르고 CI 에는 없다.
**저장소가 지킬 수 있는 것이 `.example` 뿐이라는 뜻이고, 두 파일을 같이 고쳐야 하는 이유이기도 하다** —
`.example` 만 고치면 이미 복사해 쓰는 사람은 낡은 설정으로 뜨고 그의 `./gradlew test` 는 초록이다.

### 이 절에서 일부러 안 한 것

리뷰에서 나왔지만 **오늘 결함이 아니라 대칭 문제**라 조건만 적어 둔다. 조건이 만족되는 날 하면 된다.

| 미룬 것 | 언제 하나 |
|---|---|
| batch 에도 `NoUnbound` 바인딩 층 | batch 의 모듈 전용 `spring.*` 는 4키다. 그중 하나라도 오타가 나거나 키가 늘면 |
| 사본(`resolved/application.yml`) 중복 리소스 가드 | `.example` 을 가진 **세 번째 모듈**이 생기는 날. 그때 `ConfigImportPrecedence.only()` 와 같은 판정으로 맞춘다 |
| `@Value` 스캔을 패키지 전체로 | 지금 `@Value` 는 `VerifyJobConfig` 13곳이 전부다. 스케줄러·API 가 배선되면 그때 `ClassPathScanningCandidateComponentProvider` 로 넓힌다 |
| `--spring.config.location` 문자열 상수화 | 경로가 바뀌면 "복사본이 아예 없다" 로 **시끄럽게** 죽는다. 조용히 깨지지 않아 급하지 않다 |

**여기서 멈춘 기준은 "지적이 0건인가" 가 아니라 "배포 동작이나 거짓 초록에 영향이 있는가" 다.**
전자를 종료 조건으로 두면 고칠 때마다 새 코드가 생기고 리뷰어가 그것을 또 보므로 구조적으로 안 끝난다.

> ⚠️ **이것들을 돌리는 CI 가 아직 없다.** `.github/workflows/` 넷 중 Gradle 을 실행하는 것이 하나도 없어
> (`grep -rn gradlew .github/workflows/` → 0건), 지금 이 방어선은 **사람이 로컬에서 돌릴 때만** 선다.
> 빌드 잡 추가는 저장소 전체 사안이라 별도 티켓이다.
>
> 그 티켓은 **`./gradlew test`(전 모듈)를 불러야 한다.** 겹침 판정을 지키는
> `ConfigImportPrecedenceOverlapTest` 는 `:storage:test` 에 있는데, `batch`·`api` 는 storage 의
> **testFixtures 산출물**에만 의존하지 그 모듈의 test 태스크에 의존하지 않는다.
> `./gradlew :batch:test` 만 돌리면 판정기가 헐거워져도 아무 신호가 없다.

CY-179(관측 전용 DataSource)가 머지되면 관측 풀 이름의 자리는 **storage.yml 이 그 키를
정의하는지로 갈린다** — 정의하면 `---` 아래(덮어쓰기), 정의하지 않으면 첫 문서(새 키)다.
인수인계 문서는 storage.yml 이 정의한다고 했지만 아직 어느 브랜치에도 없어서,
**지금 상태의 정답은 첫 문서다.** 주석 말고 `BatchConfigPrecedenceTest` 가 어느 쪽이라고 하는지를 믿는다.

그 키가 생기기 전까지 `OBS_POOL_NAME` 은 **아무 데서도 읽히지 않는다** — 지금 줘도 효과가 없다.

> ⚠️ **`api` 도 같은 구조라 가드를 함께 붙였다.** `api/application.yml.example` 은 같은
> `spring.config.import` 를 쓰고 `---` 가 없다. 지금은 storage.yml 과 겹치는 키가 **0개**라
> 활성 버그가 아니고, **api 가 datasource·flyway 키를 처음 적는 순간 활성화된다.**
>
> 조용히 죽는 키가 무엇인지는 따져 봐야 한다. **storage.yml 이 지금 값을 유지하는 한**
> `baseline-on-migrate`(false)·`validate-on-migrate`(true)·`clean-disabled`(true) 는 무시돼도 시끄럽다 —
> Flyway 가 기동 중에 예외를 던진다. 지금 조용한 것은 아래 표가 전부다.
>
> | 키 | 무시되면 | 얼마나 조용한가 |
> |---|---|---|
> | `spring.datasource.url` | **다른 DB 에 붙은 채로 전부 정상 동작한다** | 실패하는 것이 하나도 없다 |
> | `spring.datasource.username`·`password` | 의도한 최소 권한 계정 대신 storage 계정으로 붙는다 | 신호 없음 |
> | `spring.flyway.locations` | 추가한 로케이션의 마이그레이션이 실행되지 않는다 | 나중에 스키마 검증에서 |
> | `spring.datasource.hikari.maximum-pool-size` | `max_connections=50` 배분이 깨진다 | 부하 때 |
>
> 맨 위가 가장 위험하다. 복제본으로 조회를 돌리려고 `url` 을 적었는데 무시되면 **primary 에 붙은 채로
> 아무 일도 안 일어난다.** 복제본 분리가 됐다고 믿은 채 부하를 측정하게 된다.
>
> **값이 뒤집히면 이 표도 뒤집힌다.** storage 가 `clean-disabled: false` 가 되는 순간
> api 가 적어 둔 `true` 는 조용히 죽고, 그때는 예외가 아니라 `flyway.clean()` 이 살아난다.
> 그래서 판정은 이 표가 아니라 **검사기가 한다** — 세 키도 함께 잡는다.
>
> **마이그레이션 소유자가 api 라서** Flyway 를 만질 사람은 그 파일을 연다. 그때 빨개지지 않으면
> 스키마가 잘못 마이그레이트된 채로 지나간다. 그래서 판정기를 `storage` testFixtures 로 올려
> `api`·`batch` 가 같은 한 벌을 쓴다 — 모듈마다 복사하면 한쪽만 고쳐진다.
>
> ⚠️ 다만 이 브랜치는 `feature/CY-15`(배치 에픽) 아래라 **PR #6 이 머지되기 전까지 api 담당자에게 닿지 않는다.**
> 그때까지는 이 문서가 전달 수단이다.

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
  Step 0 는 상태만 만든다. active_usage_count 는 바로 뒤 usageCountStep 이 집계 조인
  한 문장으로 채우고, 그래서 V5 는 실행 시점에 usages 를 조인하지 않는다.
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

지금 배선된 것 — `VerifyJobConfig#verifyJob` 의 Step 체인 그대로다.

```
startRunStep         실행 행 생성 + 훑을 경계 얼리기 + 선행 조건 검사
replayStep           asof_state 생성  +  V4          접기 한 번에 산출물 둘
usageCountStep       활성 사용을 집계 조인 한 문장으로
usageMismatchStep    V5     결정론
duplicateIssuanceStep V2    결정론
replayMismatchStep   V3     현재 행을 읽는다
stockMismatchStep    V1     현재 행을 읽는다
gradeViolationStep   V6     현재 행을 읽는다
assertFrozenStep     실행 중 발급건·재고·정책(회차+등급)이 얼어 있었는지 다시 확인
finalizeRunStep      판정·검출 수·checksum·지문·종료 시각을 실행 행에 남긴다
                     CORRUPT 는 정답 매니페스트와 집합을 대조해 판정한다
```

**V6 는 `asof_state` 를 안 읽지만 결정론도 아니다.** `issued_grade` 는 스냅샷이라 안 변하는데
`coupons.eligible_grades_mask` 는 살아 있는 행이고 **지문 재료에도 그 축이 없다** —
마스크가 바뀌면 지문은 같은데 검출만 달라진다. 그 조합의 뜻은
*"데이터는 그대로인데 검증기가 비결정적"* 이라, 판정표에서 가장 찾기 어려운 칸에 잘못 떨어진다.

그래서 현재 행을 읽는 규칙들과 같은 구간에 두고, **정책 축은 지문으로 얼린다.**
`coupons` 에는 `updated_at` 이 없어 시각으로 못 보지만 **CLEAN 147 · CORRUPT 291 행이라 값을 접으면 된다.**

```
startRunStep      CONCAT(COUNT(*), ':', HEX(BIT_XOR(SHA2(종류 ⋮ 키 ⋮ 값) 상위 64비트)))  →  잡 컨텍스트
                  coupons(id, mask) 와 grades(code, bit_value) 두 축을 각각 접어 이어 붙인다
assertFrozenStep  다시 계산해 다르면 거부
```

V1 도 `coupons` 를 드라이빙으로 잡으므로 회차 INSERT·DELETE 가 같은 경로로 들어온다.
축은 이제 셋이다 — **발급건 · 재고 · 정책(회차 마스크 + 등급 비트).**

`GROUP_CONCAT` 은 쓰지 않는다 — `group_concat_max_len` 기본 1024 바이트를 넘으면 경고만 내고 잘려서 **가드가 열린 채로 실패한다.** **CLEAN 147행이 실측 920 바이트로 한계의 90% 이고, CORRUPT 291행이면 약 1820 바이트로 이미 넘는다.**
지금 안 잘리는 것은 `GROUP_CONCAT` 을 안 쓰기 때문이지 여유가 있어서가 아니다.

### 통계는 판정 뒤에 온다 (CY-202)

```
… → assertFrozenStep → finalizeRunStep → statsAggregateStep(CLEAN 만)
```

**판정을 먼저 쓴다.** 통계가 죽어도 `verdict` 는 남고, `stats_status` 가 `NULL` 이라
`v_latest_stats_run` 이 그 스냅샷을 **물리적으로 안 보여 준다.** 반대 순서면 통계 문제로 판정을 잃는다.

`PARTIAL` 은 쓰지 않는다 — 중간에 죽으면 손대지 않은 `NULL` 로 남고 뷰가 이미 걸러 낸다.
"절반 쓰다 죽었다" 를 표현하려고 죽는 코드가 값을 쓸 수는 없다. 그건 트랜잭션이 할 일이다.

> **PRD 의 `asof_state` 재사용 서술은 세 테이블 중 하나에도 해당하지 않는다.**
> PRD 는 *"앞 Step 이 만든 `asof_state` 를 재사용하므로 원본 300만 건을 다시 읽지 않는다"* 고 적었지만,
> ⑴ 통계가 세는 값은 `issuances.status` 이고 `asof_state.state` 와 **다를 수 있는 것이 검증 대상**이라
> 통계가 검증 결과에 기대면 순환이 된다, ⑵ `asof_state.coupon_id` 는 **발급건 id** 라(어휘 반전)
> 회차·등급으로 묶으려면 `issuances` 를 조인해야 해서 스캔이 조인으로 바뀔 뿐 이득이 없다,
> ⑶ 요일·시각 분포는 `issuance_histories` 에만 있다.

**값의 정본은 시드 구현이다.** `contract.json` 에 통계 조항이 없어 checksum·지문과 달리 계약으로
고정돼 있지 않다. 그래서 필드마다 시드가 **실제로 읽는 컬럼**을 그대로 쓴다:

| 필드 | 원천 | 시드 근거 |
|---|---|---|
| `hourly` | `ISSUE` 이력의 `created_at` | `_history()` 안에서 `event == EV_ISSUE` 일 때 센다 |
| `sold_out_seconds` | `MAX(issuances.issued_at)` | 이력이 아니라 루프 변수 `last_issue_at` 을 쓴다 |
| `issued/used/…` | `issuances.status` | 리플레이 상태가 아니다 |
| 완판 판정 | `issued_total >= coupon_stocks.total_quantity` | `catalog.py` 의 `n >= c.total_quantity` |

**`active_count` 로 완판을 판정하면 안 된다.** 그것은 *현재 보유량*(ISSUED + USED)이라 취소·만료로
줄어든다 — 미달 회차도 0 이 될 수 있고 완판 회차도 0 이 아닐 수 있다.

**요일 변환은 SQL 이 한다.** `WEEKDAY()` 는 0 = 월요일로 파이썬 `weekday()` 와 같아
`ELT(WEEKDAY(x) + 1, 'MON', …)` 이 시드의 `DOW[at.weekday()]` 와 글자 단위로 대응한다.
`DAYNAME()` 은 `lc_time_names` 세션 변수에 의존해 쓰지 않는다.

**`hourly_stats` 는 168행을 전부 쓴다.** 없는 행과 0인 행은 다르다 — 빼면 대시보드가
"그 시각에 데이터가 없다" 와 "0건이다" 를 구분할 수 없다.

**이력을 읽는 창은 리플레이와 같다** — `id <= frozenMaxHistoryId AND created_at <= asOf`.
`created_at` 만 걸면 다시 재는 것이라 백데이트 이력을 리플레이는 못 읽고 통계는 읽는다.

### 판정은 두 값의 조합으로만 뜻이 있다

```
findings_checksum    정렬된 (finding_type, target_key) 만 → SHA-256
                     계약이 정한 인코딩: type + U+001F + key + U+001E 반복
dataset_fingerprint  max(hist.id) | count(hist) | count(issuances)
                     | sum(active_count) | max(issuances.updated_at)
```

판정표가 읽는 것은 **조합**이다.

| 지문 | checksum | 뜻 |
|---|---|---|
| 다름 | 다름 | 데이터가 바뀌었다 |
| 같음 | 같음 | 재실행 결정론 성립 |
| 같음 | **다름** | 🔴 **검증기 버그** — 진짜로 잡고 싶은 칸 |

**두 값을 자바에서 계산한다.** checksum 은 `GROUP_CONCAT` 을 쓰면
`group_concat_max_len` 을 넘길 때 경고만 내고 잘려서, 오염셋 800행에서 **뒤쪽 검출이
checksum 에 안 들어간다** — 결정론 판정이 열린 채로 통과한다. 중간 리스트 없이 행마다 접는다. **커서 스트리밍은 아니다** — 행 수 방어는 규칙 Step 의 상한(10000 × 6 = 6만 행 천장)이 한다.

지문의 시각은 **`getTimestamp` 로 읽으면 안 된다.** JVM 기본 시간대로 변환돼,
서버가 UTC 라도 KST 머신에서 돌리면 9시간이 얹힌다 — **같은 데이터가 머신마다 다른 지문**을 낸다.
실제로 그렇게 났고 계약 대조 테스트가 잡았다. `LocalDateTime` 으로 직접 받는다.

> **정렬은 콜레이션이 아니라 코드포인트 순서다.** MySQL 기본 콜레이션은 UCA 라
> `|`(U+007C)를 숫자보다 앞에 두는데 참조 구현의 파이썬 `sorted()` 는 반대다(실측).
> `CAST(... AS BINARY)` 로 맞춘다.
>
> 덤으로 `uk_run_finding` 커버링 인덱스를 못 타게 되어 **`ORDER BY` 를 지우면 테스트가 잡는다.**
> 그 테스트가 유효한 이유는 데이터가 `COUPON:1|MEMBER:2` 와 `COUPON:11|MEMBER:2` 이기 때문이다 —
> `|` 가 없는 키만 넣으면 두 순서가 같아 다시 사각지대가 된다.

### 오염셋 판정은 집합 일치다

정상셋은 "검출 0건 = 통과" 가 규칙만으로 결정된다. 오염셋은 **개수로 판정할 수 없다** —
오탐 400 + 누락 400 도 800 이라 정확히 검출한 것과 구분되지 않는다.

```
누락  expected_findings  MINUS  verification_findings    규칙이 놓친 것
오탐  verification_findings  MINUS  expected_findings    규칙이 잘못 잡은 것
합격  누락 0 AND 오탐 0
```

**MySQL 에는 `MINUS` 가 없어 `LEFT JOIN … IS NULL` 두 방향으로 만든다.**
한 방향만 보면 두 오차가 상쇄돼 통과한다.

> ⚠️ **조인 키는 `(finding_type, target_key)` 두 컬럼뿐이다.**
> `campaign_id`·`member_id`·`coupon_id`·`history_id` 는 규칙마다 **다르게** 채운다 —
> V1 은 회차만, V4 는 이력만 쓴다. 그 컬럼으로 조인하면 안 쓰는 쪽이 양쪽 다 NULL 이고
> SQL 에서 `NULL = NULL` 은 UNKNOWN 이라 **정확히 검출한 행까지 전부 누락으로 뒤집힌다.**
> 그 사고는 *"누락이 잔뜩 나온다"* 는 모양이라 **규칙을 의심하게 만든다** — 원인은 조인이다.

**기대 행수를 상수로 박지 않는다.** 계약의 800 은 기본 설정일 때의 값이고,
시드의 `--plant-v6` 를 켜면 801 이 된다. 판정은 그 테이블을 **읽어서** 한다.

**매니페스트 부재는 `startRunStep` 이 실행 전에 죽인다.** 그대로 두면 검출 전부가 오탐으로 잡혀
*"오탐 800건"* 이라는 엉뚱한 결론이 나오고, 진짜 원인(주입을 안 돌렸다)이 안 보인다.
마지막 Step 에서 알면 리플레이 300만 건과 규칙 여섯을 다 돌린 뒤라 그 실행을 통째로 버려야 한다.

**불일치는 판정이지 실행 실패가 아니다.** 검증은 정상적으로 돌았고 답이 "다르다" 인 것이므로
`verdict=FAIL` 로 남기고 **checksum·지문도 함께 기록한다** — 그 두 값이 가장 필요한 순간이 여기다.
부재를 앞으로 옮겼기 때문에 이 자리의 `FAIL` 은 *"대조했고 틀렸다"* 한 가지 뜻만 갖는다.

무엇이 어긋났는지는 Step 종료 메시지에 싣고, Step **종료 코드는 `FAILED`** 로 둔다.
합격에도 `seedRunId` 와 두 총수를 메시지로 남긴다 — PASS 행에는 대조 상대가 안 적히기 때문이다.

> **게이트는 `verification_runs.verdict` 를 읽는다. 프로세스 종료코드가 아니다.**
> 한때 이 문서가 *"`COMPLETED` 면 Boot 가 종료코드 0 으로 매핑해 CI 가 초록으로 읽는다"* 고 적었는데 틀렸다.
> `JobExecutionExitCodeGenerator.getExitCode()` 는 `ExitStatus` 가 아니라 **`BatchStatus`** 를 읽고
> (`COMPLETED.ordinal() == 0`), `BatchApplication.main` 은 `System.exit(SpringApplication.exit(..))` 를
> 부르지 않으며, `web-application-type: servlet` 이라 프로세스가 끝나지도 않는다.
> 게다가 `SimpleJob` 은 잡 종료 코드를 **마지막 Step 값으로 대입**해서, 통계 Step 이 뒤에 붙는 순간
> 이 표식은 잡 수준에서 사라진다. `ExitStatus` 는 배치 메타DB 조회용 표식으로만 쓴다.

**정답 총수와 검출 총수를 함께 적는다** —
`정답 800 / 검출 0` 은 규칙이 안 돈 것이고, `정답 800 / 검출 800` 인데 누락·오탐이 400씩이면
`target_key` 포맷이 어긋난 것이라 두 수가 있어야 실패 모양이 갈린다.

**`seedRunId` 는 CORRUPT 에 필수다.** 기본값을 두면 정답 묶음이 둘 이상인 DB 에서
**조용히 낡은 묶음과 대조**한다 — 주입을 두 번 돌리면 실제로 그렇게 되고,
*"누락 800 · 오탐 800"* 으로 나타나 규칙을 의심하게 만든다.
참조 구현은 기본값 1 을 두지만(`cy-seed/bin/seed.py` 의 `--seed-run-id`) **여기서는 일부러 다르게 간다** —
시드는 방금 주입한 묶음을 같은 프로세스 안에서 대조하고, 배치는 **남의 DB 를 나중에 읽는다.**

### V2 는 케이스가 둘인데 규칙은 하나다

```
케이스 1  GROUP BY coupon_id, member_id  HAVING COUNT(*) > 1    오염 유형 6
케이스 2  GROUP BY coupon_id, code       HAVING COUNT(*) > 1    오염 유형 5
          MIN(id) 는 원본이라 빼고, 복제본의 member 로 키를 만든다
```

둘을 별도 규칙으로 나누면 `target_key` 형식이 같아 **같은 행이 두 규칙에 잡히고** 집합 비교가
어긋난다. `FindingType` 이 *"V7 같은 규칙을 새로 만들지 않는다"* 를 못 박은 이유가 이것이다.

**케이스 2 에서 `MIN(id)` 를 빼지 않으면 원본 회원까지 검출된다.** 오염 100건이 200건으로
부풀어 오탐이 되고, 합격 조건이 "누락 0 · 오탐 0" 이라 게이트가 통째로 떨어진다.

**둘을 `UNION` 으로 합친다 — `UNION ALL` 이면 안 된다.** 한 회원이 두 케이스에 다 걸리면
같은 키가 두 번 나오고, 잡에서는 `uk_run_finding` 중복키로 **실행 전체가 죽는다.**

`updated_at <= asOf` 는 **집계 안팎에 모두** 건다. 안쪽을 안 자르면 `asOf` 이후 행이
`COUNT(*)` 를 올려 **그 시점에는 없던 중복이 보인다.**

### 스키마 주인은 cy-be 이고, 어긋남은 테스트가 잡는다

**이 절의 결정이 CY-201 에서 뒤집혔다.** 원래는 *"CORRUPT 스키마 모양은 시드 저장소가 원본이다"* 였고
근거는 *"cy-be 가 두 번째 주인처럼 보이면 둘이 어긋나도 아무도 모른다"* 였다.

문제는 **모르는 것**이었으므로 답은 **알게 만드는 것**이다 — 주인을 한쪽으로 정하는 것으로는
어긋남을 못 잡는다. 실제로 못 잡았다: `datetime` ↔ `datetime(6)` 세 컬럼과 제약 이름 두 개
(`code`/`uk_coupon_code`, `email_hash`/`uk_email_hash`)가 아무 경고 없이 갈라져 있었다.

```
storage/src/main/resources/db/migration/   ← 스키마를 정의한다 (Flyway, cy-be 소유)
cy-seed ddl/                               ← 로더의 적재 순서 최적화. 구조는 자유
storage/src/test/.../seed-ddl/             ← 그 읽기 전용 사본
SchemaParityTest                           ← 두 DDL 의 최종 상태를 information_schema 로 대조
```

**파일 구조가 아니라 최종 상태를 맞춘다.** 시드는 300만 건을 빠르게 넣으려고
*테이블만 → 적재 → 제약* 으로 쪼개는데 그것은 로더의 사정이고, 만들어지는 스키마는 같아야 한다.
`--with-perf-indexes` 처방전은 대조에서 뺀다 — 보조 인덱스 부재는 누락이 아니라 의도다.

> 덤으로 알게 된 것: **InnoDB 는 FK 자동 인덱스를 스스로 지운다.** 선두가 일치하는 복합
> 인덱스를 만들면 자동 생성된 단일 인덱스가 사라진다(실측 확인). 처방전을 넣은 쪽과 안 넣은 쪽의
> 인덱스 목록이 두 군데서 달라 보이는 이유가 이것이다.

### CLEAN 에서 못 심는 오염은 오버레이로 재현한다

V2 가 잡는 두 케이스는 CLEAN 에서 **물리적으로 심을 수 없다.** 제약이 막는다.

```
uk_coupon_member          유형 6 — 같은 회원 2건
issuances.code (UNIQUE)   유형 5 — 같은 code 복제
ck_stock_range            유형 1(+1) · 3(-1) — 재고를 범위 밖으로
```

시드 저장소가 `ddl/11_constraints_clean.sql`(거는 쪽)과 `ddl/12_constraints_corrupt.sql`
(안 거는 쪽)으로 갈라 두었고, **실제 `coupon_corrupt` 스키마는 그쪽이 만든다.**
정의가 cy-be 에 있다는 것과 인스턴스를 누가 만드느냐는 다른 얘기다.

cy-be 는 그 모양을 테스트에서 재현할 뿐이라
`storage/src/testFixtures/resources/db/corrupt/V900__drop_clean_only_constraints.sql` 에 둔다 —
`main` 에 두면 jar 에 실려 **cy-be 가 CORRUPT 스키마의 두 번째 주인처럼 보이고**, 둘이 어긋나도
아무도 모르게 된다. 쓰는 쪽은 `@CorruptRepositoryTest` 이고 Flyway 로케이션 한 줄만 다르다.

> `uk_coupon_member` 는 `(coupon_id, member_id)` 라 `coupon_id` FK 가 쓰는 유일한 인덱스이기도 하다.
> 그냥 떨어뜨리면 MySQL 이 막으므로 **대체 인덱스를 먼저 깐다.** 시드 쪽 CORRUPT 는 그 유니크가
> 애초에 없어 FK 를 걸 때 MySQL 이 자동 생성하는데, 여기서는 있는 것을 떼는 순서라 손으로 만들어 준다.

`ck_stock_range` 는 `V1__init_schema.sql:289` 가 문서로 적어만 두고 실제 DDL 이 빠져 있었다.
`V5__stock_range_check.sql` 로 CLEAN 경로에 세운다 — 불변식을 DB 제약으로 표현한다는
PRD 설계 원칙 1번이고, 적어만 두고 안 건 것은 원칙을 어긴 것이다.

### V1 은 V3 와 정반대로 드라이빙을 잡는다

같은 "현재 행을 읽는 규칙"인데 드라이빙 테이블이 반대다. 이유가 다르기 때문이다.

| | 드라이빙 | 왜 |
|---|---|---|
| V3 | `asof_state` | 발급건 300만이라 전수가 비싸다. 접힌 상태가 없는 행은 기대 매트릭스 밖이다 |
| V1 | `coupons` | 다른 둘은 각각 한쪽을 놓친다 — 아래 참조 |

```
asof_state 드라이빙      활성이 0인데 재고가 남은 회차를 놓친다   ← 오염 유형 1
coupon_stocks 드라이빙   재고 행이 없는데 발급이 쌓인 회차를 놓친다 ← 초과 발급의 가장 위험한 형태
coupons 드라이빙         둘 다 본다
```

회차는 CLEAN 147 · CORRUPT 291 개뿐이라 **전수 비용이 어느 쪽이든 같다.** 그러니 둘 다 보는 쪽을 고른다.

활성은 `ISSUED`·`USED` 다 — 컬럼 주석이 못 박은 **현재 보유량**이지 누적 발급 수가 아니다.
`CANCELLED`·`EXPIRED` 는 재고로 돌아간 것이라 빠진다.

### 재고 축 가드를 새로 걸었다

V3 가 현재 `issuances.status` 를 읽어서 `hasIssuancesUpdatedAfter` 로 시작·끝을 막는 것과 같다.
**V1 은 현재 `coupon_stocks.active_count` 를 읽는데 그 축의 가드가 없었다.**

배치가 도는 동안 발급이 한 건만 일어나도 그 회차가 어긋난 것으로 잡히고 재실행 결과가 달라진다.
그냥 빼면 **0건이 두 뜻을 갖는다** — *"제대로 훑고 없었다"* 와 *"훑을 대상이 안 남았다"*.

### V1 의 기대 200행 — 시드가 이미 맞춰 두었다

`verification_findings` 에 `uk_run_finding(run_id, finding_type, target_key)` 가 걸려 있고
`STOCK_MISMATCH` 의 `target_key` 는 `COUPON:{coupons.id}` 하나뿐이다. SQL 도 회차당 한 행만 낸다.
**따라서 `STOCK_MISMATCH` 최대 행수 = 오염 대상 회차 수다.**

여기까지 보고 "회차 147개로는 200행이 불가능하다" 고 적었었는데 **틀렸다.**
147 은 CLEAN 의 숫자고 V1 의 200행은 CORRUPT 에서 나온다. 시드 저장소를 열어 확인한 결과
세 가지가 전부 이미 처리돼 있다.

```
seedgen/config.py
  PAST_MONTHS_CLEAN   = 12    12 브랜드 × 12개월 = 144 (+현재월 = 147)
  PAST_MONTHS_CORRUPT = 24    "오염셋은 V1(회차 그레인) 키 200개가 필요해서 24개월"
  CORRUPT_V1_TYPE1_SLOT = (0, 100)      유형 1 → 과거 회차 [0, 100)
  CORRUPT_V1_TYPE3_SLOT = (100, 200)    유형 3 → 과거 회차 [100, 200)
```

**① 회차 수** — CORRUPT 는 과거 12 × 24 = 288 에 현재 회차 3 을 더해 **291개**다
(`corrupt.py` 첫머리가 "24개월 달력(291회차)" 이라고 적는다). 200행이 나온다.
CLEAN 의 147 도 같은 셈이다 — 과거 144 + 현재 3. **두 숫자의 기준을 섞지 말 것.**

**② 유형 1·3 의 겹침** — 슬롯이 서로소로 나뉘어 있다. `corrupt.py` 첫머리가 그 이유를 적어 둔다 —
*"V1 은 회차 그레인이라 유형 1 과 유형 3 이 같은 회차에 겹치면 target_key 가 충돌한다"*.

**③ 유형 4 가 접힌 활성 수를 바꾸는가** — 바꾸지만 **재고도 같이 맞춘다.**

```python
# 유형 4 — 종단 상태에서 USED 로 불법 전이.
# 나머지 축(status·usage·재고)은 전부 맞춰서 V4 만 울리게 한다.
replay_state  = C.USED
stored_status = C.USED
usages = [(t2, None)]
...
t.active_count[coupon.id] = active_replay + quota.stock_delta
```

`active_count` 가 **접힌 활성 수(`active_replay`)에서 파생된다.** 유형 4 는 `stock_delta` 를
건드리지 않고, `USED` 로 뒤집힌 것은 이미 `active_replay` 에 들어 있다. **V1 은 침묵한다.**
유형 5·6 도 `_emit_dup_row` 의 반환값이 `active_replay` 에 더해지므로 같다.

> **여기서 배운 것.** 배치 쪽 스키마만 보고 시드의 동작을 추론하면 안 된다.
> 위 세 가지는 전부 `seedgen/` 을 한 번 열어 보면 끝나는 질문이었고, 열지 않은 채로
> "게이트가 통째로 떨어진다" 까지 적었다. **계약의 단일 출처는 시드 저장소다** —
> 그쪽 코드를 근거로 대지 못하는 주장은 적지 않는다.


위 ①②③ 에서 전부 확인 완료 — 시드가 이미 보정한다.

**판정 티켓의 방어** — 게이트가 `expected_findings` 를 읽을 때
`COUNT(*)` 와 `COUNT(DISTINCT target_key)` 를 비교해 다르면 먼저 죽인다.
*"정답 매니페스트가 회차당 2행 이상을 기대한다. `uk_run_finding` 상 불가능하다"* 가
미검출로 보이는 것보다 낫다.

### ⚠️ `coupon_stocks.updated_at` 을 찍을 책임이 아직 코드에 없다

새 가드가 이 컬럼 하나에 통째로 의존하는데, 재고를 차감·복원하는 런타임 코드가 아직 없다.
`ON UPDATE CURRENT_TIMESTAMP` 를 안 건 것은 **의도다** — 걸면 오염 주입기의 UPDATE 가
현재 시각을 찍어 실행 전체가 거부된다.

그래서 발급 경로가 붙을 때 **시각을 인자로 강제하는 시그니처**로 열어야 한다.

```java
public interface CouponStockRepository {
    int decrease(long couponId, LocalDateTime at);   // at 을 안 받으면 잊는다
}
```

`updated_at` 을 안 찍으면 시작·끝 가드가 둘 다 `false` 를 돌려주고, V1 은 배치가 도는 동안
움직인 재고와 얼어붙은 접기 결과를 비교해 **매 실행 다른 회차를 뱉는다.**

### V6 의 마스크는 순서가 아니라 집합이다

```
WELCOME 1 · SILVER 2 · GOLD 4 · VIP 8      (mask & bit) = 0  →  위반
```

마스크 12 는 *"GOLD 이상"* 이 아니라 **`{GOLD, VIP}`** 다. 마스크 9(`{WELCOME, VIP}`)처럼
중간을 건너뛸 수 있으므로 **등급 순서로 판정하면 정반대가 된다** — 그 경우 WELCOME 이 정상이고
SILVER·GOLD 가 위반이다. 테스트가 그 마스크를 못 박고 있다.

**`grades` 에 없는 등급 문자열도 위반이다.** 그래서 `LEFT JOIN` 이다 — `INNER JOIN` 이면 그 행이
조용히 빠져 미검출이 된다. 다만 CLEAN 스키마에서는 FK(`V1__init_schema.sql:641`)가 그 상태를
물리적으로 막아 **검출 테스트를 여기서 쓸 수 없다.** `uk_coupon_member` 가 V2 를 막는 것과 같은 부류다.
**다만 그 FK 는 `V900` 이 떼지 않는다** — 시드 저장소의 CORRUPT 도 등급 FK 는 그대로 둔다.
그래서 지금도 그 FK 가 있다는 것을 테스트가 고정한다. 누가 떼면 빨개지고,
그것이 V6 검출 테스트를 붙이라는 신호다.

`assertFrozenStep` 은 규칙이 늘어도 **항상 마지막**이다. 현재 행을 읽는 규칙이 전부 끝난 뒤에
확인해야 그 사이 갱신을 잡는다. 축이 늘면 여기도 같이 늘어야 한다 — 지금은 발급건·재고·정책(회차+등급) 셋이다.

**V4 는 Step 0 안에 있다.** 별도 Step 이면 이력 534만 행을 다시 접어야 하고, 접기 구현이 두 벌로
갈라져 `asof_state` 와 V4 가 서로 다른 말을 하게 된다. **순서의 주인은 `VerifyJobConfig#verifyJob`
의 Step 체인이다** — 이 표가 어긋나면 표가 틀린 것이다.

> 유형 1 의 계약 설명("재고는 줄었는데 history 에 ISSUE 기록 없음")은 **시드 구현과 다르다.**
> 시드는 이력을 지우지 않고 재고만 올려 같은 어긋남을 만든다. 판정 기준은 `matrix` 이고
> `desc` 는 산문이다 — `AsOfStateRepository` javadoc 에 같은 내용이 적혀 있다.

`V2` 가 결정론 구간에 있는 이유 — `issuances` 만 읽는데 그 테이블에는 `updated_at` 이 있어
`updated_at <= asOf` 로 완전히 자를 수 있다. 가드도 `hasIssuancesUpdatedAfter` 가 이미 갖고 있어
새로 만들 것이 없다. 현재 행을 읽는 V1·V6 과 다른 점이 이것이다.

### 오염 주입은 계약이 정한 규칙만 울려야 한다

**시드가 지켜야 하는 계약이다.** 규칙이 축을 여럿 보므로, 한 축만 비틀고 나머지를 그대로 두면
의도하지 않은 규칙이 함께 운다. 그러면 양방향 MINUS 에서 **오탐**으로 잡혀 게이트가 떨어진다.

**규칙 개수의 주인은 `docs/contract.json` 의 `matrix` 다.** 유형 3 처럼 한 주입이 두 규칙을
울려야 하는 것도 있다 — "하나만" 이 아니라 "계약이 적은 것만" 이다.
계약은 *무엇을 몇 건 기대하는가*만 적고, **어떻게 심어야 그것만 나오는가는 여기 있다.**
시드를 다시 구현할 때 이 표를 먼저 본다.

| 유형 | 비트는 축 | 반드시 함께 맞출 것 | 안 맞추면 |
|---|---|---|---|
| 1 재고 과다 | `coupon_stocks.active_count` +1 | **이력을 지우지 않는다.** 재고만 올린다 | 첫 이력이 `ISSUE` 가 아니게 되어 `V4` 가 함께 운다 |
| 2 저장 상태 지연 | `issuances.status` 를 `ISSUED` 로 | **실제 `USED` 발급건을 골라 status 만 되돌린다.** 활성 사용 행은 그대로 둔다 | `ISSUED` 발급건에 가짜 `USE` 이력을 얹으면 활성 사용이 없어 `V5` 가 함께 운다 |
| 3 사용취소 이중 기록 | `CANCEL_USE` 이력 하나 추가 **+ `coupon_stocks.active_count` 를 1 더 복원** | 최종 상태가 `USED` 로 유지되게 뒤에 `USE` 를 둔다. 활성 사용 1건 유지 | **재고를 안 건드리면 `V1` 100건이 통째로 누락된다** — 접힌 상태가 그대로라 상태 축에는 볼 차이가 없다. 최종 상태가 `ISSUED` 로 굳으면 `V3` 가, 활성 사용이 어긋나면 `V5` 가 함께 운다 |
| 4 종단에서 되살림 | `EXPIRED → USED` 이력 하나 추가 | **`issuances.status` 를 `USED` 로 맞추고 활성 사용 행을 1건 넣는다.** 재고는 따로 건드리지 않는다 — 접힌 상태가 `USED` 라 활성 집계에 이미 포함된다 | 접기가 마지막 `to_status` 를 따라가 `USED` 가 되므로, 둘 다 안 맞추면 `V3` 100건 + `V5` 100건이 함께 운다 |
| 5 코드 중복 | 같은 `code` 를 두 회원에게 | **추가 발급건에 `ISSUE` 이력을 넣고 재고 집계에도 반영한다**(상태 `ISSUED`, 사용 행 없음) | 이력을 안 넣으면 `asof_state` 에 행이 안 생겨 `V3`·`V5` 가 건너뛴다. 이력만 넣고 재고를 안 올리면 `V1` 이 함께 운다 |
| 6 1인 2매 | 같은 회원에게 두 건 | 유형 5 와 같다 | 유형 5 와 같다 |
| 7 유령 사용 | 활성 사용 행을 남김 | 이력은 `USE → CANCEL_USE` 로 최종 `ISSUED`, `status` 도 `ISSUED` | 상태를 안 맞추면 `V3` 가 함께 운다 |

유형 4 가 가장 위험하다. **불법 전이여도 접기는 그 행의 `to_status` 를 따라간다**(계약
`replay_rule.state`). 그래서 이력 한 줄만 얹으면 접힌 상태가 `USED` 로 바뀌고, `issuances.status`
와 활성 사용 두 축이 동시에 어긋난다.

**사건이 일어난 시각은 `asOf` 보다 뒤일 수 없다.** 축마다 결과가 다르다.

```
issuance_histories.created_at > asOf   실행이 거부된다 ("asOf 는 마지막 이력 시각 이상")
issuances.updated_at         > asOf   실행이 거부된다 (시작과 끝에서 두 번 본다)
coupon_stocks.updated_at     > asOf   실행이 거부된다 (시작과 끝에서 두 번 본다)
issuance_usages.used_at      > asOf   조용하다 — 활성 사용이 0 으로 세어져 V5 가 미검출된다
```

**재고 축이 새로 생겼다.** 계약상 `coupon_stocks` 를 <b>직접</b> 건드리는 것은 **유형 1·3 뿐**이다
(`corruption.matrix` 의 `STOCK_MISMATCH` 행 둘). `updated_at` 은 `ON UPDATE CURRENT_TIMESTAMP` 가
아니라 명시 컬럼이라 주입기가 값을 정한다 — 주입 시각을 찍으면 **검증 실행 전체가 거부된다.**

유형 4 는 재고를 직접 안 건드리지만 **접힌 활성 수를 바꿔 V1 을 간접적으로 울린다** — 아래 ③ 참조.

앞의 셋은 원인이 메시지에 남지만 **마지막은 아무 말이 없다.** 유형 7 의 100건이 통째로 사라지고
"누락 100" 으로만 보인다.

**`canceled_at` 만 예외다.** 이 컬럼은 사건이 아니라 *"asOf 시점에 살아 있었는가"* 를 가르는
경계라, 활성 판정식이 `canceled_at IS NULL OR canceled_at > asOf` 다.

```
활성으로 남길 사용 행    used_at <= asOf  AND  canceled_at IS NULL
비활성으로 둘 사용 행    used_at <= asOf  AND  canceled_at <= asOf
```

판정식의 `canceled_at > asOf` 가지는 "스냅샷 뒤에 취소됐다" 를 표현하지만, `asOf` 가
마지막 이력 이상이면서 실행 시작 이하로 조여 있어 **시드가 만들 수 있는 값이 아니다.**
시드는 활성을 `NULL` 로만 표현한다.

`NOW()` 로 찍고 `asOf` 를 주입 완료 시각으로 잡으면 밀리초 차이로 걸린다 —
**`asOf` 를 주입이 쓴 가장 늦은 사건 시각보다 확실히 뒤로 잡는다.**

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

서명이 없으므로 클라이언트가 무엇이든 주장할 수 있다. 그래서 방어선은 둘이다.

| 무엇 | 어떻게 |
|---|---|
| 관리 경로 `/api/v1/admin/**` | **관리 포트를 Compose 에서 외부에 노출하지 않는다** |
| 사용자 경로 | 서버가 헤더 등급을 회차의 `eligible_grades_mask` 와 **대조**한다 |

서명 없는 역할 클레임(`hasRole`)은 방어가 아니라 장식이므로 넣지 않는다.
JWT · 세션 · Spring Security 도 도입하지 않는다.

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
