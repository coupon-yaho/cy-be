# AGENTS.md

이 저장소에서 작업할 때 **코드만 봐서는 알 수 없는 것**을 적는다.
구조·규약은 `README.md` 와 `docs/` 가 갖고, 여기는 **미결 항목과 그 근거**만 둔다.

각 항목은 이렇게 적는다 — 무엇이 문제인지, **왜 아직 안 고쳤는지**, 고칠 때 무엇을 밟게 되는지.
"왜 안 고쳤나" 가 없으면 다음 사람이 같은 조사를 처음부터 다시 한다.

---

## 미결 — 관측(observation)

### A. 관측 계정을 양성 목록으로 재부여한다

**지금 상태** — 관측 전용 계정이 스키마 단위 읽기 권한을 갖는다.

```sql
GRANT SELECT ON <스키마>.* TO '<obs>'@'%';
```

계정 생성은 `infra/mysql/initdb/20-obs-account.sh` 가 하고 **compose 와 테스트 컨테이너가 같은 파일**을 마운트한다.

**문제** — 그 권한에 `members` 가 포함된다. 관측 경로는 그 테이블을 읽을 이유가 없고 실제로 한 곳도 읽지 않는다. PII 컬럼은 암호화돼 있어(`name_enc`·`email_enc`·`phone_enc` 가 `varbinary`, AES-256-GCM) 평문은 안 나오지만, **HMAC 블라인드 인덱스**(`email_hash`·`phone_hash`)로 *알려진 이메일의 존재 확인*은 된다. `membership_grade`·`created_at` 은 평문이다.

`docs/PRD-v4.15.md` 가 `개인정보 반드시 마스킹` 을 Must 로 적고 있어 코드 계층 방어만으로 충분한지는 판단이 필요하다.

**왜 "그 테이블만 빼기" 가 안 되는가 — 실측**

```
GRANT SELECT ON app.* TO 'obs'@'%';
REVOKE SELECT ON app.members FROM 'obs'@'%';
→ ERROR 1147 (42000): There is no such grant defined for user 'obs' on host '%' on table 'members'
→ REVOKE 실패 후에도 obs 는 members 를 읽는다
```

MySQL 은 스키마 단위 GRANT 위에 테이블 단위 REVOKE 를 얹지 못한다.
**스키마 GRANT 를 걷고 필요한 테이블만 다시 주는 형태**가 유일한 길이다.

**왜 계정 생성 자리에서 못 하는가 — 실측**

`docker-entrypoint-initdb.d` 는 Flyway 보다 **먼저** 돈다. 그 시점에는 테이블이 하나도 없다.

```
GRANT SELECT ON app.verification_runs TO 'obs'@'%';
→ ERROR 1146 (42S02): Table 'app.verification_runs' doesn't exist
→ 컨테이너가 exit=1 로 아예 안 뜬다
```

즉 테이블 단위 권한은 **앱이 한 번 떠서 마이그레이션이 끝난 뒤**에만 줄 수 있다.

**부여 대상 (CY-338 시점 조사 — 시작할 때 다시 셀 것)**

관측 한정자(`@Qualifier("obs")`)를 쓰는 파일 10개가 질의하는 테이블은 6종이다.

| 읽는 곳 | 테이블 |
|---|---|
| 배치 실행 이력 조회 | `BATCH_JOB_EXECUTION` · `BATCH_JOB_INSTANCE` |
| 벤치마크 회차 조회 | `benchmark_runs` |
| 도메인 Gauge 수집 | `coupons` · `coupon_stocks` · `issuances` |

`members` 는 **한 곳도 읽지 않는다.**

**제안하는 형태** — compose 에 일회성 서비스를 둔다. 선례가 있다(`compose.yml` 의 `runtime-config-seed` 가 `profiles` 로 평소엔 안 뜨고 명시 실행).

```
docker compose --profile obs-grants run --rm obs-grants
```

**함께 필요한 것**

- **계약 테스트** — obs 가 `members` 를 못 읽는 것(MySQL 1142)과 위 6종은 읽는 것을 단언한다. `batch` 의 `ObservationAccountPrivilegeTest` 옆에 붙이면 된다
- **README 절** — 실행 시점(앱 기동 후)과 미실행 시 증상
- **미실행 감지** — 이 절차는 사람이 한 번 쳐야 하고 **안 쳐도 아무 일이 안 일어난다.** `runtime-config-seed` 가 지금 그 성질이라 같은 약점을 물려받는다

**지금 대신 세워 둔 방어선** — `storage` 의 `ObservationQueryScopeTest` 가 관측 한정자를 쓰는 소스의 질의문에 `members` 가 없는지 고정한다.

⚠️ **그것이 못 막는 것은 그 테스트 javadoc 에 네 가지로 적어 뒀다.** 계정 권한은 그대로이고, `.sql`/`.yml` 질의와 뷰 경유는 안 본다. **보안 경계가 아니라 개발 시점 회귀 그물이다.**

---

## 미결 — storage

### B. 운영 풀 `JdbcTemplate` 에 쿼리 타임아웃이 없다

`storage/db/config/MainDataSourceConfig` 의 `jdbcTemplate` 은 `spring.jdbc.template.query-timeout` 을 적용하지만 그 값이 설정에 없다. 관측 풀에는 두 겹으로 있다 — `observation.datasource.query-timeout: 3s` 와 서버 쪽 `connection-init-sql: SET SESSION max_execution_time = 3000`.

**CY-338 이 만든 문제가 아니다.** 다만 그 티켓이 운영 풀 정의를 `ObservationDataSourceConfig` 에서 `MainDataSourceConfig` 로 옮기면서 **조건 없이 항상 붙게** 했으므로 이 빈이 도는 범위가 넓어졌다.

무한 대기를 막는 손잡이가 운영 풀에만 없다.

---

## 미결 — api

### C. admin API 의 역할 헤더가 서명 없이 위조 가능하다

`api/admin/support/AdminAuthorizationInterceptor` 가 `/api/v1/admin/**` 에서 `X-User-Role: ADMIN` 만 확인한다. 서명이 없어 클라이언트가 그대로 보내면 통과한다. `Caller`(`X-User-Id`)도 같은 성질이고, 그 레코드 javadoc 이 스스로 *"이 값을 권한 판정에 쓰지 않는다"* 고 적어 두었다.

**CY-338 이 만든 문제가 아니다** — admin 엔드포인트 7종이 같은 문 뒤에 있고 CY-338 은 그중 하나(`GET /api/v1/admin/batch-executions`)를 추가했을 뿐이다.

다만 compose 가 api 의 업무 포트(8080)를 호스트에 노출하므로, 그 포트에서 admin 경로가 열려 있다는 사실은 함께 봐야 한다. 관리 포트(9090)는 노출하지 않는다 — 근거는 `compose.yml` 의 api 서비스 주석에 있다.

---

## 밟기 쉬운 함정 — 실측으로 확인된 것

새로 오는 사람이 같은 자리를 다시 밟지 않도록 적는다.

| 함정 | 무슨 일이 일어나나 |
|---|---|
| `@ConditionalOnBean` 을 컴포넌트 스캔되는 `@Configuration` 에 붙임 | 대상 빈 정의가 등록되기 전에 평가돼 **항상 false**. 설정이 통째로 안 붙는데 테스트는 전부 초록불이었다. 자동설정 클래스에서만 순서가 보장된다 |
| `initdb.d` 의 `.sh` 에 실행 비트가 없음 | `bad interpreter: Permission denied` → 컨테이너 `exit=126`. 클래스패스에서 복사하면 원본 모드가 안 따라온다 |
| 초기화 SQL 에서 백슬래시를 이스케이프 안 함 | MySQL 은 `NO_BACKSLASH_ESCAPES` 가 꺼진 기본값에서 `\` 를 이스케이프 문자로 읽는다. 비밀번호가 `\` 로 **끝나면** 닫는 따옴표가 escape 되어 `ERROR 1064` → **DB 가 아예 안 뜬다** |
| 미터 라벨에 `job` 을 씀 | Prometheus 가 붙이는 타깃 라벨과 겹쳐 `exported_job` 으로 개명당한다. 규칙이 아무 시계열과도 안 맞고 `absent()` 가 항상 참이 된다. `MeterRegistry` 단계만 보는 테스트로는 못 잡는다 |
| 마지막 성공 시각을 JVM 메모리에 보관 | 재기동에 새로 태어난다. 일 1회 배치라면 최대 하루가 "모름" 이고 그동안 알림이 데이터가 아니라 배포 시각을 보고 운다. DB 되읽기가 맞다 |
| 클래스패스 밖 파일을 읽는 테스트에 `inputs.files` 미선언 | Gradle 이 그 변경을 모르고 `UP-TO-DATE` 로 건너뛴다. 가드가 있는데 안 도는 상태가 가드가 없는 것보다 나쁘다 |
| 게이지 이름에 `_total` 을 붙임 | 카운터 규약이라 Micrometer 의 Prometheus 렌더러가 떼어 낸다. 코드가 부르는 이름과 관제가 보는 이름이 갈린다 |

---

## 작업 방식

- **주석은 코드가 실제로 보장하는 것만 말한다.** 못 지키면 TODO 와 후속 티켓으로 남긴다
- **프레임워크 동작은 추측하지 말고 실측한다.** 이 문서의 수치는 전부 실제로 돌려서 얻은 것이다
- **가드 테스트는 일부러 깨뜨려 빨간불을 확인한다.** 리뷰가 "위반 0건" 을 낸 뒤에도 안 잡히는 구멍이 실제로 있었다
- **방어를 추가하면 그 방어가 만드는 반대 방향 실패도 함께 적는다**
