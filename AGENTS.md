# AGENTS.md

이 저장소에서 작업할 때 **코드만 봐서는 알 수 없는 것**을 적는다.
구조·규약은 `README.md` 와 `docs/` 가 갖고, 여기는 **미결 항목과 그 근거**만 둔다.

각 항목은 이렇게 적는다 — 무엇이 문제인지, **왜 아직 안 고쳤는지**, 고칠 때 무엇을 밟게 되는지.
"왜 안 고쳤나" 가 없으면 다음 사람이 같은 조사를 처음부터 다시 한다.

---

## 미결 — 관측(observation)

### A. 관측 계정을 양성 목록으로 재부여한다 — **[OBS-36] 해소**

**해소된 상태** — 관측 전용 계정은 이제 스키마 단위 권한을 갖지 않는다. 양성 목록의 테이블만
읽는다. 목록의 정본은 `infra/mysql/obs-grants/allowlist.txt` 이고, `apply.sh` 가 그것을 GRANT 로
옮긴다. compose 의 `--profile obs-grants` 일회성 서비스와 테스트 픽스처(`MySqlContainerConfig`)가
**같은 파일**을 돌린다.

계정 생성은 여전히 `infra/mysql/initdb/20-obs-account.sh` 가 하지만 **권한은 주지 않는다.**
두 자리로 나뉜 이유는 initdb 가 Flyway 보다 먼저 돌아 그 시점엔 테이블이 없기 때문이다
(테이블 단위로 적으면 `ERROR 1146` 으로 컨테이너가 안 뜬다 — 실측). 그리고 스키마 GRANT 위에
테이블 REVOKE 를 얹을 수도 없다(`ERROR 1147` — 실측). 그 두 실측이 지금 구조의 근거다.

`batch` 의 `ObservationAccountPrivilegeTest` 가 실제 MySQL 컨테이너에서 다섯 가지를 단언한다 —
`members` 는 1142 로 막히는 것, 목록의 테이블은 읽히는 것, 관측 질의의 테이블이 전부 목록에
있는 것, 목록의 테이블을 전부 누군가 읽는 것, 그리고 `.env.example` 이 스키마 단위 GRANT 를
권하지 않는 것.

**남은 약점** — 이 절차는 사람이 한 번 쳐야 한다. 신규 클론은 안 치면 **관측 풀이 커넥션
자체를 못 연다**(실측: `ERROR 1044 Access denied for user 'obs'@'%' to database 'app'` — 질의가
아니라 접속 단계다. JDBC 드라이버로도 같은 코드를 확인했다). 시끄러워서 금방 드러난다.
반대로 **OBS-36 이전에 만든 볼륨**은 예전 스키마 GRANT 가 남아 아무 증상이 없다.
기동 자기 진단은 두지 않기로 했다(일회성 이행 상태를 잡으려고 상시 프로브를 남기는 값이 안
맞는다). 확인 명령은 README 의 "관측 계정 권한 재부여" 절에 있다.

**함께 남은 것** — `storage` 의 `ObservationQueryScopeTest` 는 그대로 둔다. 계정 권한이 이제
DB 계층 방어선이 됐지만, 소스 스캔은 그보다 **먼저** (CI 에서, 배포 전에) 잡는다.

---

## 미결 — storage

### B. 운영 풀 `JdbcTemplate` 에 쿼리 타임아웃이 없다

`storage/db/config/MainDataSourceConfig` 의 `jdbcTemplate` 은 `spring.jdbc.template.query-timeout` 을 적용하지만 그 값이 설정에 없다. 관측 풀에는 두 겹으로 있다 — `observation.datasource.query-timeout: 3s` 와 서버 쪽 `connection-init-sql: SET SESSION max_execution_time = 3000`.

**CY-338 이 만든 문제가 아니다.** 다만 그 티켓이 운영 풀 정의를 `ObservationDataSourceConfig` 에서 `MainDataSourceConfig` 로 옮기면서 **조건 없이 항상 붙게** 했으므로 이 빈이 도는 범위가 넓어졌다.

무한 대기를 막는 손잡이가 운영 풀에만 없다.

---

## 미결 — api

### C. admin API 의 역할 헤더가 서명 없이 위조 가능하다

`api/admin/support/AdminAuthorizationInterceptor` 가 `/api/v1/admin/**` 에서 `X-User-Role: ADMIN` 만 확인한다. 서명이 없어 클라이언트가 그대로 보내면 통과한다. `Caller`(`X-Member-Id`)도 같은 성질이고, 그 레코드 javadoc 이 스스로 *"이 값을 권한 판정에 쓰지 않는다"* 고 적어 두었다.

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
