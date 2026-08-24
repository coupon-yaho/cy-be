package com.kafkick.storage.db.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * benchmark_runs 의 DB 제약을 실제 MySQL 에 마이그레이션을 적용한 뒤 검증한다.
 *
 * <p>여기서 막는 값 대부분은 {@code BenchmarkRunService} 를 지나면 만들어지지 않는다. 이 제약이
 * 지키는 건 그 코드를 지나지 않는 경로다 — 회차 백필 · 수동 보정 · 나중에 붙을 다른 모듈.
 * <b>측정은 되돌릴 수 없어서</b> 잘못된 회차 행은 "고치면 되는 데이터" 가 아니라 그 회차의 손실이다.
 *
 * <p>Spring 컨텍스트를 띄우지 않는 이유는 {@code IssueAttemptsMigrationTest} 와 같다 — storage 에
 * {@code @SpringBootConfiguration} 이 없고 엔티티가 0개라 @DataJpaTest 는 "JPA metamodel must not
 * be empty" 로 죽는다. 검증 대상이 스키마뿐이므로 JDBC 로 충분하다.
 */
class BenchmarkRunsMigrationTest {

    /** testFixtures 의 MySqlContainerConfig 는 latest 를 쓴다. 여기서는 운영(compose)과 같은 8.4 로 고정한다. */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4");

    /** uk_run_key 가 유니크라 행마다 키가 달라야 서로 안 부딪힌다. 테스트 행을 가르는 일련번호다. */
    private static final AtomicLong ROW_SEQUENCE = new AtomicLong(1);

    private static MySQLContainer mysql;

    @BeforeAll
    static void startAndMigrate() {
        mysql = new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                // 제약 판정에 영향을 주는 서버 설정만 운영과 맞춘다.
                // STRICT_TRANS_TABLES 가 없으면 길이 초과가 예외가 아니라 조용한 절단이 된다.
                .withCommand(
                        "--default-time-zone=+00:00",
                        "--character-set-server=utf8mb4",
                        "--collation-server=utf8mb4_0900_ai_ci",
                        "--default-storage-engine=InnoDB",
                        "--sql-mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
                                + "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
        mysql.start();

        Flyway.configure()
                .dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @AfterAll
    static void stop() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @AfterEach
    void clear() throws SQLException {
        try (Connection c = connection(); Statement s = c.createStatement()) {
            s.executeUpdate("DELETE FROM benchmark_runs");
        }
    }

    @Nested
    @DisplayName("마이그레이션 적용")
    class Migration {

        /**
         * 적용된 마이그레이션 목록 전체를 고정하지 않는다 — OBS-22 의 V2026082003 처럼 이 테이블과
         * 무관한 마이그레이션이 늘어도 깨지면 안 된다.
         */
        @Test
        @DisplayName("V2026082001 뒤에 성공으로 기록된다")
        void appliedAfterIssueAttempts() throws SQLException {
            assertThat(appliedVersions()).contains("2026082001", "2026082002");
            assertThat(installedRank("2026082002")).isGreaterThan(installedRank("2026082001"));
            assertThat(query("SELECT success FROM flyway_schema_history WHERE version = '2026082002'"))
                    .isEqualTo("1");
        }

        @Test
        @DisplayName("archive fencing token을 포함한 컬럼 51개와 CHECK 15종이 만들어진다")
        void schemaShape() throws SQLException {
            assertThat(query("SELECT COUNT(*) FROM information_schema.columns"
                    + " WHERE table_schema = DATABASE() AND table_name = 'benchmark_runs'"))
                    .isEqualTo("51");
            assertThat(query("SELECT COUNT(*) FROM information_schema.check_constraints"
                    + " WHERE constraint_schema = DATABASE() AND constraint_name LIKE 'ck_run%'"))
                    .isEqualTo("15");
        }

        /**
         * running_guard 는 저장되지 않는 가상 컬럼이어야 한다. STORED 로 바뀌면 행마다 바이트를
         * 쓰고 ALTER 가 테이블을 다시 쓰는데, 이 컬럼은 인덱스에만 필요하다.
         */
        @Test
        @DisplayName("running_guard 는 VIRTUAL 생성 컬럼이다")
        void runningGuardIsVirtual() throws SQLException {
            assertThat(query("SELECT extra FROM information_schema.columns"
                    + " WHERE table_schema = DATABASE() AND table_name = 'benchmark_runs'"
                    + " AND column_name = 'running_guard'"))
                    .isEqualTo("VIRTUAL GENERATED");
        }

        /**
         * coupon_id 에 FK 가 없어야 한다. 회차 기록은 쿠폰보다 오래 살아야 하는데, FK 는 측정용
         * 쿠폰을 정리할 때 회차 행을 함께 지우거나(CASCADE) 정리를 막는다(RESTRICT). 둘 다 틀렸다.
         */
        @Test
        @DisplayName("외래키가 하나도 없다")
        void hasNoForeignKey() throws SQLException {
            assertThat(query("SELECT COUNT(*) FROM information_schema.table_constraints"
                    + " WHERE constraint_schema = DATABASE() AND table_name = 'benchmark_runs'"
                    + " AND constraint_type = 'FOREIGN KEY'"))
                    .isEqualTo("0");
        }
    }

    @Nested
    @DisplayName("동시 회차 방어")
    class Concurrency {

        @Test
        @DisplayName("RUNNING 인 회차는 동시에 하나뿐이다")
        void onlyOneRunningAtATime() {
            assertThatCode(() -> insert(Map.of())).doesNotThrowAnyException();

            // 두 회차가 동시에 돌면 이벤트의 benchmark_run_id 가 섞이고, 섞인 회차는 사후에 분리할 수 없다.
            assertThatThrownBy(() -> insert(Map.of()))
                    .hasMessageContaining("uk_run_running");
        }

        /**
         * 끝난 회차끼리는 부딪히지 않아야 한다. 가상 컬럼이 NULL 을 내고 유니크 인덱스가 NULL 을
         * 여러 개 허용하는 것에 이 방어 전체가 걸려 있다 — 여기가 깨지면 두 번째 회차부터 못 연다.
         */
        @Test
        @DisplayName("끝난 회차는 몇 개든 공존한다")
        void finishedRunsCoexist() {
            assertThatCode(() -> {
                insert(loadStopped());
                insert(loadStopped());
                insert(loadStopped());
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("같은 회차 키로 두 번 열 수 없다")
        void runKeyIsUnique() {
            insert(Map.of("run_key", "'V3-MAIN-01'", "run_status", "'LOAD_STOPPED'",
                    "load_stopped_at", "'2026-08-22 00:00:01'"));
            assertThatThrownBy(() -> insert(Map.of("run_key", "'V3-MAIN-01'",
                    "run_status", "'LOAD_STOPPED'", "load_stopped_at", "'2026-08-22 00:00:01'")))
                    .hasMessageContaining("uk_run_key");
        }
    }

    @Nested
    @DisplayName("CHECK — 상태와 시각")
    class StatusAndTimes {

        @Test
        @DisplayName("상태에 없는 시각을 채울 수 없다")
        void runningCannotCarryStopTime() {
            assertThatThrownBy(() -> insert(Map.of("load_stopped_at", "'2026-08-22 00:00:01'")))
                    .hasMessageContaining("ck_run_status_times");
        }

        @Test
        @DisplayName("상태가 요구하는 시각을 빠뜨릴 수 없다")
        void loadStoppedRequiresItsTime() {
            assertThatThrownBy(() -> insert(Map.of("run_status", "'LOAD_STOPPED'")))
                    .hasMessageContaining("ck_run_status_times");
        }

        /**
         * 부하 종료와 관측 종료를 한 시각으로 합치는 것을 스키마가 막는다 — 합치면 v3 의
         * 수렴 구간이 잘리는데, 그 구간이 v3 의 핵심 증거다.
         */
        @Test
        @DisplayName("관측 종료만 있고 부하 종료가 없는 회차를 만들 수 없다")
        void observationStopRequiresLoadStop() {
            assertThatThrownBy(() -> insert(Map.of(
                    "run_status", "'OBSERVED'",
                    "observation_stopped_at", "'2026-08-22 00:01:00'",
                    "observed_lag_total", "0")))
                    .hasMessageContaining("ck_run_status_times");
        }

        @Test
        @DisplayName("관측 종료가 부하 종료보다 빠를 수 없다")
        void observationStopCannotPrecedeLoadStop() {
            assertThatThrownBy(() -> insert(Map.of(
                    "run_status", "'OBSERVED'",
                    "load_stopped_at", "'2026-08-22 00:00:05'",
                    "observation_stopped_at", "'2026-08-22 00:00:01'",
                    "observed_lag_total", "0")))
                    .hasMessageContaining("ck_run_time_order");
        }

        /**
         * 부하 종료와 관측 종료가 <b>각각</b> 상태에 묶여 있는지 본다. 부하만 끝난 회차가 관측
         * 종료 시각을 갖고 있으면 두 시각이 사실상 하나로 합쳐진 것이고, 그때 v3 의 수렴 구간은
         * 시계열에서 사라진다 — 값은 채워져 있는데 가리키는 구간이 없는 형태라 눈으로 안 보인다.
         */
        @Test
        @DisplayName("부하만 끝난 회차가 관측 종료 시각을 가질 수 없다")
        void loadStoppedCannotCarryObservationStop() {
            Map<String, String> row = new LinkedHashMap<>(loadStopped());
            row.put("observation_stopped_at", "'2026-08-22 00:01:05'");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_status_times");
        }

        @Test
        @DisplayName("관측이 끝났다면서 관측 종료 시각이 없을 수 없다")
        void observedRequiresItsTime() {
            Map<String, String> row = new LinkedHashMap<>(loadStopped());
            row.put("run_status", "'OBSERVED'");
            row.put("observed_lag_total", "0");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_status_times");
        }

        /**
         * v1 · v2 는 부하 종료와 관측 종료가 사실상 동시다. 순서를 엄격하게 잡으면 정상 회차가
         * 거부된다 — "v3 는 뒤의 둘이 실제로 벌어져야 한다" 는 회차 운용의 문제이지 스키마가 아니다.
         */
        @Test
        @DisplayName("두 종료 시각이 같은 회차는 정상이다")
        void simultaneousStopsAreAllowed() {
            assertThatCode(() -> insert(observed("'2026-08-22 00:00:05'", "'2026-08-22 00:00:05'")))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("CHECK — FINAL 진입 게이트")
    class FinalGate {

        @Test
        @DisplayName("백로그가 남은 회차는 관측 종료로 넘어갈 수 없다")
        void backlogMustBeDrained() {
            assertThatThrownBy(() -> insert(Map.of(
                    "run_status", "'OBSERVED'",
                    "load_stopped_at", "'2026-08-22 00:00:05'",
                    "observation_stopped_at", "'2026-08-22 00:01:05'",
                    "observed_lag_total", "5")))
                    .hasMessageContaining("ck_run_lag_gate");
        }

        /**
         * MySQL 의 CHECK 는 NULL 이면 통과시킨다 — FALSE 일 때만 거부한다. 게이트 조건에
         * IS NOT NULL 을 명시하지 않으면 "백로그를 재지도 않은 회차" 가 그대로 통과하는데,
         * 그건 lag=5 인 회차보다 나쁘다(재지 않았다는 사실조차 안 남는다).
         * issue_attempts 에서 실제로 밟아 CY-253 리뷰에서 고친 함정이다.
         */
        @Test
        @DisplayName("백로그를 재지 않은 회차도 관측 종료로 넘어갈 수 없다")
        void unmeasuredBacklogIsAlsoRejected() {
            assertThatThrownBy(() -> insert(Map.of(
                    "run_status", "'OBSERVED'",
                    "load_stopped_at", "'2026-08-22 00:00:05'",
                    "observation_stopped_at", "'2026-08-22 00:01:05'")))
                    .hasMessageContaining("ck_run_lag_gate");
        }

        @Test
        @DisplayName("공식 결과 없이 회차를 확정할 수 없다")
        void finalizeRequiresOfficialSummary() {
            Map<String, String> row = new LinkedHashMap<>(observed(
                    "'2026-08-22 00:00:05'", "'2026-08-22 00:01:05'"));
            row.put("run_status", "'FINALIZED'");
            row.put("finalized_at", "'2026-08-22 00:02:00'");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_final_summary");
        }
    }

    @Nested
    @DisplayName("CHECK — 공식 요약")
    class Summaries {

        /**
         * 반쯤 채워진 요약은 "아직 안 올라왔다" 와 "이 값은 못 쟀다" 가 같은 모양이 되어
         * 비교표에서 구분되지 않는다.
         */
        @Test
        @DisplayName("요약을 반만 채울 수 없다")
        void summaryIsAllOrNothing() {
            assertThatThrownBy(() -> insert(Map.of("client_p99_millis", "412.3")))
                    .hasMessageContaining("ck_run_client_summary_atomic");
        }

        @Test
        @DisplayName("p99 가 p95 보다 작을 수 없다")
        void tailCannotInvert() {
            Map<String, String> row = new LinkedHashMap<>(clientSummary());
            row.put("client_p95_millis", "412.3");
            row.put("client_p99_millis", "120.0");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_client_summary_range");
        }

        @Test
        @DisplayName("실패 수가 요청 수를 넘을 수 없다")
        void failuresCannotExceedRequests() {
            Map<String, String> row = new LinkedHashMap<>(clientSummary());
            row.put("client_request_count", "100");
            row.put("client_failure_count", "101");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_client_summary_range");
        }

        /**
         * server_* 는 client_* 와 <b>따로</b> 검증된다. 한쪽 제약이 다른 쪽까지 덮고 있으면
         * 나중에 한쪽만 고칠 때 반대쪽이 조용히 풀린다.
         */
        @Test
        @DisplayName("server 요약도 같은 규칙을 각자 받는다")
        void serverSummaryHasItsOwnConstraint() {
            assertThatThrownBy(() -> insert(Map.of("server_tps", "10.0")))
                    .hasMessageContaining("ck_run_server_summary_atomic");
        }

        /**
         * 도착률이 목표 아래로 떨어진 회차도 <b>저장은 된다.</b> 그 사실 자체가 회차의 결과이고,
         * 비교표에 넣을지는 읽는 쪽이 정한다. 스키마가 거부하면 "왜 그 회차가 없는지" 가 안 남는다.
         */
        @Test
        @DisplayName("dropped_iterations 가 0 이 아닌 회차도 기록된다")
        void droppedIterationsAreRecordedNotRejected() {
            Map<String, String> row = new LinkedHashMap<>(clientSummary());
            row.put("client_dropped_iterations", "1274");
            assertThatCode(() -> insert(row)).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("CHECK — archive 는 회차와 독립이다")
    class Archive {

        @Test
        @DisplayName("IN_PROGRESS 는 claim 시각과 token을 모두 요구한다")
        void inProgressRequiresCompleteClaimIdentity() throws SQLException {
            insert(finalized());

            assertThatThrownBy(() -> executeUpdate(
                "UPDATE benchmark_runs SET archive_status='IN_PROGRESS',"
                    + " archive_claimed_at=NOW(6), archive_claim_token=NULL"))
                .hasMessageContaining("ck_run_archive_claim");
        }

        @Test
        @DisplayName("claim 시각과 token을 모두 채운 IN_PROGRESS는 허용된다")
        void inProgressWithCompleteClaimIdentityIsAccepted() throws SQLException {
            insert(finalized());

            assertThatCode(() -> executeUpdate(
                "UPDATE benchmark_runs SET archive_status='IN_PROGRESS',"
                    + " archive_failure_reason=NULL, archive_claimed_at=NOW(6),"
                    + " archive_claim_token='00000000-0000-4000-8000-000000000001'"))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("IN_PROGRESS 밖의 상태에는 claim 식별자가 남을 수 없다")
        void completedStatusCannotCarryClaimIdentity() throws SQLException {
            insert(finalized());

            assertThatThrownBy(() -> executeUpdate(
                "UPDATE benchmark_runs SET archive_status='DONE',"
                    + " archive_claimed_at=NOW(6),"
                    + " archive_claim_token='00000000-0000-4000-8000-000000000001'"))
                .hasMessageContaining("ck_run_archive_claim");
        }

        @Test
        @DisplayName("실패 이유 없는 FAILED 를 만들 수 없다")
        void failedRequiresReason() {
            assertThatThrownBy(() -> insert(Map.of("archive_status", "'FAILED'")))
                    .hasMessageContaining("ck_run_archive_reason");
        }

        /**
         * {@code IS NOT NULL} 만으로는 부족하다 — {@code ''} 도 NOT NULL 이라 통과한다.
         * 이유 없는 FAILED 를 막으려던 제약이 정확히 그걸로 뚫린다.
         */
        @ParameterizedTest(name = "이유 = [{0}]")
        @CsvSource(value = { "''", "'   '", "'\\t'", "'\\n'", "'\\t\\n '" }, quoteCharacter = '#')
        @DisplayName("빈 실패 이유는 이유가 없는 것이다")
        void blankReasonIsRejected(String reason) {
            assertThatThrownBy(() -> insert(Map.of(
                    "archive_status", "'FAILED'", "archive_failure_reason", reason)))
                    .hasMessageContaining("ck_run_archive_reason");
        }

        @Test
        @DisplayName("성공한 archive 에 실패 이유가 남아 있을 수 없다")
        void doneCannotCarryReason() {
            assertThatThrownBy(() -> insert(Map.of(
                    "archive_status", "'DONE'", "archive_failure_reason", "'timeout'")))
                    .hasMessageContaining("ck_run_archive_reason");
        }

        /**
         * archive 가 실패해도 회차는 확정 상태로 남는다. 하나의 상태로 합쳤다면 표현할 수 없는
         * 조합이고, 이 조합이 곧 OBS-22 의 재실행 조건이다.
         */
        @Test
        @DisplayName("확정된 회차의 archive 만 실패한 상태를 표현할 수 있다")
        void finalizedRunCanHaveFailedArchive() {
            Map<String, String> row = new LinkedHashMap<>(finalized());
            row.put("archive_status", "'FAILED'");
            row.put("archive_failure_reason", "'prometheus query_range timeout'");
            assertThatCode(() -> insert(row)).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("CHECK — 조건 값")
    class Conditions {

        @Test
        @DisplayName("도착률이 0 인 회차를 만들 수 없다")
        void offeredRpsMustBePositive() {
            assertThatThrownBy(() -> insert(Map.of("offered_rps", "0")))
                    .hasMessageContaining("ck_run_load_input");
        }

        @Test
        @DisplayName("코어 수를 0 으로 기록할 수 없다")
        void availableProcessorsMustBePositive() {
            assertThatThrownBy(() -> insert(Map.of("available_processors", "0")))
                    .hasMessageContaining("ck_run_topology");
        }

        /**
         * compose 에 cpus 제한이 한 줄도 없어(실측) 지금은 총량을 모를 수 있다. 모르는 것과
         * 0 인 것은 다르다 — NULL 은 통과하고 0 은 거부되어야 한다.
         */
        @Test
        @DisplayName("모르는 자원 총량은 NULL 로 남길 수 있다")
        void unknownTotalsStayNull() {
            Map<String, String> unknown = new LinkedHashMap<>(loadStopped());
            unknown.put("cpu_millicores_total", "NULL");
            assertThatCode(() -> insert(unknown)).doesNotThrowAnyException();

            Map<String, String> zero = new LinkedHashMap<>(loadStopped());
            zero.put("cpu_millicores_total", "0");
            assertThatThrownBy(() -> insert(zero)).hasMessageContaining("ck_run_topology");
        }

        /**
         * 테이블 콜레이션이 utf8mb4_0900_ai_ci 라 IN 비교가 <b>대소문자를 무시한다.</b>
         * COLLATE 를 안 붙이면 'running' 이 통과해 소문자 그대로 저장되고, 읽는 쪽의
         * BenchmarkRunStatus.valueOf 가 "No enum constant ... running" 으로 죽는다 —
         * 값은 들어가는데 읽을 때 죽는 행이다. 실측으로 확인하고 as_cs 를 붙였다.
         */
        @ParameterizedTest(name = "{0} = {1}")
        @CsvSource({
                "run_status,     running, ck_run_status",
                "run_status,     Running, ck_run_status",
                "run_type,       main,    ck_run_type",
                "archive_status, done,    ck_run_archive_status",
                "archive_status, None,    ck_run_archive_status",
        })
        @DisplayName("대소문자가 다른 상태 값도 거부한다")
        void statusValuesAreCaseSensitive(String column, String value, String constraint) {
            // 작은따옴표는 @CsvSource 의 quote 문자라 여기서 붙인다. CSV 에 적으면 벗겨져서
            // SQL 이 문자열이 아니라 컬럼 참조가 되고(Unknown column), 제약이 아니라 문법에서 죽는다.
            assertThatThrownBy(() -> insert(Map.of(column, "'" + value + "'")))
                    .hasMessageContaining(constraint);
        }

        @Test
        @DisplayName("정의에 없는 상태 값을 쓸 수 없다")
        void statusValuesAreClosed() {
            assertThatThrownBy(() -> insert(Map.of("run_status", "'STOPPING'")))
                    .hasMessageContaining("ck_run_status");
            assertThatThrownBy(() -> insert(Map.of("archive_status", "'PENDING'")))
                    .hasMessageContaining("ck_run_archive_status");
            assertThatThrownBy(() -> insert(Map.of("run_type", "'SMOKE'")))
                    .hasMessageContaining("ck_run_type");
        }
    }

    @Nested
    @DisplayName("CHECK — 절 하나하나가 실제로 거부한다")
    class EveryClause {

        /**
         * 제약은 AND 로 묶인 절의 묶음이라, 한 절만 지워도 나머지가 통과시켜 준다. 절마다 거부
         * 사례가 없으면 그 절은 <b>한 번도 돈 적이 없는 가드</b>다 — 초록불이 "검사했다" 로 읽히는
         * 만큼 없느니만 못하다. 여기서 절 하나에 사례 하나씩 붙인다.
         */
        @ParameterizedTest(name = "{0} = {1} → {2}")
        @CsvSource({
                "app_replicas,            0,  ck_run_topology",
                "available_processors,    0,  ck_run_topology",
                "tomcat_workers_total,    0,  ck_run_topology",
                "hikari_pool_total,       0,  ck_run_topology",
                "mysql_max_connections,   0,  ck_run_topology",
                "cpu_millicores_total,    0,  ck_run_topology",
                "memory_mb_total,         0,  ck_run_topology",
                "tomcat_max_connections,  0,  ck_run_topology",
                "tomcat_accept_count,     -1, ck_run_topology",
                "offered_rps,             0,  ck_run_load_input",
                "load_hold_seconds,       0,  ck_run_load_input",
                "observation_hold_seconds, -1, ck_run_load_input",
                "stock_total,             -1, ck_run_load_input",
                "generator_idle_rtt_millis, -0.001, ck_run_load_input",
        })
        @DisplayName("조건 값의 절")
        void conditionClauses(String column, String value, String constraint) {
            assertThatThrownBy(() -> insert(Map.of(column, value)))
                    .hasMessageContaining(constraint);
        }

        /**
         * server_* 는 client_* 와 <b>같은 규칙을 각자</b> 받는다. 한쪽만 검증하면 다른 쪽 제약이
         * 나중에 느슨해져도 아무도 모른다 — 그 순간 비교표의 절반이 검증 없이 흘러간다.
         */
        @ParameterizedTest(name = "{0} = {1}")
        @CsvSource({
                "client_request_count,      -1",
                "client_failure_count,      -1",
                "client_dropped_iterations, -1",
                "client_tps,                -0.001",
                "client_p95_millis,         -0.001",
                "client_p99_millis,         -0.001",
        })
        @DisplayName("client 요약 값의 범위 절")
        void clientRangeClauses(String column, String value) {
            Map<String, String> row = new LinkedHashMap<>(clientSummary());
            row.put(column, value);
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_client_summary_range");
        }

        @ParameterizedTest(name = "{0} = {1}")
        @CsvSource({
                "server_request_count, -1",
                "server_failure_count, -1",
                "server_tps,           -0.001",
                "server_p95_millis,    -0.001",
                "server_p99_millis,    -0.001",
        })
        @DisplayName("server 요약 값의 범위 절")
        void serverRangeClauses(String column, String value) {
            Map<String, String> row = new LinkedHashMap<>(serverSummary());
            row.put(column, value);
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_server_summary_range");
        }

        @Test
        @DisplayName("server 도 p99 가 p95 보다 작을 수 없고 실패가 요청을 넘을 수 없다")
        void serverRangeKeepsTailAndCounts() {
            Map<String, String> inverted = new LinkedHashMap<>(serverSummary());
            inverted.put("server_p95_millis", "180.750");
            inverted.put("server_p99_millis", "90.500");
            assertThatThrownBy(() -> insert(inverted))
                    .hasMessageContaining("ck_run_server_summary_range");

            Map<String, String> tooManyFailures = new LinkedHashMap<>(serverSummary());
            tooManyFailures.put("server_request_count", "100");
            tooManyFailures.put("server_failure_count", "101");
            assertThatThrownBy(() -> insert(tooManyFailures))
                    .hasMessageContaining("ck_run_server_summary_range");
        }

        /** 요약 여섯(server 는 다섯) 값 중 어느 하나만 빠져도 거부돼야 한다. */
        @ParameterizedTest(name = "{0} 만 비면 거부")
        @CsvSource({
                "client_request_count", "client_failure_count", "client_dropped_iterations",
                "client_tps", "client_p95_millis", "client_p99_millis", "client_measured_at",
        })
        @DisplayName("client 요약의 통짜 절")
        void clientAtomicClauses(String column) {
            Map<String, String> row = new LinkedHashMap<>(clientSummary());
            row.put(column, "NULL");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_client_summary_atomic");
        }

        @ParameterizedTest(name = "{0} 만 비면 거부")
        @CsvSource({
                "server_request_count", "server_failure_count",
                "server_tps", "server_p95_millis", "server_p99_millis", "server_measured_at",
        })
        @DisplayName("server 요약의 통짜 절")
        void serverAtomicClauses(String column) {
            Map<String, String> row = new LinkedHashMap<>(serverSummary());
            row.put(column, "NULL");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_server_summary_atomic");
        }

        @Test
        @DisplayName("부하 종료가 회차 시작보다 빠를 수 없다")
        void loadStopCannotPrecedeStart() {
            Map<String, String> row = new LinkedHashMap<>(loadStopped());
            row.put("load_stopped_at", "'2026-08-21 23:59:59'");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_time_order");
        }

        @Test
        @DisplayName("확정이 관측 종료보다 빠를 수 없다")
        void finalizeCannotPrecedeObservationStop() {
            Map<String, String> row = new LinkedHashMap<>(finalized());
            row.put("finalized_at", "'2026-08-22 00:00:30'");
            assertThatThrownBy(() -> insert(row)).hasMessageContaining("ck_run_time_order");
        }
    }

    @Test
    @DisplayName("정상 회차 한 건이 끝까지 들어간다")
    void fullLifecycleRowIsAccepted() throws SQLException {
        assertThatCode(() -> insert(finalized())).doesNotThrowAnyException();
        assertThat(query("SELECT COUNT(*) FROM benchmark_runs WHERE run_status = 'FINALIZED'"))
                .isEqualTo("1");
    }

    // ── 행 만들기 ────────────────────────────────────────────────────────────
    //
    // 기본값은 "막 시작한 정상 회차" 다. 각 테스트는 검증하려는 컬럼만 덮어써서, 무엇을 깨뜨렸는지가
    // 테스트 본문에 그대로 보이게 한다.

    private static Map<String, String> defaults() {
        Map<String, String> row = new LinkedHashMap<>();
        long sequence = ROW_SEQUENCE.getAndIncrement();
        row.put("run_key", "'RUN-" + sequence + "'");
        row.put("run_type", "'MAIN'");
        row.put("scenario_code", "'LOAD_100K'");
        row.put("engine_version", "'V3'");
        row.put("release_stage", "'V3'");
        row.put("queue_mode", "'ADAPTIVE'");
        // 없는 쿠폰이다. FK 가 없다는 것을 정상 경로에서도 함께 지킨다.
        row.put("coupon_id", "999999");
        row.put("run_status", "'RUNNING'");
        row.put("archive_status", "'NONE'");
        row.put("started_at", "'2026-08-22 00:00:00'");
        row.put("requested_by", "'tester'");
        row.put("app_replicas", "3");
        row.put("available_processors", "6");
        row.put("tomcat_workers_total", "180");
        row.put("hikari_pool_total", "36");
        row.put("mysql_max_connections", "50");
        row.put("offered_rps", "20000");
        row.put("load_hold_seconds", "5");
        row.put("observation_hold_seconds", "60");
        return row;
    }

    private static Map<String, String> loadStopped() {
        Map<String, String> row = defaults();
        row.put("run_status", "'LOAD_STOPPED'");
        row.put("load_stopped_at", "'2026-08-22 00:00:05'");
        return row;
    }

    private static Map<String, String> observed(String loadStoppedAt, String observationStoppedAt) {
        Map<String, String> row = defaults();
        row.put("run_status", "'OBSERVED'");
        row.put("load_stopped_at", loadStoppedAt);
        row.put("observation_stopped_at", observationStoppedAt);
        row.put("observed_lag_total", "0");
        return row;
    }

    private static Map<String, String> clientSummary() {
        Map<String, String> row = defaults();
        row.put("client_request_count", "100000");
        row.put("client_failure_count", "0");
        row.put("client_dropped_iterations", "0");
        row.put("client_tps", "18472.500");
        row.put("client_p95_millis", "206.100");
        row.put("client_p99_millis", "412.300");
        row.put("client_measured_at", "'2026-08-22 00:01:10'");
        return row;
    }

    private static Map<String, String> serverSummary() {
        Map<String, String> row = defaults();
        row.put("server_request_count", "99998");
        row.put("server_failure_count", "11");
        row.put("server_tps", "18500.250");
        row.put("server_p95_millis", "90.500");
        row.put("server_p99_millis", "180.750");
        row.put("server_measured_at", "'2026-08-22 00:01:11'");
        return row;
    }

    private static Map<String, String> finalized() {
        Map<String, String> row = new LinkedHashMap<>(clientSummary());
        row.put("run_status", "'FINALIZED'");
        row.put("load_stopped_at", "'2026-08-22 00:00:05'");
        row.put("observation_stopped_at", "'2026-08-22 00:01:05'");
        row.put("finalized_at", "'2026-08-22 00:02:00'");
        row.put("observed_lag_total", "0");
        return row;
    }

    /** 기본 행에 덮어쓸 값을 얹어 원시 INSERT 를 날린다. */
    private static void insert(Map<String, String> overrides) {
        Map<String, String> row = new LinkedHashMap<>(defaults());
        row.putAll(overrides);
        String columns = row.keySet().stream().map(it -> "`" + it + "`")
                .collect(Collectors.joining(", "));
        String values = String.join(", ", row.values());
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "INSERT INTO benchmark_runs (" + columns + ") VALUES (" + values + ")");
        } catch (SQLException exception) {
            throw new IllegalStateException(exception.getMessage(), exception);
        }
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    private static void executeUpdate(String sql) {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException exception) {
            throw new IllegalStateException(exception.getMessage(), exception);
        }
    }

    private static String query(String sql) throws SQLException {
        try (Connection connection = connection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            return resultSet.next() ? resultSet.getString(1) : null;
        }
    }

    private static java.util.List<String> appliedVersions() throws SQLException {
        java.util.List<String> versions = new java.util.ArrayList<>();
        try (Connection connection = connection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT version FROM flyway_schema_history WHERE version IS NOT NULL")) {
            while (resultSet.next()) {
                versions.add(resultSet.getString(1));
            }
        }
        return versions;
    }

    private static int installedRank(String version) throws SQLException {
        return Integer.parseInt(query(
                "SELECT installed_rank FROM flyway_schema_history WHERE version = '" + version + "'"));
    }
}
