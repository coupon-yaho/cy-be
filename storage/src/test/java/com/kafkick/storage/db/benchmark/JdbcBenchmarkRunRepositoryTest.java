package com.kafkick.storage.db.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.zaxxer.hikari.HikariDataSource;

import com.kafkick.core.benchmark.BenchmarkArchiveStatus;
import com.kafkick.core.benchmark.BenchmarkErrorCode;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunStatus;
import com.kafkick.core.benchmark.BenchmarkRunType;
import com.kafkick.core.benchmark.BenchmarkTopology;
import com.kafkick.core.benchmark.ClientLoadSummary;
import com.kafkick.core.benchmark.LoadProfile;
import com.kafkick.core.benchmark.LoadToolMeta;
import com.kafkick.core.benchmark.ServerLoadSummary;
import com.kafkick.core.benchmark.StartBenchmarkRunCommand;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 어댑터를 <b>실제 MySQL 앞에서</b> 돌린다.
 *
 * <p>{@code BenchmarkRunServiceTest} 는 인메모리 대역 위에서 규칙만 보고,
 * {@code BenchmarkRunsMigrationTest} 는 원시 INSERT 로 스키마만 본다. 그 둘 사이에 어댑터가
 * 통째로 빠져 있었다 — 조건부 UPDATE 의 WHERE 절 상태값을 오타 내거나 {@code ?} 바인딩 순서를
 * 뒤바꿔도 어느 테스트도 안 깨졌다. 여기가 그 자리다.
 *
 * <h2>관측 계정은 SELECT 만 할 수 있다</h2>
 *
 * 조회 템플릿을 <b>SELECT 전용 MySQL 계정</b>에 물린다. 어댑터가 두 템플릿을 맞바꿔 들면
 * 쓰기가 권한 오류로 즉시 터지므로, 이 클래스 전체가 풀 분리의 회귀 가드가 된다.
 *
 * <p>Hikari 의 {@code read-only} 플래그로도 막히는 것은 실측했지만 그쪽을 쓰지 않는다 —
 * {@code storage.yml} 이 적어 둔 대로 그건 세션 속성이지 권한이 아니라서, 운영에서 진짜로
 * 지켜 줄 것과 다른 것을 검증하게 된다.
 */
class JdbcBenchmarkRunRepositoryTest {

    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4");

    private static MySQLContainer mysql;
    private static HikariDataSource writeDataSource;
    private static HikariDataSource observationDataSource;
    private static JdbcTemplate writeJdbcTemplate;
    private static CountingJdbcTemplate observationJdbcTemplate;
    private static JdbcBenchmarkRunRepository repository;

    @BeforeAll
    static void startAndMigrate() {
        mysql = new MySQLContainer(IMAGE)
                .withDatabaseName("app")
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

        writeDataSource = hikari(mysql.getUsername(), mysql.getPassword());
        writeJdbcTemplate = new JdbcTemplate(writeDataSource);

        // 운영 예정 구성과 같게 SELECT 만 가진 계정을 만든다.
        try (HikariDataSource admin = hikari("root", mysql.getPassword())) {
            JdbcTemplate root = new JdbcTemplate(admin);
            root.execute("CREATE USER 'obs'@'%' IDENTIFIED BY 'obs'");
            root.execute("GRANT SELECT ON app.* TO 'obs'@'%'");
            root.execute("FLUSH PRIVILEGES");
        }
        observationDataSource = hikari("obs", "obs");
        observationJdbcTemplate = new CountingJdbcTemplate(observationDataSource);

        repository = new JdbcBenchmarkRunRepository(writeJdbcTemplate, observationJdbcTemplate);
    }

    @AfterAll
    static void stop() {
        if (writeDataSource != null) {
            writeDataSource.close();
        }
        if (observationDataSource != null) {
            observationDataSource.close();
        }
        if (mysql != null) {
            mysql.stop();
        }
    }

    @AfterEach
    void clear() {
        writeJdbcTemplate.update("DELETE FROM benchmark_runs");
    }

    @Nested
    @DisplayName("풀 분리")
    class Pools {

        /**
         * 어댑터가 두 템플릿을 맞바꿔 들면 쓰기가 권한 오류로 터진다. 이 클래스의 쓰기 테스트가
         * 전부 그 가드지만, 여기서는 <b>관측 계정이 정말 못 쓴다</b>는 전제 자체를 확인한다 —
         * 계정 설정이 조용히 느슨해지면 나머지 가드가 통째로 헛돈다.
         */
        @Test
        @DisplayName("관측 계정은 쓰기를 거부당한다")
        void observationAccountCannotWrite() {
            assertThatThrownBy(() -> observationJdbcTemplate.update(
                    "UPDATE benchmark_runs SET archive_status = 'DONE' WHERE id = 1"))
                    .rootCause()
                    .hasMessageContaining("UPDATE command denied to user 'obs'");
        }

        /**
         * 조회가 실제로 관측 템플릿을 지나는지 본다. 운영 풀로 새도 결과는 똑같이 나오므로
         * 호출 자체를 세는 것 말고는 확인할 방법이 없다.
         */
        @Test
        @DisplayName("조회는 관측 템플릿을 지난다")
        void readsGoThroughObservationTemplate() {
            long id = repository.open(command("V3-MAIN-01"), START);

            int before = observationJdbcTemplate.queries.get();
            repository.findById(id);
            assertThat(observationJdbcTemplate.queries.get())
                    .as("0 이면 조회가 관측 템플릿을 안 지났거나 세는 자리를 비켜 갔다")
                    .isGreaterThan(before);
        }
    }

    @Nested
    @DisplayName("적재와 왕복")
    class OpenAndRead {

        /**
         * 값을 전부 다르게 넣는다. 같은 값이 섞여 있으면 {@code ?} 바인딩 순서가 뒤바뀌어도
         * 왕복 결과가 같아 보인다 — 그게 이 테스트가 막으려는 결함이다.
         */
        @Test
        @DisplayName("넣은 값이 제자리에서 그대로 나온다")
        void everyFieldSurvivesTheRoundTrip() {
            long id = repository.open(command("V3-MAIN-01"), START);

            BenchmarkRun run = repository.findById(id).orElseThrow();
            assertThat(run.runKey()).isEqualTo("V3-MAIN-01");
            assertThat(run.runType()).isEqualTo(BenchmarkRunType.MAIN);
            assertThat(run.scenarioCode()).isEqualTo("LOAD_100K");
            assertThat(run.engineVersion()).isEqualTo(EngineVersion.V3);
            assertThat(run.releaseStage()).isEqualTo(ReleaseStage.V2_2);
            assertThat(run.queueMode()).isEqualTo(QueueMode.ADAPTIVE);
            assertThat(run.couponId()).isEqualTo(7L);
            assertThat(run.requestedBy()).isEqualTo("tester");
            assertThat(run.runStatus()).isEqualTo(BenchmarkRunStatus.RUNNING);
            assertThat(run.archiveStatus()).isEqualTo(BenchmarkArchiveStatus.NONE);
            assertThat(run.startedAt()).isEqualTo(START);

            // 토폴로지 9개는 전부 다른 숫자다. 하나라도 자리가 밀리면 여기서 걸린다.
            assertThat(run.topology()).isEqualTo(
                    new BenchmarkTopology(3, 6, 2000, 8192, 180, 4096, 200, 36, 50));
            assertThat(run.loadProfile()).isEqualTo(
                    new LoadProfile(20_000, 5, 60, 10_000, 0.8));
            assertThat(run.toolMeta()).isEqualTo(new LoadToolMeta("k6", "0.49.0", "abc123"));
            assertThat(run.client()).isEmpty();
            assertThat(run.server()).isEmpty();
            assertThat(run.observedLagTotal()).isNull();
        }

        @Test
        @DisplayName("모르는 자원 총량은 NULL 로 왕복한다")
        void unknownTotalsStayNull() {
            long id = repository.open(new StartBenchmarkRunCommand(
                    "V1-MAIN-01", BenchmarkRunType.MAIN, "LOAD_100K",
                    EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF, null, "tester",
                    new BenchmarkTopology(1, 6, null, null, 60, null, null, 12, 50),
                    new LoadProfile(20_000, 5, 60, null, null),
                    LoadToolMeta.unknown()), START);

            BenchmarkRun run = repository.findById(id).orElseThrow();
            assertThat(run.couponId()).isNull();
            assertThat(run.topology().cpuMillicoresTotal()).isNull();
            assertThat(run.topology().tomcatAcceptCount()).isNull();
            assertThat(run.loadProfile().stockTotal()).isNull();
            assertThat(run.loadProfile().generatorIdleRttMillis()).isNull();
            assertThat(run.toolMeta()).isEqualTo(LoadToolMeta.unknown());
        }

        @Test
        @DisplayName("진행 중인 회차와 최근 목록을 관측 풀로 읽는다")
        void findsRunningAndRecent() {
            long first = repository.open(command("V3-WARMUP"), START);
            repository.markLoadStopped(first, START.plusSeconds(5), null);
            long second = repository.open(command("V3-MAIN-01"), START.plusSeconds(10));

            assertThat(repository.findRunning()).get().extracting(BenchmarkRun::id).isEqualTo(second);
            assertThat(repository.findByRunKey("V3-WARMUP")).get()
                    .extracting(BenchmarkRun::id).isEqualTo(first);
            assertThat(repository.findRecent(10)).extracting(BenchmarkRun::id)
                    .containsExactly(second, first);
        }
    }

    @Nested
    @DisplayName("중복 판정")
    class Conflicts {

        @Test
        @DisplayName("진행 중인 회차가 있으면 RUN_ALREADY_RUNNING 이다")
        void secondRunningIsRejected() {
            repository.open(command("V3-MAIN-01"), START);

            assertThatThrownBy(() -> repository.open(command("V3-MAIN-02"), START))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(it -> assertThat(((BusinessException) it).getErrorCode())
                            .isEqualTo(BenchmarkErrorCode.RUN_ALREADY_RUNNING));
        }

        /**
         * 두 원인의 조치가 정반대다 — 키를 바꾼다 / 앞 회차를 닫는다. 제약 이름으로 가르는
         * 코드가 실제 드라이버 메시지 앞에서 도는지는 여기서만 확인된다.
         */
        @Test
        @DisplayName("같은 회차 키면 RUN_KEY_DUPLICATED 다")
        void duplicateRunKeyIsRejected() {
            long first = repository.open(command("V3-MAIN-01"), START);
            repository.markLoadStopped(first, START.plusSeconds(5), null);

            assertThatThrownBy(() -> repository.open(command("V3-MAIN-01"), START))
                    .satisfies(it -> assertThat(((BusinessException) it).getErrorCode())
                            .isEqualTo(BenchmarkErrorCode.RUN_KEY_DUPLICATED));
        }
    }

    @Nested
    @DisplayName("조건부 UPDATE — WHERE 절이 실제로 상태를 본다")
    class Transitions {

        @Test
        @DisplayName("부하 종료는 RUNNING 에서만 도는 한 번짜리다")
        void loadStopRunsOnlyOnceFromRunning() {
            long id = repository.open(command("V3-MAIN-01"), START);

            assertThat(repository.markLoadStopped(id, START.plusSeconds(5), "계획대로")).isTrue();
            BenchmarkRun run = repository.findById(id).orElseThrow();
            assertThat(run.runStatus()).isEqualTo(BenchmarkRunStatus.LOAD_STOPPED);
            assertThat(run.loadStoppedAt()).isEqualTo(START.plusSeconds(5));
            assertThat(run.loadStopReason()).isEqualTo("계획대로");

            assertThat(repository.markLoadStopped(id, START.plusSeconds(9), null)).isFalse();
            assertThat(repository.findById(id).orElseThrow().loadStoppedAt())
                    .as("거부된 전이가 값을 건드리면 안 된다")
                    .isEqualTo(START.plusSeconds(5));
        }

        @Test
        @DisplayName("없는 회차의 전이는 조용히 false 다")
        void missingRunReturnsFalse() {
            assertThat(repository.markLoadStopped(404L, START, null)).isFalse();
            assertThat(repository.markObserved(404L, START, 0)).isFalse();
            assertThat(repository.markFinalized(404L, START)).isFalse();
            assertThat(repository.updateClientSummary(404L, clientSummary(0))).isFalse();
            assertThat(repository.updateServerSummary(404L, serverSummary())).isFalse();
            assertThat(repository.updateArchiveStatus(404L, BenchmarkArchiveStatus.DONE, null)).isFalse();
        }

        @Test
        @DisplayName("관측 종료는 LOAD_STOPPED 에서만 돈다")
        void observationStopRequiresLoadStopped() {
            long id = repository.open(command("V3-MAIN-01"), START);

            assertThat(repository.markObserved(id, START.plusSeconds(65), 0)).isFalse();

            repository.markLoadStopped(id, START.plusSeconds(5), null);
            assertThat(repository.markObserved(id, START.plusSeconds(65), 0)).isTrue();
            BenchmarkRun run = repository.findById(id).orElseThrow();
            assertThat(run.runStatus()).isEqualTo(BenchmarkRunStatus.OBSERVED);
            assertThat(run.observationStoppedAt()).isEqualTo(START.plusSeconds(65));
            assertThat(run.observedLagTotal()).isZero();
        }

        /**
         * 공식 요약 유무가 WHERE 절에 있다. 서비스가 미리 읽어 판단하는 구조였다면 그 사이에
         * 요약이 지워질 수 있고, 그러면 사람이 못 읽는 CHECK 예외로 대신 거부된다.
         */
        @Test
        @DisplayName("확정은 OBSERVED 이면서 공식 요약이 있을 때만 돈다")
        void finalizeNeedsObservedAndSummary() {
            long id = repository.open(command("V3-MAIN-01"), START);
            repository.markLoadStopped(id, START.plusSeconds(5), null);

            assertThat(repository.markFinalized(id, START.plusSeconds(70)))
                    .as("아직 OBSERVED 가 아니다").isFalse();

            repository.markObserved(id, START.plusSeconds(65), 0);
            assertThat(repository.markFinalized(id, START.plusSeconds(70)))
                    .as("공식 요약이 없다").isFalse();

            repository.updateClientSummary(id, clientSummary(0));
            assertThat(repository.markFinalized(id, START.plusSeconds(70))).isTrue();
            assertThat(repository.findById(id).orElseThrow().finalizedAt())
                    .isEqualTo(START.plusSeconds(70));
        }
    }

    @Nested
    @DisplayName("요약 적재 — server_* 와 client_* 가 섞이지 않는다")
    class Summaries {

        /**
         * 여섯 값을 전부 다르게 넣는다. 바인딩 순서가 하나만 밀려도 왕복에서 드러난다.
         */
        @Test
        @DisplayName("client 요약이 제자리에 들어간다")
        void clientSummaryLandsInItsOwnColumns() {
            long id = repository.open(command("V3-MAIN-01"), START);

            assertThat(repository.updateClientSummary(id, clientSummary(1274))).isTrue();

            ClientLoadSummary stored = repository.findById(id).orElseThrow().client().orElseThrow();
            assertThat(stored.requestCount()).isEqualTo(100_000);
            assertThat(stored.failureCount()).isEqualTo(37);
            assertThat(stored.droppedIterations()).isEqualTo(1274);
            assertThat(stored.tps()).isEqualTo(18_472.5);
            assertThat(stored.p95Millis()).isEqualTo(206.1);
            assertThat(stored.p99Millis()).isEqualTo(412.3);
            assertThat(stored.measuredAt()).isEqualTo(START.plusSeconds(70));
            assertThat(repository.findById(id).orElseThrow().server()).isEmpty();
        }

        @Test
        @DisplayName("server 요약이 client 자리를 건드리지 않는다")
        void serverSummaryDoesNotTouchClientColumns() {
            long id = repository.open(command("V3-MAIN-01"), START);
            repository.updateClientSummary(id, clientSummary(0));

            assertThat(repository.updateServerSummary(id, serverSummary())).isTrue();

            BenchmarkRun run = repository.findById(id).orElseThrow();
            ServerLoadSummary server = run.server().orElseThrow();
            assertThat(server.requestCount()).isEqualTo(99_998);
            assertThat(server.failureCount()).isEqualTo(11);
            assertThat(server.tps()).isEqualTo(18_500.25);
            assertThat(server.p95Millis()).isEqualTo(90.5);
            assertThat(server.p99Millis()).isEqualTo(180.75);
            assertThat(server.measuredAt()).isEqualTo(START.plusSeconds(71));

            // client 쪽 여섯 값이 그대로여야 한다 — 두 UPDATE 가 같은 컬럼을 쓰면 여기서 걸린다.
            assertThat(run.client().orElseThrow()).isEqualTo(clientSummary(0));
        }

        @Test
        @DisplayName("확정된 회차의 요약은 갱신되지 않는다")
        void finalizedSummariesAreLocked() {
            long id = finalizedRun();

            assertThat(repository.updateClientSummary(id, clientSummary(999))).isFalse();
            assertThat(repository.updateServerSummary(id, serverSummary())).isFalse();
            assertThat(repository.findById(id).orElseThrow().client().orElseThrow().droppedIterations())
                    .isZero();
        }
    }

    @Nested
    @DisplayName("archive 는 회차 상태를 보지 않는다")
    class Archive {

        @Test
        @DisplayName("확정된 회차에도 archive 결과를 남기고 다시 덮을 수 있다")
        void archiveIsIndependentAndRetryable() {
            long id = finalizedRun();

            assertThat(repository.updateArchiveStatus(
                    id, BenchmarkArchiveStatus.FAILED, "query_range timeout")).isTrue();
            BenchmarkRun failed = repository.findById(id).orElseThrow();
            assertThat(failed.runStatus()).isEqualTo(BenchmarkRunStatus.FINALIZED);
            assertThat(failed.archiveStatus()).isEqualTo(BenchmarkArchiveStatus.FAILED);
            assertThat(failed.archiveFailureReason()).isEqualTo("query_range timeout");

            assertThat(repository.updateArchiveStatus(id, BenchmarkArchiveStatus.DONE, null)).isTrue();
            BenchmarkRun done = repository.findById(id).orElseThrow();
            assertThat(done.archiveStatus()).isEqualTo(BenchmarkArchiveStatus.DONE);
            assertThat(done.archiveFailureReason()).isNull();
        }
    }

    // ── 고정값 ───────────────────────────────────────────────────────────────

    private static final Instant START = Instant.parse("2026-08-22T00:00:00Z");

    private long finalizedRun() {
        long id = repository.open(command("V3-MAIN-01"), START);
        repository.markLoadStopped(id, START.plusSeconds(5), null);
        repository.markObserved(id, START.plusSeconds(65), 0);
        repository.updateClientSummary(id, clientSummary(0));
        repository.markFinalized(id, START.plusSeconds(70));
        return id;
    }

    private static StartBenchmarkRunCommand command(String runKey) {
        return new StartBenchmarkRunCommand(
                runKey, BenchmarkRunType.MAIN, "LOAD_100K",
                EngineVersion.V3, ReleaseStage.V2_2, QueueMode.ADAPTIVE, 7L, "tester",
                new BenchmarkTopology(3, 6, 2000, 8192, 180, 4096, 200, 36, 50),
                new LoadProfile(20_000, 5, 60, 10_000, 0.8),
                new LoadToolMeta("k6", "0.49.0", "abc123"));
    }

    private static ClientLoadSummary clientSummary(long droppedIterations) {
        return new ClientLoadSummary(100_000, 37, droppedIterations, 18_472.5, 206.1, 412.3,
                START.plusSeconds(70));
    }

    private static ServerLoadSummary serverSummary() {
        return new ServerLoadSummary(99_998, 11, 18_500.25, 90.5, 180.75, START.plusSeconds(71));
    }

    private static HikariDataSource hikari(String username, String password) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(mysql.getJdbcUrl());
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(2);
        return dataSource;
    }

    /**
     * 조회가 정말 이 템플릿을 지나는지 세는 것 말고는 확인할 방법이 없다 — 운영 풀로 새도
     * 결과는 똑같이 나온다. 어댑터가 다른 오버로드로 옮겨 가면 카운터가 0 에 머물러 빨간불이 된다.
     */
    private static final class CountingJdbcTemplate extends JdbcTemplate {

        private final AtomicInteger queries = new AtomicInteger();

        private CountingJdbcTemplate(DataSource dataSource) {
            super(dataSource);
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
            queries.incrementAndGet();
            return super.query(sql, rowMapper, args);
        }
    }
}
