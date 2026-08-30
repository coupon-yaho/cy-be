package com.kafkick.storage.db.consistency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import javax.sql.DataSource;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyFinalObservation;
import com.kafkick.core.consistency.ConsistencyFinalStore;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

class JdbcConsistencyFinalStoreTest {
    private static MySQLContainer mysql;
    private static DataSource dataSource;
    private static JdbcTemplate jdbc;
    private static ConsistencyFinalStore store;
    private static final AtomicBoolean transactionSeenDuringComplete = new AtomicBoolean();

    @BeforeAll
    static void start() {
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("app")
                .withCommand("--default-time-zone=+00:00", "--character-set-server=utf8mb4",
                        "--collation-server=utf8mb4_0900_ai_ci");
        mysql.start();
        Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration").load().migrate();
        dataSource = new org.springframework.jdbc.datasource.DriverManagerDataSource(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        store = transactionalStore(new JdbcConsistencyFinalStore(recordingJdbc(), jdbc));
        jdbc.update("INSERT INTO brands (id, name, category) VALUES (1, 'brand', 'CAFE')");
        jdbc.update("""
            INSERT INTO coupon_templates
              (id, brand_id, name, policy_type, discount_amount, valid_days,
               nth_week, day_of_week, start_time, duration_hours,
               stock_per_occurrence, eligible_grades_mask)
            VALUES (1, 1, 'template', 'FIXED_AMOUNT', 1000, 1,
                    1, 'MON', '00:00:00', 1, 100, 1)
            """);
        jdbc.update("""
            INSERT INTO coupons
              (id, template_id, brand_id, name, policy_type, valid_days, eligible_grades_mask,
               open_at, close_at, status, created_at, generated_at)
            VALUES (11, 1, 1, 'first', 'FIXED_AMOUNT', 1, 1,
                    '2026-08-02', '2026-09-02', 'CLOSED', NOW(6), NOW(6))
            """);
        insertFinalizedRun(1, "run-1");
        insertFinalizedRun(2, "run-2");
    }

    @AfterAll
    static void stop() { if (mysql != null) mysql.stop(); }

    /**
     * 프로덕션과 같은 @Transactional 프록시 위에서 검증한다. 프록시 없이 new로 만들면
     * autocommit이라 complete()의 SELECT FOR UPDATE 락이 문장 끝에서 풀리고 롤백도 없다.
     */
    private static ConsistencyFinalStore transactionalStore(JdbcConsistencyFinalStore target) {
        ProxyFactory factory = new ProxyFactory(target);
        TransactionInterceptor interceptor = new TransactionInterceptor();
        interceptor.setTransactionManager(new DataSourceTransactionManager(dataSource));
        interceptor.setTransactionAttributeSource(new AnnotationTransactionAttributeSource());
        factory.addAdvice(interceptor);
        return (ConsistencyFinalStore) factory.getProxy();
    }

    /** 쓰기 문장이 실제 트랜잭션 안에서 실행됐는지 기록한다. */
    private static JdbcTemplate recordingJdbc() {
        return new JdbcTemplate(dataSource) {
            @Override
            public int update(String sql, Object... args) {
                if (TransactionSynchronizationManager.isActualTransactionActive()) {
                    transactionSeenDuringComplete.set(true);
                }
                return super.update(sql, args);
            }
        };
    }

    @BeforeEach
    void resetFinals() {
        jdbc.update("DELETE FROM consistency_finals");
        jdbc.update("""
            UPDATE benchmark_runs
               SET consistency_status='NONE', consistency_failure_reason=NULL,
                   consistency_claimed_at=NULL, consistency_claim_token=NULL
             WHERE id IN (1, 2)
            """);
    }

    @Test
    void payloadHasFifteenColumnsAndCheckRejectsWrongStateValuePair() {
        Integer payloadColumns = jdbc.queryForObject("""
            SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema=DATABASE() AND table_name='consistency_finals'
               AND (column_name LIKE '%_value' OR column_name LIKE '%_state'
                    OR column_name LIKE '%_observed_at')
            """, Integer.class);
        assertThat(payloadColumns).isEqualTo(15);

        save(1, evaluation(SourceStatus.VALID));
        assertThatThrownBy(() -> jdbc.update("""
            UPDATE consistency_finals
               SET lua_gap_state='UNAVAILABLE', lua_gap_value=1, lua_gap_observed_at=NULL
             WHERE run_id=1
            """)).hasMessageContaining("ck_final_lua_gap");
    }

    @Test
    void outerValidWrapsInnerStaleGapAndCouponName() {
        save(1, evaluation(SourceStatus.VALID));
        save(2, evaluation(SourceStatus.STALE));
        var latest = store.findLatestByCouponId(11L);
        assertThat(latest.status()).isEqualTo(SourceStatus.VALID);
        assertThat(latest.value().evaluation().gaps().get(ConsistencyGapType.LUA_GAP).state())
                .isEqualTo(SourceStatus.STALE);
        assertThat(latest.value().couponName()).isEqualTo("first");
        assertThat(latest.value().evaluatedAt()).isEqualTo(Instant.parse("2026-08-26T00:00:00Z"));
    }

    @Test
    void sameEvaluatedAtBreaksTheTieWithTheHigherRunId() {
        save(1, evaluation(SourceStatus.VALID));
        save(2, evaluation(SourceStatus.STALE));

        ConsistencyFinalObservation latest = store.findLatestByCouponIds(List.of(11L)).get(11L);

        assertThat(latest.value().evaluation().gaps().get(ConsistencyGapType.LUA_GAP).state())
                .isEqualTo(SourceStatus.STALE);
    }

    @Test
    void incompleteRunFinalDoesNotHideTheLatestFinalizedRunResult() {
        save(1, evaluation(SourceStatus.VALID));
        save(2, evaluation(SourceStatus.STALE));
        jdbc.update("UPDATE benchmark_runs SET run_status='OBSERVED', finalized_at=NULL WHERE id=2");

        ConsistencyFinalObservation latest = store.findLatestByCouponIds(List.of(11L)).get(11L);

        assertThat(latest.value().evaluation().gaps().get(ConsistencyGapType.LUA_GAP).state())
                .isEqualTo(SourceStatus.VALID);
    }

    @Test
    void missingFinalIsPendingAndMissingCouponRoundIsNotApplicable() {
        insertCoupon(12, "pending");
        insertCoupon(17, "not-applied");
        insertFinalizedRun(12, "run-12", 12);
        assertThat(store.findLatestByCouponId(12L).status()).isEqualTo(SourceStatus.PENDING);
        assertThat(store.findLatestByCouponId(17L).status()).isEqualTo(SourceStatus.N_A);
        assertThat(store.findLatestByCouponId(999L).status()).isEqualTo(SourceStatus.N_A);
    }

    @Test
    void expiredRunWithoutFinalIsUnavailableWhileRetryableFailureStaysPending() {
        insertCoupon(18, "expired-final");
        insertCoupon(19, "retryable-final");
        insertFinalizedRun(16, "run-16", 18);
        insertFinalizedRun(17, "run-17", 19);
        String expiredToken = store.claim(16, Duration.ofMinutes(5)).orElseThrow().token();
        String retryableToken = store.claim(17, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.expire(16, expiredToken, "final window expired")).isTrue();
        assertThat(store.fail(17, retryableToken, "temporary source failure")).isTrue();

        Map<Long, ConsistencyFinalObservation> result =
                store.findLatestByCouponIds(List.of(18L, 19L));

        assertThat(result.get(18L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.get(19L).status()).isEqualTo(SourceStatus.PENDING);
    }

    @Test
    void bulkReadReturnsEveryRequestedCouponRoundInFirstInputOrder() {
        save(1, evaluation(SourceStatus.VALID));
        insertCoupon(16, "bulk-pending");
        insertFinalizedRun(13, "run-13", 16);

        Map<Long, ConsistencyFinalObservation> result =
                store.findLatestByCouponIds(List.of(16L, 11L, 16L, 999L));

        assertThat(result.keySet()).containsExactly(16L, 11L, 999L);
        assertThat(result.get(16L).status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.get(11L).status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.get(999L).status()).isEqualTo(SourceStatus.N_A);
        assertThatThrownBy(() -> result.put(1000L,
                new ConsistencyFinalObservation(SourceStatus.N_A, null)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void completedFinalStaysValidWhileANewerBenchmarkIsInProgress() {
        save(1, evaluation(SourceStatus.VALID));
        insertFinalizedRun(14, "run-14");
        jdbc.update("UPDATE benchmark_runs SET consistency_status='IN_PROGRESS', "
                + "consistency_claimed_at=NOW(6), consistency_claim_token='owner' WHERE id=14");

        assertThat(store.findLatestByCouponIds(List.of(11L)).get(11L).status())
                .isEqualTo(SourceStatus.VALID);
    }

    @Test
    void invalidIdsAreRejectedBeforeObservationJdbcIsCalled() {
        JdbcTemplate observation = org.mockito.Mockito.mock(JdbcTemplate.class);
        ConsistencyFinalStore isolated = new JdbcConsistencyFinalStore(jdbc, observation);

        assertThatThrownBy(() -> isolated.findLatestByCouponIds(List.of(1L, 0L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> isolated.findLatestByCouponIds(java.util.Arrays.asList(1L, null)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(isolated.findLatestByCouponIds(List.of())).isEmpty();
        org.mockito.Mockito.verifyNoInteractions(observation);
    }

    @Test
    void corruptFinalEnumMakesOnlyThatCouponRoundUnavailable() {
        insertCoupon(15, "second");
        insertFinalizedRun(15, "run-15", 15);
        save(1, evaluation(SourceStatus.VALID));
        String token = store.claim(15, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.complete(15, token, 15L, EngineVersion.V3,
                Instant.parse("2026-08-26T00:00:00Z"), evaluation(SourceStatus.VALID))).isTrue();
        jdbc.execute("ALTER TABLE consistency_finals "
                + "ALTER CHECK ck_consistency_final_verdict NOT ENFORCED");
        try {
            jdbc.update("UPDATE consistency_finals SET verdict='BROKEN' WHERE run_id=15");

            Map<Long, ConsistencyFinalObservation> result =
                    store.findLatestByCouponIds(List.of(11L, 15L));

            assertThat(result.get(11L).status()).isEqualTo(SourceStatus.VALID);
            assertThat(result.get(15L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
        } finally {
            jdbc.update("UPDATE consistency_finals SET verdict='PASS' WHERE run_id=15");
            jdbc.execute("ALTER TABLE consistency_finals "
                    + "ALTER CHECK ck_consistency_final_verdict ENFORCED");
        }
    }

    @Test
    void sqlFailureMakesEveryRequestedCouponRoundUnavailable() throws Exception {
        DataSource unavailable = org.mockito.Mockito.mock(DataSource.class);
        org.mockito.Mockito.when(unavailable.getConnection())
                .thenThrow(new SQLException("observation database down"));
        ConsistencyFinalStore isolated = new JdbcConsistencyFinalStore(
                jdbc, new JdbcTemplate(unavailable));

        Map<Long, ConsistencyFinalObservation> result =
                isolated.findLatestByCouponIds(List.of(11L, 12L));

        assertThat(result).allSatisfy((couponId, observation) ->
                assertThat(observation.status()).isEqualTo(SourceStatus.UNAVAILABLE));
    }

    @Test
    void completeRunsInsertAndOwnershipUpdateInsideOneTransaction() {
        String token = store.claim(1, Duration.ofMinutes(5)).orElseThrow().token();
        // claim 도 @Transactional 이라 여기서 리셋해야 complete 만 보는 단언이 된다.
        transactionSeenDuringComplete.set(false);
        assertThat(store.complete(1, token, 11L, EngineVersion.V3,
                Instant.parse("2026-08-26T00:00:00Z"), evaluation(SourceStatus.VALID))).isTrue();
        assertThat(transactionSeenDuringComplete).isTrue();
        assertThat(jdbc.queryForObject(
                "SELECT consistency_status FROM benchmark_runs WHERE id=1", String.class))
                .isEqualTo("DONE");
    }

    @Test
    void concurrentClaimsOnTheSameRunProduceExactlyOneOwner() throws Exception {
        insertFinalizedRun(4, "run-4");
        int threads = 16;
        CyclicBarrier gate = new CyclicBarrier(threads);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Callable<Optional<ConsistencyFinalStore.Claim>>> tasks = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                tasks.add(() -> {
                    gate.await();
                    return store.claim(4, Duration.ofMinutes(5));
                });
            }
            long owners = 0;
            for (Future<Optional<ConsistencyFinalStore.Claim>> result : pool.invokeAll(tasks)) {
                if (result.get().isPresent()) owners++;
            }
            assertThat(owners).isEqualTo(1);
        }
    }

    @Test
    void everyClaimBumpsTheAttemptNumberStoredWithTheVerdict() {
        insertFinalizedRun(7, "run-7");
        store.claim(7, Duration.ofMinutes(5)).orElseThrow().token();
        jdbc.update("UPDATE benchmark_runs SET consistency_status='FAILED', "
                + "consistency_failure_reason='batch down', consistency_claimed_at=NULL, "
                + "consistency_claim_token=NULL WHERE id=7");
        String second = store.claim(7, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.complete(7, second, 11L, EngineVersion.V3,
                Instant.parse("2026-08-26T00:00:00Z"), evaluation(SourceStatus.VALID))).isTrue();

        // 몇 번째 시도의 라이브 관측인지가 판정과 함께 남아야 재실행 결과를 대조할 수 있다.
        assertThat(jdbc.queryForObject(
                "SELECT attempt_no FROM consistency_finals WHERE run_id=7", Integer.class))
                .isEqualTo(2);
    }

    @Test
    void claimReturnsTheFailureReasonItErases() {
        insertFinalizedRun(10, "run-10");
        String first = store.claim(10, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.fail(10, first, "batch down")).isTrue();

        var second = store.claim(10, Duration.ofMinutes(5)).orElseThrow();

        // 따로 읽으면 그 사이 다른 작업자가 남긴 사유를 놓친다.
        assertThat(second.previousFailureReason()).isEqualTo("batch down");
        assertThat(jdbc.queryForObject(
                "SELECT consistency_failure_reason FROM benchmark_runs WHERE id=10", String.class))
                .isNull();
    }

    @Test
    void expiredRunIsTerminalWhileFailedStaysRetryable() {
        insertFinalizedRun(8, "run-8");
        String failed = store.claim(8, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.fail(8, failed, "batch down")).isTrue();
        assertThat(store.claim(8, Duration.ofMinutes(5))).isPresent();

        String token = jdbc.queryForObject(
                "SELECT consistency_claim_token FROM benchmark_runs WHERE id=8", String.class);
        assertThat(store.expire(8, token, "finalize window expired: previousReason=batch down"))
                .isTrue();
        // EXPIRED 가 다시 claim 되면 의미 없는 재실행이 원래 원인을 덮어쓴다.
        assertThat(store.claim(8, Duration.ofMinutes(5))).isEmpty();
        assertThat(jdbc.queryForObject(
                "SELECT consistency_failure_reason FROM benchmark_runs WHERE id=8", String.class))
                .contains("previousReason=batch down");
    }

    @Test
    void liveObservationAfterFinalizedAtIsAcceptedBecauseEvaluatedAtIsTheRoundTime() {
        insertFinalizedRun(9, "run-9");
        String token = store.claim(9, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.complete(9, token, 11L, EngineVersion.V3,
                Instant.parse("2026-08-26T00:00:00Z"), evaluation(SourceStatus.VALID))).isTrue();

        // evaluated_at 은 확정 시각이라 관측보다 앞선다. 이걸 막는 제약이 있으면 안 된다.
        assertThat(jdbc.queryForObject("""
            SELECT lua_gap_observed_at > evaluated_at FROM consistency_finals WHERE run_id=9
            """, Boolean.class)).isTrue();
    }

    @Test
    void deletingTheRunCascadesTheStoredFinalInsteadOfLeavingAnOrphan() {
        save(1, evaluation(SourceStatus.VALID));
        jdbc.update("DELETE FROM benchmark_runs WHERE id=1");
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM consistency_finals WHERE run_id=1", Integer.class))
                .isZero();
        insertFinalizedRun(1, "run-1");
    }

    @Test
    void failureReasonLongerThanColumnLimitIsRejectedBeforeReachingMySql() {
        insertFinalizedRun(5, "run-5");
        String token = store.claim(5, Duration.ofMinutes(5)).orElseThrow().token();
        assertThatThrownBy(() -> store.fail(5, token,
                "x".repeat(ConsistencyFinalStore.FAILURE_REASON_MAX + 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deletedCouponRoundKeepsStoredFinalVisibleInsteadOfFlippingToPending() {
        insertCoupon(13, "orphan");
        insertFinalizedRun(6, "run-6", 13);
        String token = store.claim(6, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.complete(6, token, 13L, EngineVersion.V3,
                Instant.parse("2026-08-26T00:00:00Z"), evaluation(SourceStatus.VALID))).isTrue();
        jdbc.update("DELETE FROM coupons WHERE id=13");

        var latest = store.findLatestByCouponId(13L);
        assertThat(latest.status()).isEqualTo(SourceStatus.VALID);
        assertThat(latest.value().couponName()).isNull();
    }

    @Test
    void leaseRejectsConcurrentOwnerAndExpiredOwnerCanBeRecovered() {
        insertFinalizedRun(3, "run-3");
        String first = store.claim(3, Duration.ofSeconds(1)).orElseThrow().token();
        assertThat(store.claim(3, Duration.ofSeconds(1))).isEmpty();
        jdbc.update("UPDATE benchmark_runs SET consistency_claimed_at=NOW(6)-INTERVAL 2 SECOND WHERE id=3");
        String recovered = store.claim(3, Duration.ofSeconds(1)).orElseThrow().token();
        assertThat(recovered).isNotEqualTo(first);
        assertThat(store.fail(3, first, "late owner")).isFalse();
        assertThat(store.fail(3, recovered, "batch down")).isTrue();
    }

    private static void save(long runId, ConsistencyEvaluation evaluation) {
        String token = store.claim(runId, Duration.ofMinutes(5)).orElseThrow().token();
        assertThat(store.complete(runId, token, 11L, EngineVersion.V3,
                Instant.parse("2026-08-26T00:00:00Z"), evaluation)).isTrue();
    }

    private static ConsistencyEvaluation evaluation(SourceStatus luaState) {
        Instant observed = Instant.parse("2026-08-26T00:12:00Z");
        var gaps = new EnumMap<ConsistencyGapType, GapValue>(ConsistencyGapType.class);
        for (ConsistencyGapType type : ConsistencyGapType.values()) {
            SourceStatus state = type == ConsistencyGapType.LUA_GAP ? luaState : SourceStatus.VALID;
            gaps.put(type, new GapValue(0L, state, observed));
        }
        return new ConsistencyEvaluation(gaps,
                new GapValue(0L, SourceStatus.VALID, observed), ConsistencyPhase.FINAL,
                Verdict.PASS, Severity.NONE);
    }

    private static void insertFinalizedRun(long id, String key) {
        insertFinalizedRun(id, key, 11);
    }

    private static void insertCoupon(long id, String name) {
        jdbc.update("""
            INSERT INTO coupons
              (id, template_id, brand_id, name, policy_type, valid_days, eligible_grades_mask,
               open_at, close_at, status, created_at, generated_at)
            VALUES (?, 1, 1, ?, 'FIXED_AMOUNT', 1, 1,
                    ?, ?, 'CLOSED', NOW(6), NOW(6))
            """, id, name, "2026-08-%02d".formatted(id), "2026-09-%02d".formatted(id));
    }

    private static void insertFinalizedRun(long id, String key, long couponId) {
        jdbc.update("""
            INSERT INTO benchmark_runs
              (id, run_key, run_type, scenario_code, engine_version, release_stage, queue_mode,
               coupon_id, run_status, started_at, load_stopped_at, observation_stopped_at,
               finalized_at, requested_by, app_replicas, available_processors,
               tomcat_workers_total, hikari_pool_total, mysql_max_connections,
               offered_rps, load_hold_seconds, observation_hold_seconds,
               client_request_count, client_failure_count, client_dropped_iterations,
               client_tps, client_p95_millis, client_p99_millis, client_measured_at,
               observed_lag_total)
            VALUES (?, ?, 'MAIN', 'SPIKE', 'V3', 'V3', 'ADAPTIVE', ?, 'FINALIZED',
                    '2026-08-25 23:00:00', '2026-08-25 23:01:00', '2026-08-25 23:02:00',
                    '2026-08-26 00:00:00', 'tester', 1, 6, 60, 12, 50, 20000, 5, 60,
                    1, 0, 0, 1, 1, 1, '2026-08-25 23:01:00', 0)
            """, id, key, couponId);
    }
}
