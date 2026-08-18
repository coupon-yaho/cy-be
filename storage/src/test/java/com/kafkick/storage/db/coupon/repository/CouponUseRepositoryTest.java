// 실제 MySQL에서 쿠폰 사용·멱등 응답·조건부 상태 전이의 원자성을 검증합니다.
package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.coupon.service.CouponUseCommand;
import com.kafkick.core.coupon.service.CouponUseResult;
import com.kafkick.core.coupon.service.CouponUseService;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RepositoryTest
@Import({
        CouponRoundRepositoryImpl.class,
        IssuanceRepositoryImpl.class,
        IssuanceUsageRepositoryImpl.class,
        IssuanceHistoryRepositoryImpl.class,
        IdempotencyRepositoryImpl.class,
        CouponUseRepositoryTest.AuditTestConfig.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class CouponUseRepositoryTest {

    private static final long TIMEOUT_SECONDS = 30;
    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String REQUEST_HASH = "a".repeat(64);
    private static final Instant USED_AT =
            Instant.parse("2026-08-20T05:30:00Z");

    @Autowired
    private CouponRoundRepository couponRoundRepository;

    @Autowired
    private IssuanceRepository issuanceRepository;

    @Autowired
    private IssuanceUsageRepository issuanceUsageRepository;

    @Autowired
    private IssuanceHistoryRepository issuanceHistoryRepository;

    @Autowired
    private IdempotencyRepository idempotencyRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private CouponUseService couponUseService;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        resetData();
        saveCouponData();
        transactionTemplate = new TransactionTemplate(transactionManager);
        couponUseService = new CouponUseService(
                issuanceRepository,
                couponRoundRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository
        );
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("쿠폰 상태·사용 실적·USE 이력·멱등 응답을 한 번에 저장한다")
    void useCouponAtomically() {
        CouponUseResult result = executeUse(IDEMPOTENCY_KEY, REQUEST_HASH);

        Map<String, Object> issuance = jdbcTemplate.queryForMap(
                "SELECT status, updated_at FROM issuances WHERE id = 100"
        );
        Map<String, Object> usage = jdbcTemplate.queryForMap(
                """
                SELECT issuance_id, order_id, discount_amount,
                       used_at, canceled_at, created_at
                FROM issuance_usages
                WHERE issuance_id = 100
                """
        );
        Map<String, Object> history = jdbcTemplate.queryForMap(
                """
                SELECT event_type, from_status, to_status, request_id
                FROM issuance_histories
                WHERE issuance_id = 100 AND event_type = 'USE'
                """
        );
        Map<String, Object> idempotency = jdbcTemplate.queryForMap(
                """
                SELECT member_id, issuance_id, status, response_body
                FROM idempotency_records
                WHERE idem_key = ?
                """,
                IDEMPOTENCY_KEY
        );

        assertThat(result.discountAmount()).isEqualTo(5_000);
        assertThat(issuance.get("status")).isEqualTo("USED");
        assertThat(((Number) usage.get("issuance_id")).longValue())
                .isEqualTo(100L);
        assertThat(((Number) usage.get("order_id")).longValue())
                .isEqualTo(30L);
        assertThat(((Number) usage.get("discount_amount")).intValue())
                .isEqualTo(5_000);
        assertThat(usage.get("canceled_at")).isNull();
        assertThat(((LocalDateTime) usage.get("created_at"))
                .toInstant(ZoneOffset.UTC))
                .isEqualTo(USED_AT);
        assertThat(history.get("from_status")).isEqualTo("ISSUED");
        assertThat(history.get("to_status")).isEqualTo("USED");
        assertThat(history.get("request_id")).isEqualTo(IDEMPOTENCY_KEY);
        assertThat(idempotency.get("status")).isEqualTo("DONE");
        assertThat(idempotency.get("response_body"))
                .isEqualTo("stored-response");
        assertThat(activeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("DONE 멱등 레코드는 대상과 응답이 모두 있어야 한다")
    void enforceDoneIdempotencyTargets() {
        assertThatThrownBy(() -> jdbcTemplate.update(
                """
                INSERT INTO idempotency_records (
                    idem_key, member_id, issuance_id, request_hash,
                    status, response_body, created_at
                ) VALUES (?, NULL, NULL, ?, 'DONE', NULL, ?)
                """,
                "550e8400-e29b-41d4-a716-446655440010",
                REQUEST_HASH,
                Timestamp.from(USED_AT)
        )).isInstanceOf(DataAccessException.class);
    }

    @Test
    @DisplayName("IN_PROGRESS 멱등 레코드는 완료 대상 값을 가질 수 없다")
    void enforceInProgressIdempotencyTargets() {
        assertThatThrownBy(() -> jdbcTemplate.update(
                """
                INSERT INTO idempotency_records (
                    idem_key, member_id, issuance_id, request_hash,
                    status, response_body, created_at
                ) VALUES (?, ?, ?, ?, 'IN_PROGRESS', NULL, ?)
                """,
                "550e8400-e29b-41d4-a716-446655440011",
                20L,
                100L,
                REQUEST_HASH,
                Timestamp.from(USED_AT)
        )).isInstanceOf(DataAccessException.class);
    }

    @Test
    @DisplayName("회수된 멱등 선점은 이전 소유자의 완료를 거부한다")
    void rejectCompletionFromPreviousClaimOwner() {
        Instant reclaimedAt = USED_AT.plusSeconds(31);
        transactionTemplate.executeWithoutResult(status ->
                assertThat(idempotencyRepository.tryStart(
                        IDEMPOTENCY_KEY,
                        REQUEST_HASH,
                        USED_AT
                )).isTrue()
        );

        transactionTemplate.executeWithoutResult(status ->
                assertThat(idempotencyRepository.tryReclaim(
                        IDEMPOTENCY_KEY,
                        REQUEST_HASH,
                        USED_AT,
                        reclaimedAt
                )).isTrue()
        );

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(
                status -> idempotencyRepository.complete(
                        IDEMPOTENCY_KEY,
                        20L,
                        100L,
                        "stored-response",
                        USED_AT
                )
        )).isInstanceOf(IdempotencyPersistenceException.class);

        transactionTemplate.executeWithoutResult(status ->
                idempotencyRepository.complete(
                        IDEMPOTENCY_KEY,
                        20L,
                        100L,
                        "stored-response",
                        reclaimedAt
                )
        );
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM idempotency_records WHERE idem_key = ?",
                String.class,
                IDEMPOTENCY_KEY
        )).isEqualTo("DONE");
    }

    @Test
    @DisplayName("같은 멱등키 동시 10회는 한 번만 사용하고 모두 같은 응답을 받는다")
    void replaySameResponseForConcurrentIdempotentRequests()
            throws Exception {
        int requestCount = 10;
        executor = Executors.newFixedThreadPool(requestCount);
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>();

        for (int index = 0; index < requestCount; index++) {
            futures.add(executor.submit(() -> {
                ready.countDown();
                if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "동시 요청 시작 신호를 기다리지 못했습니다."
                    );
                }
                return processIdempotentRequest(completed);
            }));
        }

        assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        start.countDown();

        for (Future<String> future : futures) {
            assertThat(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isEqualTo("stored-response");
        }
        assertThat(countRows("issuance_usages")).isEqualTo(1);
        assertThat(countUseHistories()).isEqualTo(1);
        assertThat(countRows("idempotency_records")).isEqualTo(1);
        assertThat(activeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 쿠폰을 서로 다른 키로 동시에 사용해도 상태 전이는 한 번만 반영된다")
    void allowOnlyOneConcurrentStateTransition() throws Exception {
        int requestCount = 2;
        executor = Executors.newFixedThreadPool(requestCount);
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger sequence = new AtomicInteger();
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int index = 0; index < requestCount; index++) {
            String key = index == 0
                    ? "550e8400-e29b-41d4-a716-446655440001"
                    : "550e8400-e29b-41d4-a716-446655440002";
            futures.add(executor.submit(() -> {
                ready.countDown();
                if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "동시 요청 시작 신호를 기다리지 못했습니다."
                    );
                }
                try {
                    transactionTemplate.executeWithoutResult(status -> {
                        assertThat(idempotencyRepository.tryStart(
                                key,
                                String.valueOf(sequence.incrementAndGet())
                                        .repeat(64),
                                USED_AT
                        )).isTrue();
                        couponUseService.use(command(key));
                        idempotencyRepository.complete(
                                key,
                                20L,
                                100L,
                                "stored-response",
                                USED_AT
                        );
                    });
                    return true;
                } catch (BusinessException exception) {
                    assertThat(exception.getErrorCode())
                            .isEqualTo(
                                    CouponIssueErrorCode.INVALID_TRANSITION
                            );
                    return false;
                }
            }));
        }

        assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        start.countDown();

        int successCount = 0;
        for (Future<Boolean> future : futures) {
            if (future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                successCount++;
            }
        }
        assertThat(successCount).isEqualTo(1);
        assertThat(countRows("issuance_usages")).isEqualTo(1);
        assertThat(countUseHistories()).isEqualTo(1);
        assertThat(countRows("idempotency_records")).isEqualTo(1);
        assertThat(issuanceStatus()).isEqualTo("USED");
        assertThat(activeCount()).isEqualTo(1);
    }

    private CouponUseResult executeUse(String key, String requestHash) {
        return transactionTemplate.execute(status -> {
            assertThat(idempotencyRepository.tryStart(
                    key,
                    requestHash,
                    USED_AT
            )).isTrue();
            CouponUseResult result = couponUseService.use(command(key));
            idempotencyRepository.complete(
                    key,
                    20L,
                    100L,
                    "stored-response",
                    USED_AT
            );
            return result;
        });
    }

    private String processIdempotentRequest(CountDownLatch completed)
            throws InterruptedException {
        boolean firstRequest = Boolean.TRUE.equals(transactionTemplate.execute(
                status -> idempotencyRepository.tryStart(
                        IDEMPOTENCY_KEY,
                        REQUEST_HASH,
                        USED_AT
                )
        ));
        if (firstRequest) {
            try {
                return transactionTemplate.execute(status -> {
                    couponUseService.use(command(IDEMPOTENCY_KEY));
                    idempotencyRepository.complete(
                            IDEMPOTENCY_KEY,
                            20L,
                            100L,
                            "stored-response",
                            USED_AT
                    );
                    return "stored-response";
                });
            } finally {
                completed.countDown();
            }
        }

        if (!completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                    "최초 멱등 요청 완료를 기다리지 못했습니다."
            );
        }
        IdempotencyRecord record = transactionTemplate.execute(
                status -> idempotencyRepository
                        .findByKey(IDEMPOTENCY_KEY)
                        .orElseThrow()
        );
        assertThat(record.status()).isEqualTo(IdempotencyStatus.DONE);
        assertThat(record.requestHash()).isEqualTo(REQUEST_HASH);
        return record.responseBody();
    }

    private CouponUseCommand command(String idempotencyKey) {
        return new CouponUseCommand(
                100L,
                20L,
                30L,
                20_000,
                idempotencyKey,
                USED_AT
        );
    }

    private void resetData() {
        jdbcTemplate.update("DELETE FROM idempotency_records");
        jdbcTemplate.update("DELETE FROM issuance_usages");
        jdbcTemplate.update("DELETE FROM issuance_histories");
        jdbcTemplate.update("DELETE FROM issuances");
        jdbcTemplate.update("DELETE FROM coupon_stocks");
        jdbcTemplate.update("DELETE FROM coupons");
        jdbcTemplate.update("DELETE FROM coupon_templates");
        jdbcTemplate.update("DELETE FROM members");
        jdbcTemplate.update("DELETE FROM brands");
        jdbcTemplate.update("DELETE FROM grades");
    }

    private void saveCouponData() {
        jdbcTemplate.batchUpdate(
                "INSERT INTO grades (code, bit_value) VALUES (?, ?)",
                List.of(
                        new Object[]{"WELCOME", 1},
                        new Object[]{"SILVER", 2},
                        new Object[]{"GOLD", 4},
                        new Object[]{"VIP", 8}
                )
        );
        jdbcTemplate.update(
                "INSERT INTO brands (id, name, category) VALUES (?, ?, ?)",
                1L,
                "테스트 브랜드",
                "카페"
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    discount_amount, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                1L,
                1L,
                "정액 5천원 할인",
                "FIXED_AMOUNT",
                5_000,
                7,
                3,
                "TUE",
                LocalTime.of(14, 0),
                2,
                10,
                12,
                true
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupons (
                    id, template_id, brand_id, name, policy_type,
                    discount_amount, valid_days, eligible_grades_mask,
                    open_at, close_at, status, generated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                10L,
                1L,
                1L,
                "정액 5천원 할인",
                "FIXED_AMOUNT",
                5_000,
                7,
                12,
                LocalDateTime.of(2026, 8, 18, 5, 0),
                LocalDateTime.of(2026, 8, 18, 7, 0),
                "OPEN",
                LocalDateTime.of(2026, 8, 17, 0, 0),
                LocalDateTime.of(2026, 8, 17, 0, 1)
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_stocks (
                    coupon_id, total_quantity, active_count, updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                10L,
                10,
                1,
                LocalDateTime.of(2026, 8, 18, 5, 0)
        );
        jdbcTemplate.update(
                """
                INSERT INTO members (id, membership_grade, created_at)
                VALUES (?, ?, ?)
                """,
                20L,
                "GOLD",
                LocalDateTime.of(2026, 1, 1, 0, 0)
        );
        jdbcTemplate.update(
                """
                INSERT INTO issuances (
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                "GOLD",
                "ISSUED",
                LocalDateTime.of(2026, 8, 18, 5, 30),
                LocalDateTime.of(2026, 8, 25, 5, 30),
                LocalDateTime.of(2026, 8, 18, 5, 30),
                LocalDateTime.of(2026, 8, 18, 5, 30)
        );
    }

    private int activeCount() {
        return jdbcTemplate.queryForObject(
                "SELECT active_count FROM coupon_stocks WHERE coupon_id = 10",
                Integer.class
        );
    }

    private String issuanceStatus() {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM issuances WHERE id = 100",
                String.class
        );
    }

    private int countUseHistories() {
        return jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuance_histories
                WHERE issuance_id = 100 AND event_type = 'USE'
                """,
                Integer.class
        );
    }

    private int countRows(String tableName) {
        String query = switch (tableName) {
            case "issuance_usages" ->
                    "SELECT COUNT(*) FROM issuance_usages";
            case "idempotency_records" ->
                    "SELECT COUNT(*) FROM idempotency_records";
            default -> throw new IllegalArgumentException(
                    "허용되지 않은 테스트 테이블입니다."
            );
        };
        return jdbcTemplate.queryForObject(query, Integer.class);
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableJpaAuditing(dateTimeProviderRef = "couponUseTestDateTimeProvider")
    static class AuditTestConfig {

        @Bean
        DateTimeProvider couponUseTestDateTimeProvider() {
            return () -> Optional.of(USED_AT);
        }
    }
}
