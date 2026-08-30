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
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.expiration.ExpirationRepository;
import com.kafkick.core.expiration.ExpireCandidate;
import com.kafkick.storage.db.expiration.ExpirationJdbcAdapter;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.coupon.port.OrderNumberGenerator;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.coupon.service.result.CouponUseResult;
import com.kafkick.core.coupon.service.CouponUseService;
import com.kafkick.core.coupon.service.command.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.CouponCancelUseService;
import com.kafkick.core.coupon.service.command.CouponCancelCommand;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.CouponCancelService;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.CouponIssueService;
import com.kafkick.core.notification.NotificationRequestService;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// 실제 MySQL에서 쿠폰 사용·멱등 응답·조건부 상태 전이의 원자성을 검증합니다.

@RepositoryTest
@Import({
        CouponRoundRepositoryImpl.class,
        CouponStockRepositoryImpl.class,
        IssuanceRepositoryImpl.class,
        IssuanceUsageRepositoryImpl.class,
        OrderNumberGeneratorImpl.class,
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
    private CouponStockRepository couponStockRepository;

    @Autowired
    private IssuanceRepositoryImpl issuanceRepository;

    @Autowired
    private IssuanceUsageRepository issuanceUsageRepository;

    @Autowired
    private IssuanceHistoryRepository issuanceHistoryRepository;

    @Autowired
    private OrderNumberGenerator orderNumberGenerator;

    @Autowired
    private IdempotencyRepository idempotencyRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /** 살아 있는 만료 경로. JdbcClient 하나만 받으므로 테스트에서 바로 조립한다. */
    private ExpirationRepository expiration;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private CouponUseService couponUseService;
    private CouponCancelUseService couponCancelUseService;
    private CouponCancelService couponCancelService;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        resetData();
        saveCouponData();
        transactionTemplate = new TransactionTemplate(transactionManager);
        expiration = new ExpirationJdbcAdapter(JdbcClient.create(jdbcTemplate.getDataSource()));
        couponUseService = new CouponUseService(
                issuanceRepository,
                couponRoundRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository,
                orderNumberGenerator
        );
        couponCancelUseService = new CouponCancelUseService(
                issuanceRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
        couponCancelService = new CouponCancelService(
                issuanceRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
    }


    /**
     * <b>살아 있는 만료 경로로 만료시킨다.</b> {@code ExpireJobConfig} 의 청크가 도는
     * 네 문장을 같은 순서로 부른다 — 재고 락 → 조건부 UPDATE → 이력 → 재고 반환.
     *
     * <p><b>왜 서비스가 아니라 이것인가.</b> 이 파일의 경합 검사들은 원래
     * {@code CouponExpirationService} 를 만료 주체로 썼는데 그 서비스가 <b>호출자 0</b> 이라
     * CY-769 에서 지웠다. 그런데 검사가 재는 것은 서비스가 아니라
     * <b>{@code active_count} 가 현재 보유량과 일치하는가</b> — 이 과제의 본체다. 그리고
     * <b>발급 ↔ 만료 · 사용 ↔ 만료 경합은 배치 테스트에 없다</b>(실측: 배치 테스트가
     * {@code CouponIssueService}·{@code CouponUseService} 를 0회 참조한다). 그냥 지우면
     * 커버리지가 셋 빈다. 그래서 <b>주체만 살아 있는 경로로 갈아끼웠다.</b>
     *
     * <p>⚠️ <b>{@code lockStock} 이 실패하면 0 을 돌려준다 — 정상적인 "만료할 것이 없다"
     * 와 같은 값이다.</b> 배치도 그 자리에서 청크를 넘기므로 모양은 같지만, 검사에서는
     * 그 둘이 구분이 안 된다는 것을 알고 봐야 한다. 재고 행이 없는 회차(그럴 일이 없게
     * 시드가 함께 만든다)나 락 대기 초과가 그 갈래다 — 경합 검사가 예상과 다른 수를 내면
     * <b>여기부터 의심한다.</b>
     *
     * @return 만료된 건수. 조건부 UPDATE 의 매치 수라 "실제로 우리가 바꾼 행" 이다.
     *         {@code lockStock} 실패도 0 이다
     */
    private int expireViaBatchPath(long couponId, List<ExpireCandidate> candidates, Instant asOf) {
        LocalDateTime at = LocalDateTime.ofInstant(asOf, ZoneOffset.UTC);
        long afterId = candidates.stream().mapToLong(ExpireCandidate::id).min().orElseThrow() - 1;
        long lastId = candidates.stream().mapToLong(ExpireCandidate::id).max().orElseThrow();

        // 배치와 같은 순서다. 재고를 먼저 잠가야 발급·취소와 잠금 순서가 통일된다.
        if (!expiration.lockStock(couponId)) {
            return 0;
        }
        int expired = expiration.expireBatch(at, at, afterId, lastId, couponId);
        if (expired == 0) {
            return 0;
        }
        expiration.appendExpireHistories(at, at, afterId, lastId, couponId);
        expiration.releaseStock(couponId, expired, at);
        return expired;
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
                .isEqualTo(result.orderId());
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
    @DisplayName("사용 취소 후 재사용하면 서버가 새로운 주문번호를 발급한다")
    void generateNewOrderNumberWhenReusingCanceledCoupon() {
        String firstUseKey =
                "60400000-0000-4000-8000-000000000001";
        String cancelUseKey =
                "60400000-0000-4000-8000-000000000002";
        String secondUseKey =
                "60400000-0000-4000-8000-000000000003";
        Instant canceledAt = USED_AT.plusSeconds(60);
        Instant reusedAt = USED_AT.plusSeconds(120);

        CouponUseResult first = executeUseAt(
                firstUseKey,
                REQUEST_HASH,
                USED_AT
        );
        executeCancelUse(cancelUseKey, canceledAt);
        CouponUseResult second = executeUseAt(
                secondUseKey,
                REQUEST_HASH,
                reusedAt
        );

        assertThat(first.orderId()).isPositive();
        assertThat(second.orderId()).isPositive().isNotEqualTo(first.orderId());
        assertThat(jdbcTemplate.queryForList(
                """
                SELECT order_id
                FROM issuance_usages
                WHERE issuance_id = 100
                ORDER BY id
                """,
                Long.class
        )).containsExactly(first.orderId(), second.orderId());
        assertThat(activeUsageCount()).isEqualTo(1);
        assertThat(issuanceStatus()).isEqualTo("USED");
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
    @DisplayName("같은 발급 건과 주문의 사용 실적은 중복 저장할 수 없다")
    void rejectDuplicateIssuanceAndOrderUsage() {
        insertUsage(30L, USED_AT.plusSeconds(60));
        int rowCountBeforeFailure = countRows("issuance_usages");

        assertThatThrownBy(() ->
                insertUsage(30L, USED_AT.plusSeconds(120))
        ).isInstanceOf(DuplicateKeyException.class);
        assertThat(countRows("issuance_usages"))
                .isEqualTo(rowCountBeforeFailure);
    }

    @Test
    @DisplayName("한 발급 건에는 활성 사용 실적을 하나만 저장할 수 있다")
    void rejectMultipleActiveUsagesForIssuance() {
        insertUsage(30L, null);
        int rowCountBeforeFailure = countRows("issuance_usages");

        assertThatThrownBy(() -> insertUsage(31L, null))
                .isInstanceOf(DuplicateKeyException.class);
        assertThat(countRows("issuance_usages"))
                .isEqualTo(rowCountBeforeFailure);
    }

    @Test
    @DisplayName("취소된 사용 이력을 보존하면서 다른 주문으로 다시 사용할 수 있다")
    void allowReuseAfterCanceledUsage() {
        insertUsage(30L, USED_AT.plusSeconds(60));
        insertUsage(31L, null);

        List<Map<String, Object>> usages = jdbcTemplate.queryForList(
                """
                SELECT order_id, canceled_at
                FROM issuance_usages
                WHERE issuance_id = 100
                ORDER BY order_id
                """
        );

        assertThat(usages).hasSize(2);
        assertThat(((Number) usages.get(0).get("order_id")).longValue())
                .isEqualTo(30L);
        assertThat(usages.get(0).get("canceled_at")).isNotNull();
        assertThat(((Number) usages.get(1).get("order_id")).longValue())
                .isEqualTo(31L);
        assertThat(usages.get(1).get("canceled_at")).isNull();
    }

    @Test
    @DisplayName("사용 시각보다 빠른 취소 시각은 DB 제약으로 거부한다")
    void rejectCancellationBeforeUsageAtDatabase() {
        assertThatThrownBy(() ->
                insertUsage(30L, USED_AT.minusSeconds(1))
        ).isInstanceOfSatisfying(
                UncategorizedSQLException.class,
                exception -> {
                    assertThat(exception.getSQLException().getErrorCode())
                            .isEqualTo(3819);
                    assertThat(exception.getSQLException().getMessage())
                            .contains("ck_issuance_usages_cancel_time");
                }
        );
        assertThat(countRows("issuance_usages")).isZero();
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

    @Test
    @DisplayName("만료 전 사용 취소는 상태와 실적을 갱신하고 현재 보유량을 유지한다")
    void cancelUseBeforeExpirationAtomically() {
        Instant canceledAt = USED_AT.plusSeconds(60);
        prepareUsedIssuance(Instant.parse("2026-08-25T05:30:00Z"));

        CouponCancelUseResult result = executeCancelUse(
                IDEMPOTENCY_KEY,
                canceledAt
        );

        assertThat(result.status()).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(issuanceStatus()).isEqualTo("ISSUED");
        assertThat(activeCount()).isEqualTo(1);
        assertThat(activeUsageCount()).isZero();
        assertThat(canceledAt(30L)).isNotNull();
        assertThat(countCancelUseHistories()).isEqualTo(1);
        assertThat(idempotencyStatus(IDEMPOTENCY_KEY)).isEqualTo("DONE");
    }

    @Test
    @DisplayName("만료 후 사용 취소는 EXPIRED로 전이하고 현재 보유량을 한 번 감소시킨다")
    void cancelExpiredUseAndReleaseStockAtomically() {
        Instant canceledAt = USED_AT.plusSeconds(60);
        prepareUsedIssuance(USED_AT.plusSeconds(30));

        CouponCancelUseResult result = executeCancelUse(
                IDEMPOTENCY_KEY,
                canceledAt
        );

        assertThat(result.status()).isEqualTo(IssuanceStatus.EXPIRED);
        assertThat(issuanceStatus()).isEqualTo("EXPIRED");
        assertThat(activeCount()).isZero();
        assertThat(activeUsageCount()).isZero();
        assertThat(canceledAt(30L)).isNotNull();
        assertThat(countCancelUseHistories()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 만료 쿠폰 사용 취소를 동시에 5회 요청해도 재고와 이력은 한 번만 반영된다")
    void cancelExpiredUseConcurrentlyOnlyOnce() throws Exception {
        int requestCount = 5;
        Instant canceledAt = USED_AT.plusSeconds(60);
        prepareUsedIssuance(USED_AT.plusSeconds(30));
        executor = Executors.newFixedThreadPool(requestCount);
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int index = 0; index < requestCount; index++) {
            String key = "550e8400-e29b-41d4-a716-44665544000" + index;
            futures.add(executor.submit(() -> {
                ready.countDown();
                if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "동시 사용 취소 시작 신호를 기다리지 못했습니다."
                    );
                }
                try {
                    transactionTemplate.executeWithoutResult(status ->
                            couponCancelUseService.cancelUse(
                                    cancelCommand(key, canceledAt)
                            )
                    );
                    return true;
                } catch (BusinessException exception) {
                    assertThat(exception.getErrorCode()).isEqualTo(
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
        assertThat(issuanceStatus()).isEqualTo("EXPIRED");
        assertThat(activeCount()).isZero();
        assertThat(activeUsageCount()).isZero();
        assertThat(countCancelUseHistories()).isEqualTo(1);
    }

    @Test
    @DisplayName("발급 취소는 CANCELLED 상태·재고 감소·CANCEL 이력을 한 트랜잭션으로 반영한다")
    void cancelIssuanceAtomically() {
        Instant canceledAt = USED_AT.plusSeconds(60);

        CouponCancelResult result = transactionTemplate.execute(status ->
                couponCancelService.cancel(new CouponCancelCommand(
                        100L,
                        20L,
                        IDEMPOTENCY_KEY,
                        canceledAt
                ))
        );

        assertThat(result.status()).isEqualTo(IssuanceStatus.CANCELLED);
        assertThat(issuanceStatus()).isEqualTo("CANCELLED");
        assertThat(activeCount()).isZero();
        assertThat(countCancelHistories()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 ISSUED 쿠폰을 동시에 5회 취소해도 재고와 CANCEL 이력은 한 번만 반영된다")
    void cancelIssuanceConcurrentlyOnlyOnce() throws Exception {
        int requestCount = 5;
        Instant canceledAt = USED_AT.plusSeconds(60);
        executor = Executors.newFixedThreadPool(requestCount);
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int index = 0; index < requestCount; index++) {
            String key = "550e8400-e29b-41d4-a716-44665544001" + index;
            futures.add(executor.submit(() -> {
                ready.countDown();
                if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "동시 발급 취소 시작 신호를 기다리지 못했습니다."
                    );
                }
                try {
                    transactionTemplate.executeWithoutResult(status ->
                            couponCancelService.cancel(
                                    new CouponCancelCommand(
                                            100L,
                                            20L,
                                            key,
                                            canceledAt
                                    )
                            )
                    );
                    return true;
                } catch (BusinessException exception) {
                    assertThat(exception.getErrorCode()).isEqualTo(
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
        assertThat(issuanceStatus()).isEqualTo("CANCELLED");
        assertThat(activeCount()).isZero();
        assertThat(countCancelHistories()).isEqualTo(1);
    }




    @Test
    @DisplayName("신규 발급과 만료가 동시에 실행돼도 active_count는 현재 보유량과 일치한다")
    void keepStockInvariantDuringIssueAndExpiration() throws Exception {
        Instant asOf = Instant.parse("2026-08-26T05:30:00Z");
        Instant issuedAt = USED_AT;
        jdbcTemplate.update(
                """
                UPDATE coupons
                SET open_at = ?, close_at = ?, status = 'OPEN'
                WHERE id = 10
                """,
                Timestamp.from(issuedAt.minusSeconds(3_600)),
                Timestamp.from(issuedAt.plusSeconds(3_600))
        );
        jdbcTemplate.update(
                """
                INSERT INTO members (id, membership_grade, created_at)
                VALUES (21, 'GOLD', ?)
                """,
                Timestamp.from(issuedAt)
        );
        List<ExpireCandidate> candidates = expiration.nextCandidates(
                LocalDateTime.ofInstant(asOf, ZoneOffset.UTC), 0L, 10, List.of());
        CouponIssueService issueService = new CouponIssueService(
                couponRoundRepository,
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                () -> "DEFGHJKLM2345678",
                org.mockito.Mockito.mock(NotificationRequestService.class)
        );
        executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        Future<?> expiration = executor.submit(() -> {
            ready.countDown();
            awaitStart(start);
            transactionTemplate.executeWithoutResult(status ->
                    expireViaBatchPath(10L, candidates, asOf)
            );
        });
        Future<?> issuance = executor.submit(() -> {
            ready.countDown();
            awaitStart(start);
            transactionTemplate.executeWithoutResult(status ->
                    issueService.issue(new CouponIssueCommand(
                            10L,
                            21L,
                            MembershipGrade.GOLD,
                            "22300000-0000-4000-8000-000000000001",
                            issuedAt
                    ))
            );
        });

        assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        expiration.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        issuance.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        int activeIssuanceCount = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuances
                WHERE coupon_id = 10 AND status IN ('ISSUED', 'USED')
                """,
                Integer.class
        );
        assertThat(activeCount()).isEqualTo(activeIssuanceCount);
        assertThat(activeCount()).isEqualTo(1);
        assertThat(issuanceStatus()).isEqualTo("EXPIRED");
        assertThat(jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuance_histories
                WHERE event_type IN ('ISSUE', 'EXPIRE')
                """,
                Integer.class
        )).isEqualTo(2);
    }

    @Test
    @DisplayName("만료 3건과 신규 발급 3건이 경합해도 현재 보유량이 일치한다")
    void keepStockInvariantDuringMultipleIssuesAndExpirations()
            throws Exception {
        Instant asOf = Instant.parse("2026-08-26T05:30:00Z");
        Instant issuedAt = USED_AT;
        insertIssuance(
                101L,
                21L,
                "BCDEFGHJKLM23456",
                "ISSUED",
                Instant.parse("2026-08-25T05:30:00Z")
        );
        insertIssuance(
                102L,
                22L,
                "CDEFGHJKLM234567",
                "ISSUED",
                Instant.parse("2026-08-25T05:30:00Z")
        );
        jdbcTemplate.update(
                "UPDATE coupon_stocks SET active_count = 3 WHERE coupon_id = 10"
        );
        jdbcTemplate.update(
                """
                UPDATE coupons
                SET open_at = ?, close_at = ?, status = 'OPEN'
                WHERE id = 10
                """,
                Timestamp.from(issuedAt.minusSeconds(3_600)),
                Timestamp.from(issuedAt.plusSeconds(3_600))
        );
        for (long memberId = 23L; memberId <= 25L; memberId++) {
            jdbcTemplate.update(
                    """
                    INSERT INTO members (id, membership_grade, created_at)
                    VALUES (?, 'GOLD', ?)
                    """,
                    memberId,
                    Timestamp.from(issuedAt)
            );
        }
        List<ExpireCandidate> candidates = expiration.nextCandidates(
                LocalDateTime.ofInstant(asOf, ZoneOffset.UTC), 0L, 10, List.of());
        AtomicInteger codeSequence = new AtomicInteger();
        CouponIssueService issueService = new CouponIssueService(
                couponRoundRepository,
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                () -> String.format(
                        "E%015d",
                        codeSequence.incrementAndGet()
                ),
                org.mockito.Mockito.mock(NotificationRequestService.class)
        );
        executor = Executors.newFixedThreadPool(4);
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        futures.add(executor.submit(() -> {
            ready.countDown();
            awaitStart(start);
            transactionTemplate.executeWithoutResult(status ->
                    expireViaBatchPath(10L, candidates, asOf)
            );
        }));
        for (long memberId = 23L; memberId <= 25L; memberId++) {
            long targetMemberId = memberId;
            futures.add(executor.submit(() -> {
                ready.countDown();
                awaitStart(start);
                transactionTemplate.executeWithoutResult(status ->
                        issueService.issue(new CouponIssueCommand(
                                10L,
                                targetMemberId,
                                MembershipGrade.GOLD,
                                String.format(
                                        "22300000-0000-4000-8000-%012d",
                                        targetMemberId
                                ),
                                issuedAt
                        ))
                );
            }));
        }

        assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        for (Future<?> future : futures) {
            future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        int activeIssuanceCount = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuances
                WHERE coupon_id = 10 AND status IN ('ISSUED', 'USED')
                """,
                Integer.class
        );
        assertThat(activeCount()).isEqualTo(activeIssuanceCount);
        assertThat(activeCount()).isEqualTo(3);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM issuances WHERE status = 'EXPIRED'",
                Integer.class
        )).isEqualTo(3);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM issuance_histories WHERE event_type = 'EXPIRE'",
                Integer.class
        )).isEqualTo(3);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM issuance_histories WHERE event_type = 'ISSUE'",
                Integer.class
        )).isEqualTo(3);
    }

    @Test
    @DisplayName("발급 취소와 만료가 동시에 실행돼도 재고와 종단 이력은 한 번만 반영된다")
    void releaseStockOnceDuringCancelAndExpiration() throws Exception {
        Instant asOf = Instant.parse("2026-08-26T05:30:00Z");
        Instant canceledAt = Instant.parse("2026-08-25T05:29:59Z");
        List<ExpireCandidate> candidates = expiration.nextCandidates(
                LocalDateTime.ofInstant(asOf, ZoneOffset.UTC), 0L, 10, List.of());
        executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        Future<Boolean> expiration = executor.submit(() -> {
            ready.countDown();
            awaitStart(start);
            int result = transactionTemplate.execute(
                    status -> expireViaBatchPath(10L, candidates, asOf)
            );
            return result == 1;
        });
        Future<Boolean> cancellation = executor.submit(() -> {
            ready.countDown();
            awaitStart(start);
            try {
                transactionTemplate.executeWithoutResult(status ->
                        couponCancelService.cancel(
                                new CouponCancelCommand(
                                        100L,
                                        20L,
                                        "22300000-0000-4000-8000-000000000002",
                                        canceledAt
                                )
                        )
                );
                return true;
            } catch (BusinessException exception) {
                assertThat(exception.getErrorCode()).isEqualTo(
                        CouponIssueErrorCode.INVALID_TRANSITION
                );
                return false;
            }
        });

        assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        assertThat(List.of(
                expiration.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                cancellation.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        )).containsExactlyInAnyOrder(true, false);

        assertThat(activeCount()).isZero();
        assertThat(issuanceStatus()).isIn("CANCELLED", "EXPIRED");
        assertThat(jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuance_histories
                WHERE issuance_id = 100
                  AND event_type IN ('CANCEL', 'EXPIRE')
                """,
                Integer.class
        )).isEqualTo(1);
    }

    @Test
    @DisplayName("쿠폰 사용과 만료가 동시에 실행돼도 상태와 active_count가 일치한다")
    void keepStateAndStockConsistentDuringUseAndExpiration()
            throws Exception {
        Instant asOf = Instant.parse("2026-08-26T05:30:00Z");
        Instant usedAt = Instant.parse("2026-08-25T05:29:59Z");
        List<ExpireCandidate> candidates = expiration.nextCandidates(
                LocalDateTime.ofInstant(asOf, ZoneOffset.UTC), 0L, 10, List.of());
        executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        Future<Boolean> expiration = executor.submit(() -> {
            ready.countDown();
            awaitStart(start);
            int result = transactionTemplate.execute(
                    status -> expireViaBatchPath(10L, candidates, asOf)
            );
            return result == 1;
        });
        Future<Boolean> usage = executor.submit(() -> {
            ready.countDown();
            awaitStart(start);
            try {
                transactionTemplate.executeWithoutResult(status ->
                        couponUseService.use(new CouponUseCommand(
                                100L,
                                20L,
                                20_000,
                                "22300000-0000-4000-8000-000000000003",
                                usedAt
                        ))
                );
                return true;
            } catch (BusinessException exception) {
                assertThat(exception.getErrorCode()).isEqualTo(
                        CouponIssueErrorCode.INVALID_TRANSITION
                );
                return false;
            }
        });

        assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        assertThat(List.of(
                expiration.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                usage.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        )).containsExactlyInAnyOrder(true, false);

        String finalStatus = issuanceStatus();
        int activeIssuanceCount = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuances
                WHERE coupon_id = 10 AND status IN ('ISSUED', 'USED')
                """,
                Integer.class
        );
        assertThat(finalStatus).isIn("USED", "EXPIRED");
        assertThat(activeCount()).isEqualTo(activeIssuanceCount);
        assertThat(jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuance_histories
                WHERE issuance_id = 100
                  AND event_type IN ('USE', 'EXPIRE')
                """,
                Integer.class
        )).isEqualTo(1);
    }

    private CouponUseResult executeUse(String key, String requestHash) {
        return executeUseAt(key, requestHash, USED_AT);
    }

    private CouponUseResult executeUseAt(
            String key,
            String requestHash,
            Instant usedAt
    ) {
        return transactionTemplate.execute(status -> {
            assertThat(idempotencyRepository.tryStart(
                    key,
                    requestHash,
                    usedAt
            )).isTrue();
            CouponUseResult result = couponUseService.use(
                    command(key, usedAt)
            );
            idempotencyRepository.complete(
                    key,
                    20L,
                    100L,
                    "stored-response",
                    usedAt
            );
            return result;
        });
    }

    private CouponCancelUseResult executeCancelUse(
            String key,
            Instant canceledAt
    ) {
        return transactionTemplate.execute(status -> {
            assertThat(idempotencyRepository.tryStart(
                    key,
                    REQUEST_HASH,
                    canceledAt
            )).isTrue();
            CouponCancelUseResult result = couponCancelUseService.cancelUse(
                    cancelCommand(key, canceledAt)
            );
            idempotencyRepository.complete(
                    key,
                    20L,
                    100L,
                    "stored-cancel-response",
                    canceledAt
            );
            return result;
        });
    }

    private CouponCancelUseCommand cancelCommand(
            String key,
            Instant canceledAt
    ) {
        return new CouponCancelUseCommand(
                100L,
                20L,
                key,
                canceledAt
        );
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
        return command(idempotencyKey, USED_AT);
    }

    private CouponUseCommand command(
            String idempotencyKey,
            Instant usedAt
    ) {
        return new CouponUseCommand(
                100L,
                20L,
                20_000,
                idempotencyKey,
                usedAt
        );
    }

    private void resetData() {
        jdbcTemplate.update("DELETE FROM idempotency_records");
        jdbcTemplate.update("DELETE FROM issuance_usages");
        jdbcTemplate.update("DELETE FROM coupon_order_numbers");
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

    private void insertUsage(Long orderId, Instant canceledAt) {
        jdbcTemplate.update(
                """
                INSERT INTO issuance_usages (
                    issuance_id, order_id, discount_amount,
                    used_at, canceled_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                100L,
                orderId,
                5_000,
                Timestamp.from(USED_AT),
                canceledAt == null ? null : Timestamp.from(canceledAt),
                Timestamp.from(USED_AT)
        );
    }

    private void insertIssuance(
            Long issuanceId,
            Long memberId,
            String code,
            String status,
            Instant expiresAt
    ) {
        jdbcTemplate.update(
                """
                INSERT INTO members (id, membership_grade, created_at)
                VALUES (?, 'GOLD', ?)
                """,
                memberId,
                Timestamp.from(USED_AT)
        );
        jdbcTemplate.update(
                """
                INSERT INTO issuances (
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at
                ) VALUES (?, 10, ?, ?, 'GOLD', ?, ?, ?, ?, ?)
                """,
                issuanceId,
                memberId,
                code,
                status,
                Timestamp.from(USED_AT),
                Timestamp.from(expiresAt),
                Timestamp.from(USED_AT),
                Timestamp.from(USED_AT)
        );
    }

    private static void awaitStart(CountDownLatch start) {
        try {
            if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "동시성 테스트 시작 신호를 기다리지 못했습니다."
                );
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "동시성 테스트 시작 대기가 중단되었습니다.",
                    exception
            );
        }
    }

    private void prepareUsedIssuance(Instant expiresAt) {
        jdbcTemplate.update(
                """
                UPDATE issuances
                SET status = 'USED', expires_at = ?, updated_at = ?
                WHERE id = 100
                """,
                Timestamp.from(expiresAt),
                Timestamp.from(USED_AT)
        );
        insertUsage(30L, null);
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

    private int countCancelUseHistories() {
        return jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuance_histories
                WHERE issuance_id = 100 AND event_type = 'CANCEL_USE'
                """,
                Integer.class
        );
    }

    private int countCancelHistories() {
        return jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuance_histories
                WHERE issuance_id = 100 AND event_type = 'CANCEL'
                """,
                Integer.class
        );
    }

    private int activeUsageCount() {
        return jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM issuance_usages
                WHERE issuance_id = 100 AND canceled_at IS NULL
                """,
                Integer.class
        );
    }

    private LocalDateTime canceledAt(Long orderId) {
        return jdbcTemplate.queryForObject(
                """
                SELECT canceled_at
                FROM issuance_usages
                WHERE issuance_id = 100 AND order_id = ?
                """,
                LocalDateTime.class,
                orderId
        );
    }

    private String idempotencyStatus(String key) {
        return jdbcTemplate.queryForObject(
                """
                SELECT status
                FROM idempotency_records
                WHERE idem_key = ?
                """,
                String.class,
                key
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

    @TestConfiguration
    @EnableJpaAuditing(dateTimeProviderRef = "couponUseTestDateTimeProvider")
    static class AuditTestConfig {

        @Bean
        DateTimeProvider couponUseTestDateTimeProvider() {
            return () -> Optional.of(USED_AT);
        }
    }
}
