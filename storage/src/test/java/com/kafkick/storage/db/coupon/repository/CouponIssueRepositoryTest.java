package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponStockOverflowException;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.query.CouponIssuePolicySnapshot;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.CouponIssueService;
import com.kafkick.core.coupon.service.code.CouponCodeGenerator;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.RequestTokenGenerator;
import com.kafkick.core.coupon.v2.V2CouponIssueResult;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.V2CouponIssueException;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.notification.NotificationRequestService;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 실제 MySQL 조건부 UPDATE로 발급·재고·이력 원자성과 1인 1매를 검증합니다.

@RepositoryTest
@Import({
        CouponRoundRepositoryImpl.class,
        CouponStockRepositoryImpl.class,
        IssuanceRepositoryImpl.class,
        IssuanceHistoryRepositoryImpl.class,
        IdempotencyRepositoryImpl.class,
        CouponIssueRepositoryTest.AuditTestConfig.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class CouponIssueRepositoryTest {

    private static final long TIMEOUT_SECONDS = 30;
    private static final int DEADLOCK_MAX_ATTEMPTS = 3;
    private static final Instant ISSUED_AT =
            Instant.parse("2026-08-18T05:30:00Z");
    private static final Instant AUDIT_CREATED_AT =
            Instant.parse("2026-08-18T05:30:05Z");
    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String REQUEST_HASH = "a".repeat(64);

    @Autowired
    private CouponRoundRepository couponRoundRepository;

    @Autowired
    private CouponStockRepository couponStockRepository;

    @Autowired
    private IssuanceRepository issuanceRepository;

    @Autowired
    private IssuanceHistoryRepository issuanceHistoryRepository;

    @Autowired
    private IdempotencyRepository idempotencyRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;
    private CouponIssueService couponIssueService;
    private AtomicLong codeSequence;

    @BeforeEach
    void setUp() {
        resetData();
        saveOpenCouponRound(10);
        saveMembers(20);
        transactionTemplate = new TransactionTemplate(transactionManager);
        codeSequence = new AtomicLong();
        couponIssueService = new CouponIssueService(
                couponRoundRepository,
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                () -> String.format("%016d", codeSequence.incrementAndGet()),
                org.mockito.Mockito.mock(NotificationRequestService.class)
        );
    }

    @Test
    @DisplayName("발급건·재고 점유·ISSUE 이력을 한 트랜잭션으로 저장한다")
    void issueCouponAtomically() {
        Issuance issuance = issue(1L);

        Map<String, Object> issuanceRow = jdbcTemplate.queryForMap(
                """
                SELECT coupon_id, member_id, code, issued_grade, status,
                       issued_at, expires_at, created_at, updated_at
                FROM issuances
                WHERE id = ?
                """,
                issuance.id()
        );
        Map<String, Object> historyRow = jdbcTemplate.queryForMap(
                """
                SELECT issuance_id, event_type, from_status, to_status,
                       request_id, created_at
                FROM issuance_histories
                WHERE issuance_id = ?
                """,
                issuance.id()
        );

        assertThat(((Number) issuanceRow.get("coupon_id")).longValue())
                .isEqualTo(10L);
        assertThat(((Number) issuanceRow.get("member_id")).longValue())
                .isEqualTo(1L);
        assertThat(issuanceRow.get("issued_grade")).isEqualTo("GOLD");
        assertThat(issuanceRow.get("status")).isEqualTo("ISSUED");
        assertThat(issuanceRow.get("issued_at"))
                .isEqualTo(LocalDateTime.of(2026, 8, 18, 5, 30));
        assertThat(issuanceRow.get("expires_at"))
                .isEqualTo(LocalDateTime.of(2026, 8, 25, 5, 30));
        assertThat(issuanceRow.get("created_at"))
                .isEqualTo(LocalDateTime.of(2026, 8, 18, 5, 30, 5));
        assertThat(issuance.id()).isNotNull();
        assertThat(issuance.code()).isEqualTo("0000000000000001");
        assertThat(issuance.issuedAt()).isEqualTo(ISSUED_AT);
        assertThat(issuance.expiresAt())
                .isEqualTo(issuance.issuedAt().plusSeconds(7L * 24 * 60 * 60));
        assertThat(activeCount()).isEqualTo(1);
        assertThat(((Number) historyRow.get("issuance_id")).longValue())
                .isEqualTo(issuance.id());
        assertThat(historyRow.get("event_type")).isEqualTo("ISSUE");
        assertThat(historyRow.get("from_status")).isNull();
        assertThat(historyRow.get("to_status")).isEqualTo("ISSUED");
        assertThat(historyRow.get("request_id")).isEqualTo("request-1");
    }

    @Test
    @DisplayName("v2 선점 뒤 발급·이력·DONE을 한 트랜잭션으로 저장하고 완료 승격한다")
    void issueV2AtomicallyAndCompleteClaim() {
        IssuanceGatePort gate = mock(IssuanceGatePort.class);
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(9L));
        when(gate.complete(any(Long.class), any(Long.class), any()))
                .thenReturn(CompleteOutcome.PROMOTED);
        V2CouponIssueService v2 = v2Service(gate);

        V2CouponIssueResult result = v2.issue(
                new CouponIssueCommand(10L, 1L, MembershipGrade.GOLD, IDEMPOTENCY_KEY, ISSUED_AT),
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.issueResult()).isPresent();
        assertThat(countRows("issuances")).isEqualTo(1);
        assertThat(countRows("issuance_histories")).isEqualTo(1);
        assertThat(countRows("idempotency_records")).isEqualTo(1);
        assertThat(activeCount()).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT updated_at FROM coupon_stocks WHERE coupon_id = 10", LocalDateTime.class))
                .isEqualTo(LocalDateTime.ofInstant(ISSUED_AT, java.time.ZoneOffset.UTC));
        verify(gate).complete(any(Long.class), any(Long.class), any());
    }

    @Test
    @DisplayName("갱신 시각이 없으면 DB 에 가기 전에 거절한다 — NOT NULL 위반을 재고 사고로 오분류하지 않는다")
    void rejectsNullUpdatedAtBeforeReachingTheDatabase() {
        assertThatThrownBy(() -> couponStockRepository.incrementActiveCount(10L, null))
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(CouponStockOverflowException.class);

        assertThat(activeCount()).isZero();
    }

    @Test
    @DisplayName("V2에서 DB CHECK가 총재고 초과 발급을 롤백하고 Redis 선점을 보상한다")
    void compensateV2WhenActiveCountCheckRejectsIssue() {
        jdbcTemplate.update("UPDATE coupon_stocks SET total_quantity = 1 WHERE coupon_id = 10");
        IssuanceGatePort gate = mock(IssuanceGatePort.class);
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(0L));
        when(gate.complete(any(Long.class), any(Long.class), any()))
                .thenReturn(CompleteOutcome.PROMOTED);
        when(gate.compensate(any(Long.class), any(Long.class), any()))
                .thenReturn(CompensateOutcome.REVERTED);
        V2CouponIssueService v2 = v2Service(gate);
        CouponRoundIssuanceDefinition definition = new CouponRoundIssuanceDefinition(
                10L, 7, EngineVersion.V2);

        v2.issue(new CouponIssueCommand(
                10L, 1L, MembershipGrade.GOLD, IDEMPOTENCY_KEY, ISSUED_AT), definition);

        assertThatThrownBy(() -> v2.issue(new CouponIssueCommand(
                10L, 2L, MembershipGrade.GOLD,
                "550e8400-e29b-41d4-a716-446655440001", ISSUED_AT), definition))
                .isInstanceOf(V2CouponIssueException.class)
                // CHECK 위반은 커넥션 끊김·락 타임아웃과 구분돼야 한다. 같은 예외로 뭉개면
                // 관제에 "MySQL 실패 1건" 으로만 찍혀 초과 발급 시도가 사라진다.
                .cause()
                .isInstanceOf(CouponStockOverflowException.class);

        assertThat(countRows("issuances")).isEqualTo(1);
        assertThat(countRows("issuance_histories")).isEqualTo(1);
        assertThat(countRows("idempotency_records")).isEqualTo(1);
        assertThat(activeCount()).isEqualTo(1);
        verify(gate).compensate(any(Long.class), any(Long.class), any());
    }

    @Test
    @DisplayName("V2 선점 20건이 동시에 와도 DB 카운터는 재고 10장을 넘지 않는다")
    void issueV2ExactlyAvailableStockConcurrently() throws Exception {
        IssuanceGatePort gate = mock(IssuanceGatePort.class);
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(0L));
        when(gate.complete(any(Long.class), any(Long.class), any()))
                .thenReturn(CompleteOutcome.PROMOTED);
        when(gate.compensate(any(Long.class), any(Long.class), any()))
                .thenReturn(CompensateOutcome.REVERTED);
        V2CouponIssueService v2 = v2Service(gate);
        CouponRoundIssuanceDefinition definition = new CouponRoundIssuanceDefinition(
                10L, 7, EngineVersion.V2);
        CountDownLatch ready = new CountDownLatch(20);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        List<Future<Boolean>> futures = new ArrayList<>();

        try {
            for (long memberId = 1; memberId <= 20; memberId++) {
                long requestedMemberId = memberId;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("V2 동시 발급 시작 대기 시간이 초과되었습니다.");
                    }
                    try {
                        return v2.issue(new CouponIssueCommand(
                                10L, requestedMemberId, MembershipGrade.GOLD,
                                "550e8400-e29b-41d4-a716-" + String.format("%012d", requestedMemberId),
                                ISSUED_AT), definition).issueResult().isPresent();
                    } catch (V2CouponIssueException expectedCheckFailure) {
                        return false;
                    }
                }));
            }
            assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            long succeeded = 0;
            for (Future<Boolean> future : futures) {
                if (future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    succeeded++;
                }
            }
            assertThat(succeeded).isEqualTo(10);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(countRows("issuances")).isEqualTo(10);
        assertThat(countRows("issuance_histories")).isEqualTo(10);
        assertThat(countRows("idempotency_records")).isEqualTo(10);
        assertThat(activeCount()).isEqualTo(10);
        verify(gate, times(10)).compensate(any(Long.class), any(Long.class), any());
    }

    @Test
    @DisplayName("v2 트랜잭션 실패 후 발급 행이 없으면 롤백하고 같은 토큰으로 보상한다")
    void compensateV2AfterRolledBackPersistence() {
        Issuance existing = issue(1L);
        transactionTemplate.executeWithoutResult(status -> idempotencyRepository.insertCompleted(
                IDEMPOTENCY_KEY, 1L, existing.id(), REQUEST_HASH, "existing", ISSUED_AT));
        IssuanceGatePort gate = mock(IssuanceGatePort.class);
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(9L));
        when(gate.compensate(any(Long.class), any(Long.class), any()))
                .thenReturn(CompensateOutcome.REVERTED);
        V2CouponIssueService v2 = v2Service(gate);

        assertThatThrownBy(() -> v2.issue(
                new CouponIssueCommand(10L, 2L, MembershipGrade.GOLD, IDEMPOTENCY_KEY, ISSUED_AT),
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOf(V2CouponIssueException.class);

        assertThat(issuanceRepository.findForCouponRoundMemberAndIdempotencyKey(
                10L, 2L, IDEMPOTENCY_KEY)).isEmpty();
        assertThat(countRows("issuances")).isEqualTo(1);
        assertThat(countRows("issuance_histories")).isEqualTo(1);
        verify(gate).compensate(any(Long.class), any(Long.class), any());
    }

    private V2CouponIssueService v2Service(IssuanceGatePort gate) {
        CouponCodeGenerator codes = () -> String.format(
                "%016d", codeSequence.incrementAndGet());
        return new V2CouponIssueService(
                gate,
                issuanceRepository,
                issuanceHistoryRepository,
                idempotencyRepository,
                couponStockRepository,
                codes,
                new com.kafkick.core.coupon.port.IdempotencyResultCodec<>() {
                    @Override
                    public String write(CouponIssueResult result) {
                        return "result-" + result.issuanceId();
                    }

                    @Override
                    public CouponIssueResult read(String responseBody) {
                        throw new UnsupportedOperationException();
                    }
                },
                new RequestTokenGenerator("storage-test-api"),
                transactionTemplate
        );
    }

    @Test
    @DisplayName("같은 회원이 다시 요청하면 재고와 이력을 추가하지 않는다")
    void rejectAlreadyIssuedMemberWithoutStockLeak() {
        issue(1L);

        assertThatThrownBy(() -> issue(1L))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(
                                        CouponIssueErrorCode.ALREADY_ISSUED
                                )
                );

        assertThat(countRows("issuances")).isEqualTo(1);
        assertThat(countRows("issuance_histories")).isEqualTo(1);
        assertThat(activeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("회차와 회원으로 기존 발급 여부를 조회한다")
    void findsExistingIssuanceByCouponAndMember() {
        issue(1L);

        assertThat(issuanceRepository.existsForCouponRoundAndMember(10L, 1L))
                .isTrue();
        assertThat(issuanceRepository.existsForCouponRoundAndMember(10L, 2L))
                .isFalse();
    }

    @Test
    @DisplayName("§4.9 확인을 위해 회차와 회원으로 발급 결과 전체를 다시 읽는다")
    void findsIssuanceForCouponRoundAndMember() {
        Issuance issued = issue(1L);

        assertThat(issuanceRepository.findForCouponRoundMemberAndIdempotencyKey(
                10L, 1L, "request-1"))
                .contains(issued);
        assertThat(issuanceRepository.findForCouponRoundMemberAndIdempotencyKey(
                10L, 1L, "different-request"))
                .isEmpty();
    }

    @Test
    @DisplayName("존재하지 않는 회원은 404로 거부하고 재고를 점유하지 않는다")
    void rejectMissingMemberWithoutStockLeak() {
        assertThatThrownBy(() -> issue(999L))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(
                                        CouponIssueErrorCode.MEMBER_NOT_FOUND
                                )
                );

        assertThat(countRows("issuances")).isZero();
        assertThat(countRows("issuance_histories")).isZero();
        assertThat(activeCount()).isZero();
    }

    @Test
    @DisplayName("재고 행이 없으면 발급건과 이력을 함께 롤백한다")
    void rollbackIssuanceWhenStockRowIsMissing() {
        jdbcTemplate.update(
                "DELETE FROM coupon_stocks WHERE coupon_id = ?",
                10L
        );

        assertThatThrownBy(() -> issue(1L))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(
                                        CouponIssueErrorCode
                                                .COUPON_STOCK_NOT_FOUND
                                )
                );

        assertThat(countRows("issuances")).isZero();
        assertThat(countRows("issuance_histories")).isZero();
    }

    @Test
    @DisplayName("재고 10장에 회원 20명이 동시에 요청해도 정확히 10장만 발급한다")
    void issueExactlyAvailableStockConcurrently() throws Exception {
        List<CouponIssueErrorCode> results = executeConcurrently(
                20,
                index -> index + 1L
        );

        assertThat(results.stream().filter(result -> result == null).count())
                .isEqualTo(10);
        assertThat(results.stream()
                .filter(CouponIssueErrorCode.SOLD_OUT::equals)
                .count()).isEqualTo(10);
        assertThat(countRows("issuances")).isEqualTo(10);
        assertThat(countRows("issuance_histories")).isEqualTo(10);
        assertThat(activeCount()).isEqualTo(10);
    }

    @Test
    @DisplayName("같은 회원의 동시 발급 10회는 한 건만 성공한다")
    void issueOnceForSameMemberConcurrently() throws Exception {
        List<CouponIssueErrorCode> results = executeConcurrently(
                10,
                index -> 1L
        );

        assertThat(results.stream().filter(result -> result == null).count())
                .isEqualTo(1);
        assertThat(results.stream()
                .filter(CouponIssueErrorCode.ALREADY_ISSUED::equals)
                .count()).isEqualTo(9);
        assertThat(countRows("issuances")).isEqualTo(1);
        assertThat(countRows("issuance_histories")).isEqualTo(1);
        assertThat(activeCount()).isEqualTo(1);
    }

    private Issuance issue(Long memberId) {
        return transactionTemplate.execute(status -> couponIssueService.issue(
                new CouponIssueCommand(
                        10L,
                        memberId,
                        MembershipGrade.GOLD,
                        "request-" + memberId,
                        ISSUED_AT
                )
        ));
    }

    private List<CouponIssueErrorCode> executeConcurrently(
            int requestCount,
            MemberIdProvider memberIdProvider
    ) throws Exception {
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(requestCount);
        List<Future<CouponIssueErrorCode>> futures = new ArrayList<>();

        try {
            for (int index = 0; index < requestCount; index++) {
                long memberId = memberIdProvider.memberId(index);
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "동시 발급 시작 대기 시간이 초과되었습니다."
                        );
                    }
                    try {
                        issueWithDeadlockRetry(memberId);
                        return null;
                    } catch (BusinessException exception) {
                        return (CouponIssueErrorCode) exception.getErrorCode();
                    }
                }));
            }

            assertThat(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
            start.countDown();

            List<CouponIssueErrorCode> results = new ArrayList<>();
            for (Future<CouponIssueErrorCode> future : futures) {
                results.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(
                    TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
            )).isTrue();
        }
    }

    private Issuance issueWithDeadlockRetry(Long memberId) {
        for (int attempt = 1; attempt <= DEADLOCK_MAX_ATTEMPTS; attempt++) {
            try {
                return issue(memberId);
            } catch (CannotAcquireLockException exception) {
                if (attempt == DEADLOCK_MAX_ATTEMPTS) {
                    // 계속되는 락 장애까지 성공으로 숨기지 않는다.
                    throw exception;
                }
                Thread.yield();
            }
        }
        throw new IllegalStateException("도달할 수 없는 데드락 재시도 상태입니다.");
    }

    @Test
    @DisplayName("회차 정책과 1인 1매 여부를 한 번의 조회로 읽는다")
    void readsPolicyAndDuplicateFlagInOneQuery() {
        CouponIssuePolicySnapshot before = couponRoundRepository
                .findIssuePolicySnapshot(10L, 1L)
                .orElseThrow();

        assertThat(before.couponRound().id()).isEqualTo(10L);
        assertThat(before.couponRound().validDays()).isPositive();
        assertThat(before.couponRound().eligibleGrades()).isNotEmpty();
        assertThat(before.alreadyIssued()).isFalse();

        issue(1L);

        CouponIssuePolicySnapshot after = couponRoundRepository
                .findIssuePolicySnapshot(10L, 1L)
                .orElseThrow();
        assertThat(after.alreadyIssued()).isTrue();
        assertThat(couponRoundRepository.findIssuePolicySnapshot(10L, 2L)
                .orElseThrow()
                .alreadyIssued()).isFalse();
    }

    @Test
    @DisplayName("없는 회차는 사전검증 조회에서 빈 값이다")
    void returnsEmptySnapshotForMissingCouponRound() {
        assertThat(couponRoundRepository.findIssuePolicySnapshot(999L, 1L))
                .isEmpty();
    }

    @Test
    @DisplayName("완성된 DONE 멱등 레코드를 INSERT 한 번으로 기록한다")
    void insertsCompletedIdempotencyRecordInOneStatement() {
        Issuance issued = issue(1L);

        boolean recorded = transactionTemplate.execute(status ->
                idempotencyRepository.insertCompleted(
                        IDEMPOTENCY_KEY,
                        1L,
                        issued.id(),
                        REQUEST_HASH,
                        "stored-response",
                        ISSUED_AT
                )
        );

        assertThat(recorded).isTrue();
        IdempotencyRecord stored = idempotencyRepository
                .findByKey(IDEMPOTENCY_KEY)
                .orElseThrow();
        assertThat(stored.status()).isEqualTo(IdempotencyStatus.DONE);
        assertThat(stored.memberId()).isEqualTo(1L);
        assertThat(stored.issuanceId()).isEqualTo(issued.id());
        assertThat(stored.responseBody()).isEqualTo("stored-response");
    }

    @Test
    @DisplayName("같은 멱등키가 이미 있으면 기록하지 않고 false 를 돌려준다")
    void rejectsDuplicateIdempotencyKeyWithoutOverwriting() {
        Issuance first = issue(1L);
        Issuance second = issue(2L);
        transactionTemplate.executeWithoutResult(status ->
                idempotencyRepository.insertCompleted(
                        IDEMPOTENCY_KEY,
                        1L,
                        first.id(),
                        REQUEST_HASH,
                        "first-response",
                        ISSUED_AT
                )
        );

        boolean recorded = transactionTemplate.execute(status ->
                idempotencyRepository.insertCompleted(
                        IDEMPOTENCY_KEY,
                        2L,
                        second.id(),
                        REQUEST_HASH,
                        "second-response",
                        ISSUED_AT
                )
        );

        assertThat(recorded).isFalse();
        assertThat(idempotencyRepository.findByKey(IDEMPOTENCY_KEY)
                .orElseThrow()
                .responseBody()).isEqualTo("first-response");
    }

    private void resetData() {
        jdbcTemplate.update("DELETE FROM idempotency_records");
        jdbcTemplate.update("DELETE FROM issuance_histories");
        jdbcTemplate.update("DELETE FROM issuances");
        jdbcTemplate.update("DELETE FROM coupon_stocks");
        jdbcTemplate.update("DELETE FROM coupons");
        jdbcTemplate.update("DELETE FROM coupon_templates");
        jdbcTemplate.update("DELETE FROM members");
        jdbcTemplate.update("DELETE FROM brands");
        jdbcTemplate.update("DELETE FROM grades");
    }

    private void saveOpenCouponRound(int totalQuantity) {
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
                "골드 VIP 5천원 할인",
                "FIXED_AMOUNT",
                5_000,
                7,
                3,
                "TUE",
                LocalTime.of(14, 0),
                2,
                totalQuantity,
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
                "골드 VIP 5천원 할인",
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
                totalQuantity,
                0,
                LocalDateTime.of(2026, 8, 17, 0, 0)
        );
    }

    private void saveMembers(int count) {
        List<Object[]> rows = new ArrayList<>();
        for (long memberId = 1; memberId <= count; memberId++) {
            rows.add(new Object[]{
                    memberId,
                    "GOLD",
                    LocalDateTime.of(2026, 1, 1, 0, 0)
            });
        }
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO members (id, membership_grade, created_at)
                VALUES (?, ?, ?)
                """,
                rows
        );
    }

    private int activeCount() {
        return jdbcTemplate.queryForObject(
                "SELECT active_count FROM coupon_stocks WHERE coupon_id = 10",
                Integer.class
        );
    }

    private int countRows(String tableName) {
        String query = switch (tableName) {
            case "issuances" -> "SELECT COUNT(*) FROM issuances";
            case "issuance_histories" ->
                    "SELECT COUNT(*) FROM issuance_histories";
            case "idempotency_records" ->
                    "SELECT COUNT(*) FROM idempotency_records";
            default -> throw new IllegalArgumentException(
                    "허용되지 않은 테스트 테이블입니다."
            );
        };
        return jdbcTemplate.queryForObject(query, Integer.class);
    }

    @TestConfiguration
    @EnableJpaAuditing(
            dateTimeProviderRef = "couponIssueTestDateTimeProvider"
    )
    static class AuditTestConfig {

        @Bean
        DateTimeProvider couponIssueTestDateTimeProvider() {
            return () -> Optional.of(AUDIT_CREATED_AT);
        }
    }

    @FunctionalInterface
    private interface MemberIdProvider {

        long memberId(int requestIndex);
    }
}
