package com.kafkick.core.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.code.CouponCodeGenerator;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.membership.domain.MembershipGrade;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;

class V2CouponIssueServiceTest {

    private static final Instant ISSUED_AT = Instant.parse("2026-08-28T00:00:00Z");
    private static final String IDEM = "123e4567-e89b-42d3-a456-426614174000";
    private static final String TOKEN = "api-boot-1-0";

    private IssuanceGatePort gate;
    private IssuanceRepository issuances;
    private IssuanceHistoryRepository histories;
    private IdempotencyRepository idempotencies;
    private IdempotencyResultCodec<CouponIssueResult> codec;
    private V2CouponIssueService service;
    private CouponIssueCommand command;

    @BeforeEach
    void setUp() {
        gate = mock(IssuanceGatePort.class);
        issuances = mock(IssuanceRepository.class);
        histories = mock(IssuanceHistoryRepository.class);
        idempotencies = mock(IdempotencyRepository.class);
        TransactionOperations transactions = immediateTransactions();
        CouponCodeGenerator codes = () -> "1234567890ABCDEF";
        codec = mock(IdempotencyResultCodec.class);
        when(codec.write(any())).thenReturn("{result}");
        service = new V2CouponIssueService(gate, issuances, histories, idempotencies,
                codes, codec, new RequestTokenGenerator("api", "boot", 0L), transactions);
        command = new CouponIssueCommand(10L, 20L, MembershipGrade.GOLD, IDEM, ISSUED_AT);
    }

    @Test
    void persistsThreeRowsInOneTransactionAndCompletesTheClaim() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenAnswer(invocation -> saved(invocation.getArgument(0)));
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        when(gate.complete(10L, 20L, TOKEN)).thenReturn(CompleteOutcome.PROMOTED);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult()).isEqualTo(ClaimResult.claimed(8L));
        assertThat(result.issueResult()).isPresent();
        assertThat(result.completeOutcome()).contains(CompleteOutcome.PROMOTED);
        verify(issuances).save(any());
        verify(histories).save(any());
        verify(idempotencies).insertCompleted(any(), any(), any(), any(), any(), any());
    }

    @Test
    void returnsGateRejectionWithoutTouchingTheDatabase() {
        when(gate.claim(any())).thenReturn(ClaimResult.rejected(ClaimOutcome.SOLD_OUT));

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.SOLD_OUT);
        assertThat(result.issueResult()).isEmpty();
        verify(issuances, never()).save(any());
    }

    @Test
    void replayDoneReadsAndReturnsTheOriginalDatabaseResponse() {
        CouponIssueResult original = CouponIssueResult.from(saved(Issuance.issue(
                10L, 20L, "1234567890ABCDEF", MembershipGrade.GOLD, 7, ISSUED_AT)));
        String requestHash = com.kafkick.core.coupon.service.idempotency.IdempotencyKeys.hash(
                CouponIssueCommand.canonicalRequest(10L, 20L, MembershipGrade.GOLD));
        when(gate.claim(any())).thenReturn(ClaimResult.rejected(ClaimOutcome.REPLAY_DONE));
        when(idempotencies.findByKey(IDEM)).thenReturn(Optional.of(new IdempotencyRecord(
                IDEM, 20L, original.issuanceId(), requestHash, IdempotencyStatus.DONE,
                "{original}", ISSUED_AT)));
        when(codec.read("{original}")).thenReturn(original);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.issueResult()).contains(original);
        assertThat(result.replayed()).isTrue();
        verify(issuances, never()).save(any());
    }

    @Test
    void compensatesWithTheSameTokenWhenClaimTransportThrows() {
        RuntimeException timeout = new RuntimeException("claim timeout");
        when(gate.claim(any())).thenThrow(timeout);
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.REVERTED);

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, failure -> {
                    assertThat(failure.getCause()).isSameAs(timeout);
                    assertThat(failure.dependency()).isEqualTo(Dependency.REDIS);
                    assertThat(failure.compensateOutcome())
                            .contains(CompensateOutcome.REVERTED);
                });

        verify(gate).compensate(10L, 20L, TOKEN);
    }

    @Test
    void persistenceFailureWithoutCommittedIssuanceCompensates() {
        RuntimeException failure = new RuntimeException("insert failed");
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(failure);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenReturn(Optional.empty());
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.REVERTED);

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, wrapped -> {
                    assertThat(wrapped.getCause()).isSameAs(failure);
                    assertThat(wrapped.dependency()).isEqualTo(Dependency.MYSQL);
                    assertThat(wrapped.compensateOutcome())
                            .contains(CompensateOutcome.REVERTED);
                });

        verify(gate).compensate(10L, 20L, TOKEN);
    }

    @Test
    void persistenceFailureWithCommittedIssuanceCompletesAndReturnsIt() {
        RuntimeException failure = new RuntimeException("commit response lost");
        Issuance committed = saved(Issuance.issue(10L, 20L, "1234567890ABCDEF",
                MembershipGrade.GOLD, 7, ISSUED_AT));
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(failure);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenReturn(Optional.of(committed));
        when(idempotencies.findByKey(IDEM)).thenReturn(Optional.of(new IdempotencyRecord(
                IDEM, 20L, committed.id(),
                com.kafkick.core.coupon.service.idempotency.IdempotencyKeys.hash(
                        CouponIssueCommand.canonicalRequest(10L, 20L, MembershipGrade.GOLD)),
                IdempotencyStatus.DONE, "{result}", ISSUED_AT)));
        when(gate.complete(10L, 20L, TOKEN)).thenReturn(CompleteOutcome.PROMOTED);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.issueResult()).contains(CouponIssueResult.from(committed));
        assertThat(result.recoveredAfterFailure()).isTrue();
        verify(gate, never()).compensate(any(Long.class), any(Long.class), any());
    }

    @Test
    void requeryFailureLeavesPendingAndDoesNotCompensate() {
        RuntimeException insertFailure = new RuntimeException("insert failed");
        RuntimeException queryFailure = new RuntimeException("db unavailable");
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(insertFailure);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenThrow(queryFailure);

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, wrapped -> {
                    assertThat(wrapped.getCause()).isSameAs(insertFailure);
                    assertThat(wrapped.getCause().getSuppressed()).contains(queryFailure);
                    assertThat(wrapped.compensateOutcome()).isEmpty();
                });

        verify(gate, never()).compensate(any(Long.class), any(Long.class), any());
    }

    @Test
    void committedIssuanceWithoutDoneRecordIsNotPromoted() {
        RuntimeException failure = new RuntimeException("commit response lost");
        Issuance committed = saved(Issuance.issue(10L, 20L, "1234567890ABCDEF",
                MembershipGrade.GOLD, 7, ISSUED_AT));
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(failure);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenReturn(Optional.of(committed));
        when(idempotencies.findByKey(IDEM)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOf(V2CouponIssueException.class);

        verify(gate, never()).complete(any(Long.class), any(Long.class), any());
        verify(gate, never()).compensate(any(Long.class), any(Long.class), any());
    }

    private static IdempotencyRecord doneRecord(Issuance committed) {
        return new IdempotencyRecord(
                IDEM, 20L, committed.id(),
                com.kafkick.core.coupon.service.idempotency.IdempotencyKeys.hash(
                        CouponIssueCommand.canonicalRequest(10L, 20L, MembershipGrade.GOLD)),
                IdempotencyStatus.DONE, "{result}", ISSUED_AT);
    }

    private static Issuance issued() {
        return saved(Issuance.issue(10L, 20L, "1234567890ABCDEF",
                MembershipGrade.GOLD, 7, ISSUED_AT));
    }

    private static Issuance saved(Issuance issuance) {
        return Issuance.restore(99L, issuance.couponRoundId(), issuance.memberId(), issuance.code(),
                issuance.issuedGrade(), issuance.status(), issuance.issuedAt(), issuance.expiresAt(),
                issuance.updatedAt());
    }

    @SuppressWarnings("unchecked")
    @Test
    void completeFailureAfterCommitRetriesTheGateAndStaysAttributedToRedis() {
        // persist 가 커밋된 뒤의 complete 실패다. 재조회는 그 발급을 반드시 찾으므로
        // 복구 분기의 complete 재시도로 간다 — 그 재시도가 또 실패해도 REDIS 여야 하고,
        // 이미 커밋된 발급의 재고를 보상으로 되돌려서는 안 된다.
        RuntimeException completeFailure = new RuntimeException("complete timeout");
        Issuance committed = issued();
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenReturn(committed);
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        when(gate.complete(10L, 20L, TOKEN)).thenThrow(completeFailure);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenReturn(Optional.of(committed));
        when(idempotencies.findByKey(IDEM)).thenReturn(Optional.of(doneRecord(committed)));

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, wrapped -> {
                    assertThat(wrapped.dependency()).isEqualTo(Dependency.REDIS);
                    assertThat(wrapped.getCause()).isSameAs(completeFailure);
                });

        verify(gate, times(2)).complete(10L, 20L, TOKEN);
        verify(gate, never()).compensate(anyLong(), anyLong(), any());
    }

    @Test
    void completeSucceedingOnRetryReturnsTheCommittedIssuance() {
        Issuance committed = issued();
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenReturn(committed);
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        when(gate.complete(10L, 20L, TOKEN))
                .thenThrow(new RuntimeException("complete timeout"))
                .thenReturn(CompleteOutcome.PROMOTED);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenReturn(Optional.of(committed));
        when(idempotencies.findByKey(IDEM)).thenReturn(Optional.of(doneRecord(committed)));

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.issueResult()).contains(CouponIssueResult.from(committed));
        assertThat(result.recoveredAfterFailure()).isTrue();
        verify(gate, never()).compensate(anyLong(), anyLong(), any());
    }

    @Test
    void idempotencyLookupFailureDuringRecoveryIsAttributedToMysql() {
        // 재조회는 통과했는데 같은 DB 를 보는 멱등 레코드 조회가 깨진 경우다.
        // 같은 장애가 호출 순서에 따라 다른 의존성으로 집계되면 안 된다.
        RuntimeException persistFailure = new RuntimeException("insert failed");
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(persistFailure);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenReturn(Optional.of(issued()));
        when(idempotencies.findByKey(IDEM)).thenThrow(new RuntimeException("pool exhausted"));

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, wrapped ->
                        assertThat(wrapped.dependency()).isEqualTo(Dependency.MYSQL));
    }

    @Test
    void requeryFailureIsAttributedToMysqlEvenWhenTheGateFailedFirst() {
        RuntimeException completeFailure = new RuntimeException("complete timeout");
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenReturn(issued());
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        when(gate.complete(10L, 20L, TOKEN)).thenThrow(completeFailure);
        when(issuances.findForCouponRoundMemberAndIdempotencyKey(10L, 20L, IDEM))
                .thenThrow(new RuntimeException("db down"));

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, wrapped -> {
                    assertThat(wrapped.dependency()).isEqualTo(Dependency.MYSQL);
                    assertThat(wrapped.compensateOutcome()).isEmpty();
                });
        verify(gate, never()).compensate(anyLong(), anyLong(), any());
    }

    @ParameterizedTest
    @EnumSource(value = CompleteOutcome.class,
            names = {"CLAIM_GONE", "FOREIGN_CLAIM", "CORRUPT_VALUE", "BAD_ARGUMENT"})
    void abnormalCompleteOutcomeIsNotReportedAsASuccessfulIssue(CompleteOutcome abnormal) {
        // DB 에는 발급이 있는데 게이트는 완료되지 않은 불일치다. 성공으로 반환하면
        // 그 불일치가 200 응답에 은폐된다.
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenReturn(issued());
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        when(gate.complete(10L, 20L, TOKEN)).thenReturn(abnormal);

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, wrapped ->
                        assertThat(wrapped.dependency()).isEqualTo(Dependency.REDIS));

        // 이미 커밋된 발급이다. 보상하면 DB 에 짝이 있는 재고를 되돌린다.
        verify(gate, never()).compensate(anyLong(), anyLong(), any());
    }

    @ParameterizedTest
    @EnumSource(value = CompleteOutcome.class, names = {"PROMOTED", "ALREADY_DONE"})
    void normalCompleteOutcomesStaySuccessful(CompleteOutcome healthy) {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenReturn(issued());
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        when(gate.complete(10L, 20L, TOKEN)).thenReturn(healthy);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.completeOutcome()).contains(healthy);
    }

    /** 재고 N 에 2N 스레드. 게이트가 낸 선점 수를 넘겨 발급되지 않는다. */
    @Test
    void neverPersistsMoreIssuancesThanTheGateClaims() throws Exception {
        int stock = 50;
        int threads = 100;
        AtomicLong remaining = new AtomicLong(stock);
        AtomicInteger saved = new AtomicInteger();
        when(gate.claim(any())).thenAnswer(invocation -> {
            long left = remaining.getAndDecrement();
            return left > 0 ? ClaimResult.claimed(left - 1)
                    : ClaimResult.rejected(ClaimOutcome.SOLD_OUT);
        });
        when(issuances.save(any())).thenAnswer(invocation -> {
            saved.incrementAndGet();
            return issued();
        });
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any()))
                .thenReturn(true);
        when(gate.complete(anyLong(), anyLong(), any()))
                .thenReturn(CompleteOutcome.PROMOTED);

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger issuedCount = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                long memberId = 100L + i;
                pool.execute(() -> {
                    try {
                        start.await();
                        V2CouponIssueResult result = service.issue(
                                new CouponIssueCommand(10L, memberId, MembershipGrade.GOLD,
                                        IDEM, ISSUED_AT),
                                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));
                        if (result.issueResult().isPresent()) {
                            issuedCount.incrementAndGet();
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(issuedCount).hasValue(stock);
        assertThat(saved).hasValue(stock);
    }

    private static TransactionOperations immediateTransactions() {
        TransactionOperations operations = mock(TransactionOperations.class);
        when(operations.execute(any())).thenAnswer(invocation ->
                ((TransactionCallback<Object>) invocation.getArgument(0))
                        .doInTransaction(mock(TransactionStatus.class)));
        return operations;
    }
}
