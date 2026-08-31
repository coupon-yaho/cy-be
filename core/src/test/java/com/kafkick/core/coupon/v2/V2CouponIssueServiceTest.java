package com.kafkick.core.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
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
import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.exception.CouponAlreadyIssuedException;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
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
    private CouponStockRepository stocks;
    private V2CouponIssueService service;
    private CouponIssueCommand command;

    @BeforeEach
    void setUp() {
        gate = mock(IssuanceGatePort.class);
        issuances = mock(IssuanceRepository.class);
        histories = mock(IssuanceHistoryRepository.class);
        idempotencies = mock(IdempotencyRepository.class);
        stocks = mock(CouponStockRepository.class);
        when(stocks.occupyOne(anyLong(), any())).thenReturn(CouponStockOccupationResult.OCCUPIED);
        TransactionOperations transactions = immediateTransactions();
        CouponCodeGenerator codes = () -> "1234567890ABCDEF";
        codec = mock(IdempotencyResultCodec.class);
        when(codec.write(any())).thenReturn("{result}");
        service = new V2CouponIssueService(gate, issuances, histories, idempotencies, stocks,
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
        verify(stocks).occupyOne(10L, ISSUED_AT);
        org.mockito.InOrder order = inOrder(issuances, histories, idempotencies, stocks);
        order.verify(issuances).save(any());
        order.verify(histories).save(any());
        order.verify(idempotencies).insertCompleted(any(), any(), any(), any(), any(), any());
        order.verify(stocks).occupyOne(10L, ISSUED_AT);
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
    void dbStockIsTheFinalAuthorityAndRevertsTheRedisClaimWhenSoldOut() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenAnswer(invocation -> saved(invocation.getArgument(0)));
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any())).thenReturn(true);
        when(stocks.occupyOne(10L, ISSUED_AT)).thenReturn(CouponStockOccupationResult.SOLD_OUT);
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.REVERTED);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.SOLD_OUT);
        assertThat(result.issueResult()).isEmpty();
        verify(gate).compensate(10L, 20L, TOKEN);
        org.mockito.InOrder order = inOrder(issuances, histories, idempotencies, stocks);
        order.verify(issuances).save(any());
        order.verify(histories).save(any());
        order.verify(idempotencies).insertCompleted(any(), any(), any(), any(), any(), any());
        order.verify(stocks).occupyOne(10L, ISSUED_AT);
    }

    @Test
    void dbUniqueDuplicateBecomesDuplicateRejectionAndCompensatesTheRedisClaim() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(new CouponAlreadyIssuedException("duplicate", null));
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.REVERTED);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.DUP_PER_MEMBER);
        verify(gate).compensate(10L, 20L, TOKEN);
    }

    /**
     * uk_coupon_member 가 잡은 1인 다매는 DB 가 확정한 판정이다. 보상이 되돌릴 선점을
     * 못 찾았다는 이유로 예외가 되면 api 가 그것을 REDIS_UNAVAILABLE 503 으로 바꿔
     * "잠시 후 다시" 를 내보낸다 — 기다려도 바뀌지 않는 판정이라 재시도 루프가 된다.
     */
    @Test
    void duplicateStaysADuplicateRejectionWhenCompensationFoundNothingToRevert() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(new CouponAlreadyIssuedException("duplicate", null));
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.NO_CLAIM);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.DUP_PER_MEMBER);
        assertThat(result.issueResult()).isEmpty();
        assertThat(result.databaseDuplicateAfterRedisClaim()).isTrue();
        verify(gate).compensate(10L, 20L, TOKEN);
    }

    /**
     * 게이트가 통과시킨 중복은 게이트가 스스로 거른 중복과 응답이 같아도 원인이 다르다 —
     * 복제 유실의 직접 신호라 관제에서 갈려야 한다.
     */
    @Test
    void duplicateCaughtOnlyByTheDatabaseIsFlaggedAsMemberSetDivergence() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(new CouponAlreadyIssuedException("duplicate", null));
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.REVERTED);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.databaseDuplicateAfterRedisClaim()).isTrue();
        assertThat(result.databaseSoldOutAfterRedisClaim()).isFalse();
    }

    /**
     * 보상 호출이 깨진 것과 "보상하지 않기로 한 것"을 같은 값으로 접지 않는다. 접으면
     * 제일 불확실한 경로가 재시도 안내 없는 500 으로 새어 나간다.
     */
    @Test
    void aCompensationThatThrewIsReportedAsAnAttemptFailureRatherThanAbsence() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(new CouponAlreadyIssuedException("duplicate", null));
        when(gate.compensate(10L, 20L, TOKEN)).thenThrow(new RuntimeException("compensate timeout"));

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.compensateOutcome()).contains(CompensateOutcome.ATTEMPT_FAILED);
    }

    /** 되돌리지 못한 선점은 결과에 실려 나가야 관제가 재고 누수를 셀 수 있다. */
    @Test
    void carriesTheCompensationOutcomeSoTheLeakCanBeCounted() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(new CouponAlreadyIssuedException("duplicate", null));
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.NOT_MINE);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.compensateOutcome()).contains(CompensateOutcome.NOT_MINE);
        assertThat(result.databaseDuplicateAfterRedisClaim()).isTrue();
    }

    /**
     * 매진도 DB 가 확정한 판정이다 — 보상이 깨져도 SOLD_OUT 으로 끝낸다. 예외로 올리면
     * 매진된 회차가 차단기 OPEN 구간 내내 503 이 되어 클라이언트가 무한 재시도한다.
     */
    @Test
    void soldOutStaysSoldOutEvenWhenCompensationThrew() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenAnswer(invocation -> saved(invocation.getArgument(0)));
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any())).thenReturn(true);
        when(stocks.occupyOne(10L, ISSUED_AT)).thenReturn(CouponStockOccupationResult.SOLD_OUT);
        when(gate.compensate(10L, 20L, TOKEN)).thenThrow(new RuntimeException("compensate timeout"));

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.SOLD_OUT);
        assertThat(result.databaseSoldOutAfterRedisClaim()).isTrue();
        assertThat(result.compensateOutcome()).contains(CompensateOutcome.ATTEMPT_FAILED);
    }

    /** 매진 경로도 같은 판정을 쓴다 — 되돌릴 선점이 없으면 503 이 아니라 거절이다. */
    @Test
    void soldOutStaysASoldOutRejectionWhenCompensationFoundNothingToRevert() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenAnswer(invocation -> saved(invocation.getArgument(0)));
        when(idempotencies.insertCompleted(any(), any(), any(), any(), any(), any())).thenReturn(true);
        when(stocks.occupyOne(10L, ISSUED_AT)).thenReturn(CouponStockOccupationResult.SOLD_OUT);
        when(gate.compensate(10L, 20L, TOKEN)).thenReturn(CompensateOutcome.NO_CLAIM);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.SOLD_OUT);
        assertThat(result.databaseSoldOutAfterRedisClaim()).isTrue();
    }

    /**
     * 차단기가 열려 보상을 보내지도 못해도 <b>응답은 409 다</b>. OPEN 구간에서는 그것이
     * 기본값이라, 예외로 올리면 DB 가 확정한 1인 다매가 통째로 "1초 뒤 재시도" 503 이 된다 —
     * 이미 쿠폰을 받은 회원이 무한 재시도한다. 남은 선점은 결과에 실어 관제로 넘긴다.
     */
    @Test
    void duplicateStaysA409EvenWhenCompensationWasNeverSent() {
        when(gate.claim(any())).thenReturn(ClaimResult.claimed(8L));
        when(issuances.save(any())).thenThrow(new CouponAlreadyIssuedException("duplicate", null));
        when(gate.compensate(10L, 20L, TOKEN))
                .thenReturn(CompensateOutcome.NOT_ATTEMPTED_CIRCUIT_OPEN);

        V2CouponIssueResult result = service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.DUP_PER_MEMBER);
        assertThat(result.compensateOutcome())
                .contains(CompensateOutcome.NOT_ATTEMPTED_CIRCUIT_OPEN);
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
    void doesNotSendCompensationWhenTheCircuitRejectedClaimBeforeRedis() {
        IssuanceGateCircuitOpenException open = new IssuanceGateCircuitOpenException(
                new RuntimeException("circuit open"));
        when(gate.claim(any())).thenThrow(open);

        assertThatThrownBy(() -> service.issue(command,
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)))
                .isInstanceOfSatisfying(V2CouponIssueException.class, failure -> {
                    assertThat(failure.getCause()).isSameAs(open);
                    assertThat(failure.claimFailedBeforeResult()).isTrue();
                    assertThat(failure.compensateOutcome()).isEmpty();
                });

        verify(gate, never()).compensate(anyLong(), anyLong(), any());
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

    /**
     * <b>첫 보상 실패는 반드시 로그 후보가 된다.</b> 센티널을 {@code Long.MIN_VALUE} 로 두면
     * {@code now - previous} 가 부호 있는 64비트를 넘겨 음수로 접히고, 그러면 첫 줄부터
     * 영원히 억제된다 — 로그가 안 나가는 것은 "사고가 없다" 와 구분되지 않는다.
     */
    @Test
    void theFirstCompensationFailureAlwaysGetsALogSlot() {
        assertThat(service.claimLogSlot(System.nanoTime())).isTrue();
    }

    /** 같은 창 안의 두 번째는 생략한다 — failover 구간에 요청 수만큼 찍히면 안 된다. */
    @Test
    void asecondFailureInTheSameWindowIsSuppressed() {
        long now = System.nanoTime();

        assertThat(service.claimLogSlot(now)).isTrue();
        assertThat(service.claimLogSlot(now)).isFalse();
    }

    /** 창이 지나면 다시 한 줄 남긴다. */
    @Test
    void aFailureAfterTheWindowGetsANewSlot() {
        long now = System.nanoTime();
        service.claimLogSlot(now);

        assertThat(service.claimLogSlot(now + java.time.Duration.ofSeconds(1).toNanos())).isTrue();
    }
}
