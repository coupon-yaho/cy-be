package com.kafkick.batch.coupon.expiration;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.port.CouponExpirationCandidateQueryPort;
import com.kafkick.core.coupon.service.command.CouponExpirationCommand;
import com.kafkick.core.coupon.service.result.CouponExpirationResult;
import com.kafkick.core.coupon.service.CouponExpirationService;
import com.kafkick.core.coupon.service.V2StockRestorationService;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 고정한 기준 시각으로 keyset 청크를 읽고 회차별 만료 트랜잭션을 실행하는지 검증합니다.

@ExtendWith(MockitoExtension.class)
class CouponExpirationRunnerTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-26T05:00:00.123456Z");

    @Mock
    private CouponExpirationCandidateQueryPort expirationCandidateQueryPort;

    @Mock
    private CouponExpirationService expirationService;

    @Mock
    private TimeProvider timeProvider;

    @Mock
    private V2StockRestorationService stockRestorationService;

    private CouponExpirationRunner runner;

    @BeforeEach
    void setUp() {
        runner = new CouponExpirationRunner(
                expirationCandidateQueryPort,
                expirationService,
                stockRestorationService,
                timeProvider,
                new CouponExpirationProperties(3, 2)
        );
    }

    @Test
    @DisplayName("기준 시각을 한 번 고정하고 keyset 청크를 회차별로 묶어 만료한다")
    void expireCandidatesWithOneAsOfAndRoundGrouping() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(
                AS_OF,
                0L,
                3
        )).thenReturn(List.of(
                issuance(100L, 10L),
                issuance(101L, 10L),
                issuance(102L, 10L)
        ));
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(
                AS_OF,
                102L,
                3
        ))
                .thenReturn(List.of());
        when(expirationService.expire(org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation -> {
                    CouponExpirationCommand command = invocation.getArgument(0);
                    return new CouponExpirationResult(
                            command.issuances().size(),
                            command.issuances().size()
                    );
                });

        CouponExpirationBatchResult result = runner.runOnce();

        assertThat(result.asOf()).isEqualTo(AS_OF);
        assertThat(result.scannedCount()).isEqualTo(3);
        assertThat(result.expiredCount()).isEqualTo(3);
        verify(timeProvider, times(1)).instant();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<CouponExpirationCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponExpirationCommand.class);
        verify(expirationService, times(2))
                .expire(commandCaptor.capture());
        assertThat(commandCaptor.getAllValues())
                .extracting(CouponExpirationCommand::couponRoundId)
                .containsExactly(10L, 10L);
        assertThat(commandCaptor.getAllValues().get(0).issuances())
                .extracting(Issuance::id)
                .containsExactly(100L, 101L);
        assertThat(commandCaptor.getAllValues().get(1).issuances())
                .extracting(Issuance::id)
                .containsExactly(102L);
        assertThat(commandCaptor.getAllValues())
                .allSatisfy(command -> assertThat(command.asOf())
                        .isEqualTo(AS_OF));
    }

    @Test
    @DisplayName("트랜잭션 크기는 0보다 커야 한다")
    void rejectNonPositiveTransactionSize() {
        assertThatThrownBy(() -> new CouponExpirationProperties(500, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("쿠폰 만료 배치 크기는 0보다 커야 합니다.");
    }

    private static Issuance issuance(Long issuanceId, Long roundId) {
        return Issuance.restore(
                issuanceId,
                roundId,
                issuanceId + 1_000L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                Instant.parse("2026-08-18T05:00:00Z"),
                Instant.parse("2026-08-25T05:00:00Z"),
                Instant.parse("2026-08-18T05:00:00Z")
        );
    }

    /**
     * C11 — 배치 복원이 상한을 넘어 거절되면 그 회차는 <b>남은 청크를 돌리지 않는다.</b>
     * 거절은 재고가 이미 틀어져 있다는 신호이고, 계속 돌리면 DB 만 EXPIRED 로 전이돼
     * 어긋남이 청크마다 커진다. 다른 회차는 영향을 받지 않는다.
     */
    @Test
    @DisplayName("복원이 상한 초과로 거절되면 그 회차의 남은 청크를 멈추고 결과에 남긴다")
    void haltRemainingChunksOfTheRoundWhenRestorationIsHalted() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 0L, 3))
                .thenReturn(List.of(
                        issuance(100L, 10L),
                        issuance(101L, 10L),
                        issuance(102L, 10L)
                ));
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 102L, 3))
                .thenReturn(List.of(issuance(103L, 10L), issuance(104L, 20L)));
        // 첫 트랜잭션의 afterCommit 에서 -2 가 났다.
        when(stockRestorationService.isRestorationHalted(10L))
                .thenReturn(false, true, true, true);
        when(stockRestorationService.isRestorationHalted(20L)).thenReturn(false);
        when(expirationService.expire(org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation -> {
                    CouponExpirationCommand command = invocation.getArgument(0);
                    return new CouponExpirationResult(
                            command.issuances().size(),
                            command.issuances().size()
                    );
                });

        CouponExpirationBatchResult result = runner.runOnce();

        ArgumentCaptor<CouponExpirationCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponExpirationCommand.class);
        verify(expirationService, times(2)).expire(commandCaptor.capture());
        // 10번 회차는 첫 트랜잭션 하나뿐. 102 · 103 은 안 돈다. 20번은 그대로 돈다.
        assertThat(commandCaptor.getAllValues())
                .extracting(CouponExpirationCommand::couponRoundId)
                .containsExactly(10L, 20L);
        assertThat(commandCaptor.getAllValues().get(0).issuances())
                .extracting(Issuance::id)
                .containsExactly(100L, 101L);
        assertThat(result.expiredCount()).isEqualTo(3);
        assertThat(result.haltedRoundIds()).containsExactly(10L);
    }

    /**
     * ⑤ 이음매 — 앞 트랜잭션의 <b>커밋 뒤</b>에 표식이 서고, 그것을 다음 청크가 본다.
     *
     * <p>목으로 {@code thenReturn(false, true, ...)} 를 적으면 순서를 테스트가 스스로 쓰게 되어,
     * 복원 등록이 커밋 경계 밖으로 옮겨져도 초록이다. 여기서는 <b>실물</b>
     * {@link V2StockRestorationService} 에 실제 트랜잭션 동기화를 태워 순서를 코드가 만들게 한다.
     */
    @Test
    @DisplayName("커밋 뒤에 선 중단 표식을 다음 청크가 본다 — 순서를 코드가 만든다")
    void haltMarkRaisedAfterCommitStopsTheNextChunk() {
        RestorationHaltStore haltStore = new InMemoryHaltStore();
        IssuanceGatePort gate = mock(IssuanceGatePort.class);
        when(gate.restore(org.mockito.ArgumentMatchers.eq(10L), org.mockito.ArgumentMatchers.anyLong()))
                .thenReturn(RestoreOutcome.OVER_CAP);
        V2StockRestorationService restorations = new V2StockRestorationService(
                definitions(10L), provider(gate), provider(haltStore), provider(null));
        runner = new CouponExpirationRunner(
                expirationCandidateQueryPort, expirationService, restorations, timeProvider,
                new CouponExpirationProperties(3, 2));

        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 0L, 3))
                .thenReturn(List.of(issuance(100L, 10L), issuance(101L, 10L), issuance(102L, 10L)));
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 102L, 3))
                .thenReturn(List.of());
        // 만료 서비스가 하는 일을 그대로 흉내 낸다 — 트랜잭션 안에서 복원을 등록하고 커밋한다.
        when(expirationService.expire(org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation -> {
                    CouponExpirationCommand command = invocation.getArgument(0);
                    TransactionSynchronizationManager.initSynchronization();
                    try {
                        restorations.restoreAfterCommit(
                                command.couponRoundId(), command.issuances().size());
                        for (TransactionSynchronization synchronization
                                : TransactionSynchronizationManager.getSynchronizations()) {
                            synchronization.afterCommit();
                        }
                    } finally {
                        TransactionSynchronizationManager.clearSynchronization();
                    }
                    return new CouponExpirationResult(
                            command.issuances().size(), command.issuances().size());
                });

        CouponExpirationBatchResult result = runner.runOnce();

        // 첫 트랜잭션(100·101)만 돈다. 그 커밋 뒤 -2 가 표식을 세우므로 102 는 안 돈다.
        verify(expirationService, times(1)).expire(org.mockito.ArgumentMatchers.any());
        assertThat(result.expiredCount()).isEqualTo(2);
        assertThat(result.haltedRoundIds()).containsExactly(10L);
        assertThat(haltStore.isHalted(10L)).isTrue();
    }

    /**
     * 폴백은 실행 하나짜리다. 러너가 시작을 안 알리면 그 폴백이 실행을 넘어 살아남아,
     * 그 사이 재구성이 푼 회차를 배치가 다시 멈춘 것처럼 본다.
     */
    @Test
    @DisplayName("실행 시작에서 폴백을 걷는다 — 재구성이 푼 회차를 들고 가지 않는다")
    void clearsTheProcessLocalFallbackAtTheStartOfEachRun() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 0L, 3))
                .thenReturn(List.of());

        runner.runOnce();

        verify(stockRestorationService).beginExpirationRun();
    }

    /**
     * 한 회차의 실패가 <b>그 틱 전체</b>를 죽이면 안 된다. 뒤 회차의 만료가 통째로 밀리고,
     * 중단 회차 경보 로그(스케줄러)까지 못 찍혀 무슨 일이 있었는지도 안 남는다.
     */
    @Test
    @DisplayName("한 회차가 터져도 다른 회차는 계속 만료하고 결과에 남는다")
    void isolatesFailuresToTheRoundThatCausedThem() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 0L, 3))
                .thenReturn(List.of(issuance(100L, 10L), issuance(101L, 20L)));
        when(expirationService.expire(org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation -> {
                    CouponExpirationCommand command = invocation.getArgument(0);
                    if (command.couponRoundId() == 10L) {
                        throw new org.springframework.dao.DeadlockLoserDataAccessException(
                                "deadlock", null);
                    }
                    return new CouponExpirationResult(
                            command.issuances().size(), command.issuances().size());
                });

        CouponExpirationBatchResult result = runner.runOnce();

        assertThat(result.expiredCount())
                .as("20번 회차는 그대로 걷힌다")
                .isEqualTo(1);
        assertThat(result.failedRoundIds()).containsExactly(10L);
    }

    /**
     * 조립 사고는 회차 하나의 일시 장애가 아니다. 삼키면 매 틱 모든 회차가 조용히
     * {@code failedRoundIds} 로 떨어지고 스케줄러는 {@code expired=0} 을 info 로 남긴다 —
     * 만료가 영구 정지인데 정상과 구별되지 않는다. 큰 소리로 죽는 쪽이 맞다.
     */
    @Test
    @DisplayName("조립 오류는 삼키지 않고 틱을 죽인다")
    void assemblyErrorsAreNotSwallowed() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 0L, 3))
                .thenReturn(List.of(issuance(100L, 10L)));
        when(expirationService.expire(org.mockito.ArgumentMatchers.any()))
                .thenThrow(new IllegalStateException("V2 회차인데 재고 복원 게이트가 없습니다"));

        assertThatThrownBy(() -> runner.runOnce())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("게이트");
    }

    /**
     * 회차 중간에 터져도 <b>이미 커밋된 앞 청크는 집계에 남아야 한다.</b> 지역변수째 버리면
     * 실제로 걷힌 건수가 0 으로 보고돼 관제 수치가 과소가 된다.
     */
    @Test
    @DisplayName("회차 중간 실패에도 앞 청크의 커밋분은 집계에 남는다")
    void keepsTheCommittedCountOfChunksBeforeTheFailure() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 0L, 3))
                .thenReturn(List.of(
                        issuance(100L, 10L), issuance(101L, 10L), issuance(102L, 10L)));
        when(expirationService.expire(org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation -> {
                    CouponExpirationCommand command = invocation.getArgument(0);
                    if (command.issuances().get(0).id() == 102L) {
                        throw new org.springframework.dao.QueryTimeoutException("lock wait");
                    }
                    return new CouponExpirationResult(
                            command.issuances().size(), command.issuances().size());
                });

        CouponExpirationBatchResult result = runner.runOnce();

        assertThat(result.expiredCount())
                .as("100·101 은 이미 커밋됐다")
                .isEqualTo(2);
        assertThat(result.failedRoundIds()).containsExactly(10L);
    }

    /**
     * 실패한 회차도 <b>이번 실행에서는 더 건드리지 않는다.</b> 중단 회차만 거르고 실패 회차를
     * 안 거르면, 지속형 장애(락 대기·영구 실패)에서 그 회차가 페이지 수만큼 트랜잭션을 다시
     * 열어 실패한다 — 격리를 넣은 목적(뒤 회차가 밀리지 않게)이 정확히 뒤집힌다.
     */
    @Test
    @DisplayName("실패한 회차는 같은 실행의 다음 페이지에서 다시 시도하지 않는다")
    void doesNotRetryAFailedRoundInTheSameRun() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 0L, 3))
                .thenReturn(List.of(
                        issuance(100L, 10L), issuance(101L, 10L), issuance(102L, 10L)));
        when(expirationCandidateQueryPort.findExpiredIssuedAfterId(AS_OF, 102L, 3))
                .thenReturn(List.of(issuance(103L, 10L), issuance(104L, 20L)));
        when(expirationService.expire(org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation -> {
                    CouponExpirationCommand command = invocation.getArgument(0);
                    if (command.couponRoundId() == 10L) {
                        throw new org.springframework.dao.QueryTimeoutException("lock wait");
                    }
                    return new CouponExpirationResult(
                            command.issuances().size(), command.issuances().size());
                });

        CouponExpirationBatchResult result = runner.runOnce();

        ArgumentCaptor<CouponExpirationCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponExpirationCommand.class);
        verify(expirationService, times(2)).expire(commandCaptor.capture());
        assertThat(commandCaptor.getAllValues())
                .extracting(CouponExpirationCommand::couponRoundId)
                .as("10번은 첫 트랜잭션에서 실패한 뒤 이 실행에서 다시 안 들어간다")
                .containsExactly(10L, 20L);
        assertThat(result.failedRoundIds()).containsExactly(10L);
    }

    private static CouponRoundIssuanceDefinitionRepository definitions(long roundId) {
        CouponRoundIssuanceDefinitionRepository repository =
                mock(CouponRoundIssuanceDefinitionRepository.class);
        when(repository.findById(roundId)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(roundId, 7, EngineVersion.V2)));
        return repository;
    }

    @SuppressWarnings("unchecked")
    private static <T> ObjectProvider<T> provider(T value) {
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(value);
        return provider;
    }

    private static final class InMemoryHaltStore implements RestorationHaltStore {

        private final Set<Long> halted = ConcurrentHashMap.newKeySet();

        @Override
        public void halt(long couponRoundId) {
            halted.add(couponRoundId);
        }

        @Override
        public boolean isHalted(long couponRoundId) {
            return halted.contains(couponRoundId);
        }

        @Override
        public void clear(long couponRoundId) {
            halted.remove(couponRoundId);
        }
    }
}
