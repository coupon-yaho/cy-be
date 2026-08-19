// 고정한 기준 시각으로 keyset 청크를 읽고 회차별 만료 트랜잭션을 실행하는지 검증합니다.
package com.kafkick.batch.coupon.expiration;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.CouponExpirationCommand;
import com.kafkick.core.coupon.service.CouponExpirationResult;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponExpirationRunnerTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-26T05:00:00.123456Z");

    @Mock
    private IssuanceRepository issuanceRepository;

    @Mock
    private CouponExpirationTransactionExecutor transactionExecutor;

    @Mock
    private TimeProvider timeProvider;

    private CouponExpirationRunner runner;

    @BeforeEach
    void setUp() {
        runner = new CouponExpirationRunner(
                issuanceRepository,
                transactionExecutor,
                timeProvider,
                new CouponExpirationProperties(2)
        );
    }

    @Test
    @DisplayName("기준 시각을 한 번 고정하고 keyset 청크를 회차별로 묶어 만료한다")
    void expireCandidatesWithOneAsOfAndRoundGrouping() {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(issuanceRepository.findExpiredIssuedAfterId(
                AS_OF,
                0L,
                2
        )).thenReturn(List.of(issuance(100L, 10L), issuance(101L, 10L)));
        when(issuanceRepository.findExpiredIssuedAfterId(
                AS_OF,
                101L,
                2
        )).thenReturn(List.of(issuance(102L, 20L)));
        when(transactionExecutor.execute(org.mockito.ArgumentMatchers.any()))
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
        verify(transactionExecutor, times(2))
                .execute(commandCaptor.capture());
        assertThat(commandCaptor.getAllValues())
                .extracting(CouponExpirationCommand::couponRoundId)
                .containsExactly(10L, 20L);
        assertThat(commandCaptor.getAllValues().get(0).issuances())
                .extracting(Issuance::id)
                .containsExactly(100L, 101L);
        assertThat(commandCaptor.getAllValues())
                .allSatisfy(command -> assertThat(command.asOf())
                        .isEqualTo(AS_OF));
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
}
