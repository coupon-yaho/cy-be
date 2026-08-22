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
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.port.CouponExpirationCandidateQueryPort;
import com.kafkick.core.coupon.service.command.CouponExpirationCommand;
import com.kafkick.core.coupon.service.result.CouponExpirationResult;
import com.kafkick.core.coupon.service.CouponExpirationService;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    private CouponExpirationRunner runner;

    @BeforeEach
    void setUp() {
        runner = new CouponExpirationRunner(
                expirationCandidateQueryPort,
                expirationService,
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
}
