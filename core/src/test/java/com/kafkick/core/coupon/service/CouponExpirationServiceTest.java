package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.command.CouponExpirationCommand;
import com.kafkick.core.coupon.service.result.CouponExpirationResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 회차별 만료 성공 건수만큼 재고와 EXPIRE 이력이 반영되는지 검증합니다.

@ExtendWith(MockitoExtension.class)
class CouponExpirationServiceTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-26T05:00:00Z");

    @Mock
    private IssuanceRepository issuanceRepository;

    @Mock
    private IssuanceHistoryRepository issuanceHistoryRepository;

    @Mock
    private CouponStockRepository couponStockRepository;

    private CouponExpirationService expirationService;

    @BeforeEach
    void setUp() {
        expirationService = new CouponExpirationService(
                issuanceRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
    }

    @Test
    @DisplayName("조건부 만료에 성공한 발급건만 재고 감소와 EXPIRE 이력에 반영한다")
    void expireOnlySuccessfullyTransitionedIssuances() {
        Issuance first = issuance(100L);
        Issuance second = issuance(101L);
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.EXPIRED,
                AS_OF
        )).thenReturn(true);
        when(issuanceRepository.updateStatusIfCurrent(
                101L,
                21L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.EXPIRED,
                AS_OF
        )).thenReturn(false);
        when(couponStockRepository.lockForUpdate(10L)).thenReturn(true);
        when(couponStockRepository.release(10L, 1, AS_OF))
                .thenReturn(true);

        CouponExpirationResult result = expirationService.expire(
                new CouponExpirationCommand(
                        10L,
                        List.of(first, second),
                        AS_OF
                )
        );

        assertThat(result.requestedCount()).isEqualTo(2);
        assertThat(result.expiredCount()).isEqualTo(1);
        InOrder ordered = inOrder(
                couponStockRepository,
                issuanceRepository,
                issuanceHistoryRepository
        );
        ordered.verify(couponStockRepository).lockForUpdate(10L);
        ordered.verify(issuanceRepository).updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.EXPIRED,
                AS_OF
        );
        ordered.verify(issuanceRepository).updateStatusIfCurrent(
                101L,
                21L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.EXPIRED,
                AS_OF
        );
        ordered.verify(couponStockRepository).release(
                10L,
                1,
                AS_OF
        );
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<IssuanceHistory>> historyCaptor =
                ArgumentCaptor.forClass(List.class);
        ordered.verify(issuanceHistoryRepository)
                .saveAllExpirations(historyCaptor.capture());
        assertThat(historyCaptor.getValue())
                .singleElement()
                .satisfies(history -> {
                    assertThat(history.issuanceId()).isEqualTo(100L);
                    assertThat(history.eventType())
                            .isEqualTo(IssuanceEventType.EXPIRE);
                    assertThat(history.fromStatus())
                            .isEqualTo(IssuanceStatus.ISSUED);
                    assertThat(history.toStatus())
                            .isEqualTo(IssuanceStatus.EXPIRED);
                    assertThat(history.createdAt()).isEqualTo(AS_OF);
                });
    }

    @Test
    @DisplayName("조건부 만료가 모두 실패하면 재고와 이력을 변경하지 않는다")
    void skipStockAndHistoryWhenEveryTransitionLosesRace() {
        Issuance issuance = issuance(100L);
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.EXPIRED,
                AS_OF
        )).thenReturn(false);
        when(couponStockRepository.lockForUpdate(10L)).thenReturn(true);

        CouponExpirationResult result = expirationService.expire(
                new CouponExpirationCommand(10L, List.of(issuance), AS_OF)
        );

        assertThat(result.requestedCount()).isEqualTo(1);
        assertThat(result.expiredCount()).isZero();
        verify(couponStockRepository, never()).release(
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.any()
        );
        verify(issuanceHistoryRepository, never()).saveAllExpirations(
                org.mockito.ArgumentMatchers.anyList()
        );
    }

    private static Issuance issuance(Long issuanceId) {
        return Issuance.restore(
                issuanceId,
                10L,
                issuanceId - 80L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                Instant.parse("2026-08-18T05:00:00Z"),
                Instant.parse("2026-08-25T05:00:00Z"),
                Instant.parse("2026-08-18T05:00:00Z")
        );
    }
}
