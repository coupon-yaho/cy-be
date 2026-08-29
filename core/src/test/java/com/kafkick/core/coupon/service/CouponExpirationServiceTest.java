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
import static org.mockito.Mockito.verifyNoInteractions;
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
    @Mock
    private V2StockRestorationService v2StockRestorationService;

    private CouponExpirationService expirationService;

    @BeforeEach
    void setUp() {
        expirationService = new CouponExpirationService(
                issuanceRepository,
                issuanceHistoryRepository,
                couponStockRepository,
                v2StockRestorationService
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
                issuanceHistoryRepository,
                v2StockRestorationService
        );
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
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<IssuanceHistory>> historyCaptor =
                ArgumentCaptor.forClass(List.class);
        // 이력 INSERT 가 재고 행 X 락 밖이어야 한다(설계 §9.6 D9). release 를 먼저 하면
        // 그 락이 커밋까지 유지되면서 이력 수백 건을 쓰는 동안 V2 발급이 같은 행에서 대기한다.
        ordered.verify(issuanceHistoryRepository)
                .saveAllExpirations(historyCaptor.capture());
        // 엔진 판별은 coupons 를 한 번 읽는다. release() 뒤에 두면 그 왕복이 재고 행 X 락
        // 보유 구간 안으로 들어간다 — 락을 늦게 잡으려고 이력을 앞으로 뺀 의미가 사라진다.
        ordered.verify(v2StockRestorationService).restoreAfterCommit(10L, 1L);
        ordered.verify(couponStockRepository).release(
                10L,
                1,
                AS_OF
        );
        // 요청 2건 중 전이에 성공한 1건만 복원한다. requestedCount 를 넘기면 Redis 잔여가
        // 실재고보다 커진다 — 초과 발급 방향이다.
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
        verifyNoInteractions(v2StockRestorationService);
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
