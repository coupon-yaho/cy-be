// 쿠폰 소유권·만료·조건부 상태 전이와 할인 계산을 검증합니다.
package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.domain.IssuanceUsage;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponUseServiceTest {

    private static final Instant USED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IssuanceRepository issuanceRepository;

    @Mock
    private CouponRoundRepository couponRoundRepository;

    @Mock
    private IssuanceUsageRepository issuanceUsageRepository;

    @Mock
    private IssuanceHistoryRepository issuanceHistoryRepository;

    private CouponUseService couponUseService;

    @BeforeEach
    void setUp() {
        couponUseService = new CouponUseService(
                issuanceRepository,
                couponRoundRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository
        );
    }

    @Test
    @DisplayName("정액 쿠폰을 사용하면 주문 금액 이내의 할인 실적과 USE 이력을 저장한다")
    void useFixedAmountCoupon() {
        prepareSuccessfulUse(fixedRound(5_000));

        CouponUseResult result = couponUseService.use(command(3_000));

        assertThat(result.status()).isEqualTo(IssuanceStatus.USED);
        assertThat(result.discountAmount()).isEqualTo(3_000);
        ArgumentCaptor<IssuanceUsage> usageCaptor =
                ArgumentCaptor.forClass(IssuanceUsage.class);
        verify(issuanceUsageRepository).save(usageCaptor.capture());
        assertThat(usageCaptor.getValue().orderId()).isEqualTo(30L);
        assertThat(usageCaptor.getValue().discountAmount()).isEqualTo(3_000);

        ArgumentCaptor<IssuanceHistory> historyCaptor =
                ArgumentCaptor.forClass(IssuanceHistory.class);
        verify(issuanceHistoryRepository).save(historyCaptor.capture());
        assertThat(historyCaptor.getValue().eventType())
                .isEqualTo(IssuanceEventType.USE);
        assertThat(historyCaptor.getValue().fromStatus())
                .isEqualTo(IssuanceStatus.ISSUED);
        assertThat(historyCaptor.getValue().toStatus())
                .isEqualTo(IssuanceStatus.USED);
        assertThat(historyCaptor.getValue().requestId())
                .isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    }

    @Test
    @DisplayName("정률 쿠폰의 실제 할인액은 최대 할인 금액을 넘지 않는다")
    void capPercentDiscount() {
        prepareSuccessfulUse(percentRound(20, 10_000));

        CouponUseResult result = couponUseService.use(command(100_000));

        assertThat(result.discountAmount()).isEqualTo(10_000);
    }

    @Test
    @DisplayName("다른 회원의 쿠폰은 사용할 수 없다")
    void rejectCouponOwnedByAnotherMember() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance()));

        CouponUseCommand command = new CouponUseCommand(
                100L,
                21L,
                30L,
                10_000,
                "550e8400-e29b-41d4-a716-446655440000",
                USED_AT
        );

        assertErrorCode(command, CouponUseErrorCode.NOT_COUPON_OWNER);
        verifyNoInteractions(
                couponRoundRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository
        );
        verify(issuanceRepository, never()).updateStatusIfCurrent(
                any(), any(), any(), any(), any()
        );
    }

    @Test
    @DisplayName("만료된 쿠폰은 상태와 사용 실적을 변경하지 않는다")
    void rejectExpiredCoupon() {
        Issuance expired = Issuance.restore(
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                Instant.parse("2026-08-10T05:00:00Z"),
                Instant.parse("2026-08-19T05:00:00Z"),
                Instant.parse("2026-08-10T05:00:00Z")
        );
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(expired));
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(fixedRound(5_000)));

        assertErrorCode(command(10_000), CouponUseErrorCode.COUPON_EXPIRED);
        verify(issuanceRepository, never()).updateStatusIfCurrent(
                any(), any(), any(), any(), any()
        );
        verifyNoInteractions(
                issuanceUsageRepository,
                issuanceHistoryRepository
        );
    }

    @Test
    @DisplayName("동시 상태 변경으로 조건부 UPDATE가 실패하면 실적을 저장하지 않는다")
    void rejectLostConditionalUpdate() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance()));
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(fixedRound(5_000)));
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.USED,
                USED_AT
        )).thenReturn(false);

        assertErrorCode(command(10_000), CouponIssueErrorCode.INVALID_TRANSITION);
        verifyNoInteractions(
                issuanceUsageRepository,
                issuanceHistoryRepository
        );
    }

    private void prepareSuccessfulUse(CouponRound couponRound) {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance()));
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound));
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.USED,
                USED_AT
        )).thenReturn(true);
        when(issuanceUsageRepository.save(any(IssuanceUsage.class)))
                .thenAnswer(invocation -> {
                    IssuanceUsage usage = invocation.getArgument(0);
                    return IssuanceUsage.restore(
                            200L,
                            usage.issuanceId(),
                            usage.orderId(),
                            usage.discountAmount(),
                            usage.usedAt(),
                            usage.canceledAt()
                    );
                });
    }

    private CouponUseCommand command(int orderAmount) {
        return new CouponUseCommand(
                100L,
                20L,
                30L,
                orderAmount,
                "550e8400-e29b-41d4-a716-446655440000",
                USED_AT
        );
    }

    private Issuance issuance() {
        return Issuance.restore(
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                Instant.parse("2026-08-18T05:00:00Z"),
                Instant.parse("2026-08-25T05:00:00Z"),
                Instant.parse("2026-08-18T05:00:00Z")
        );
    }

    private CouponRound fixedRound(int discountAmount) {
        return round(
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                discountAmount
        );
    }

    private CouponRound percentRound(int rate, int cap) {
        return round(
                CouponPolicyType.PERCENT_CAPPED,
                rate,
                cap,
                null
        );
    }

    private CouponRound round(
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount
    ) {
        return CouponRound.restore(
                10L,
                1L,
                1L,
                "테스트 쿠폰",
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
                7,
                Set.of(MembershipGrade.GOLD),
                Instant.parse("2026-08-18T05:00:00Z"),
                Instant.parse("2026-08-18T07:00:00Z"),
                CouponRoundStatus.OPEN,
                Instant.parse("2026-08-17T05:00:00Z")
        );
    }

    private void assertErrorCode(
            CouponUseCommand command,
            com.kafkick.core.support.exception.ErrorCode errorCode
    ) {
        assertThatThrownBy(() -> couponUseService.use(command))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(errorCode)
                );
    }
}
