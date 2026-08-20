package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Optional;

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
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.command.CouponCancelCommand;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// 쿠폰 발급 취소의 상태 전이·재고 복원·이력 저장 규칙을 검증합니다.

@ExtendWith(MockitoExtension.class)
class CouponCancelServiceTest {

    private static final Instant CANCELED_AT =
            Instant.parse("2026-08-20T05:00:00Z");
    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";

    @Mock
    private IssuanceRepository issuanceRepository;

    @Mock
    private IssuanceHistoryRepository issuanceHistoryRepository;

    @Mock
    private CouponStockRepository couponStockRepository;

    private CouponCancelService cancelService;

    @BeforeEach
    void setUp() {
        cancelService = new CouponCancelService(
                issuanceRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
    }

    @Test
    @DisplayName("ISSUED 쿠폰을 취소하면 CANCELLED로 전이하고 재고와 이력을 한 번 변경한다")
    void cancelIssuedCouponAndReleaseStock() {
        Issuance issuance = issuance(CANCELED_AT.plusSeconds(1));
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance));
        when(couponStockRepository.lockForUpdate(10L)).thenReturn(true);
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.CANCELLED,
                CANCELED_AT
        )).thenReturn(true);
        when(couponStockRepository.release(10L, 1, CANCELED_AT))
                .thenReturn(true);

        CouponCancelResult result = cancelService.cancel(command());

        assertThat(result.issuanceId()).isEqualTo(100L);
        assertThat(result.status()).isEqualTo(IssuanceStatus.CANCELLED);
        assertThat(result.canceledAt()).isEqualTo(CANCELED_AT);
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
                IssuanceStatus.CANCELLED,
                CANCELED_AT
        );
        ordered.verify(couponStockRepository).release(
                10L,
                1,
                CANCELED_AT
        );
        ArgumentCaptor<IssuanceHistory> historyCaptor =
                ArgumentCaptor.forClass(IssuanceHistory.class);
        ordered.verify(issuanceHistoryRepository)
                .save(historyCaptor.capture());
        assertThat(historyCaptor.getValue().eventType())
                .isEqualTo(IssuanceEventType.CANCEL);
        assertThat(historyCaptor.getValue().fromStatus())
                .isEqualTo(IssuanceStatus.ISSUED);
        assertThat(historyCaptor.getValue().toStatus())
                .isEqualTo(IssuanceStatus.CANCELLED);
        assertThat(historyCaptor.getValue().requestId())
                .isEqualTo(IDEMPOTENCY_KEY);
    }

    @Test
    @DisplayName("만료 시각이 지난 쿠폰은 상태와 재고를 변경하지 않고 거부한다")
    void rejectExpiredCouponBeforeChangingStock() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance(
                        CANCELED_AT.minusSeconds(1)
                )));

        assertErrorCode(command(), CouponUseErrorCode.COUPON_EXPIRED);

        verify(issuanceRepository, never()).updateStatusIfCurrent(
                any(), any(), any(), any(), any()
        );
        verifyNoInteractions(
                couponStockRepository,
                issuanceHistoryRepository
        );
    }

    @Test
    @DisplayName("다른 회원의 쿠폰은 발급 취소할 수 없다")
    void rejectCouponOwnedByAnotherMember() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance(
                        CANCELED_AT.plusSeconds(1)
                )));
        CouponCancelCommand anotherMemberCommand = new CouponCancelCommand(
                100L,
                21L,
                IDEMPOTENCY_KEY,
                CANCELED_AT
        );

        assertErrorCode(
                anotherMemberCommand,
                CouponUseErrorCode.NOT_COUPON_OWNER
        );

        verifyNoInteractions(
                couponStockRepository,
                issuanceHistoryRepository
        );
    }

    @Test
    @DisplayName("동시 상태 변경으로 ISSUED 조건부 전이가 실패하면 재고와 이력을 변경하지 않는다")
    void rejectLostStatusTransition() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance(
                        CANCELED_AT.plusSeconds(1)
                )));
        when(couponStockRepository.lockForUpdate(10L)).thenReturn(true);
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.CANCELLED,
                CANCELED_AT
        )).thenReturn(false);

        assertErrorCode(command(), CouponIssueErrorCode.INVALID_TRANSITION);

        verify(couponStockRepository, never()).release(
                anyLong(), anyInt(), any()
        );
        verifyNoInteractions(issuanceHistoryRepository);
    }

    private CouponCancelCommand command() {
        return new CouponCancelCommand(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                CANCELED_AT
        );
    }

    private Issuance issuance(Instant expiresAt) {
        return Issuance.restore(
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                Instant.parse("2026-08-18T05:00:00Z"),
                expiresAt,
                Instant.parse("2026-08-19T05:00:00Z")
        );
    }

    private void assertErrorCode(
            CouponCancelCommand command,
            ErrorCode errorCode
    ) {
        assertThatThrownBy(() -> cancelService.cancel(command))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(errorCode)
                );
    }
}
