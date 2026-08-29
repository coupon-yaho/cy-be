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
import com.kafkick.core.coupon.domain.IssuanceUsage;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.coupon.service.command.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// 사용 취소 상태 전이·재고 복원·실적 종료·이력 저장 규칙을 검증합니다.

@ExtendWith(MockitoExtension.class)
class CouponCancelUseServiceTest {

    private static final Instant CANCELED_AT =
            Instant.parse("2026-08-20T05:00:00Z");
    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";

    @Mock
    private IssuanceRepository issuanceRepository;

    @Mock
    private IssuanceUsageRepository issuanceUsageRepository;

    @Mock
    private IssuanceHistoryRepository issuanceHistoryRepository;

    @Mock
    private CouponStockRepository couponStockRepository;
    @Mock
    private V2StockRestorationService v2StockRestorationService;

    private CouponCancelUseService cancelUseService;

    @BeforeEach
    void setUp() {
        cancelUseService = new CouponCancelUseService(
                issuanceRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository,
                couponStockRepository,
                v2StockRestorationService
        );
    }

    @Test
    @DisplayName("만료 전 사용 취소는 USED를 ISSUED로 되돌리고 재고를 변경하지 않는다")
    void cancelUseBeforeExpiration() {
        prepareSuccessfulCancel(issuance(expirationAfterCancel()));

        CouponCancelUseResult result = cancelUseService.cancelUse(command());

        assertThat(result.status()).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(result.orderId()).isEqualTo(30L);
        assertThat(result.discountAmount()).isEqualTo(5_000);
        assertThat(result.canceledAt()).isEqualTo(CANCELED_AT);
        verifyNoInteractions(couponStockRepository);
        // ISSUED 로 되돌아간 건은 재고를 쓴 적이 없다. 여기서 복원하면 Redis 가 부푼다.
        verifyNoInteractions(v2StockRestorationService);
        verifyStateAndHistory(IssuanceStatus.ISSUED);
    }

    @Test
    @DisplayName("만료된 USED 쿠폰의 사용 취소는 EXPIRED로 전이하고 재고를 한 번 복원한다")
    void cancelExpiredUseAndReleaseStock() {
        prepareSuccessfulCancel(issuance(expirationBeforeCancel()));

        CouponCancelUseResult result = cancelUseService.cancelUse(command());

        assertThat(result.status()).isEqualTo(IssuanceStatus.EXPIRED);
        InOrder ordered = inOrder(
                couponStockRepository,
                issuanceRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository,
                v2StockRestorationService
        );
        ordered.verify(issuanceRepository).updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.USED,
                IssuanceStatus.EXPIRED,
                CANCELED_AT
        );
        ordered.verify(issuanceUsageRepository).cancelIfActive(200L, CANCELED_AT);
        // 이력 INSERT 는 재고 행 X 락 밖이다(설계 §9.6 D9).
        ordered.verify(issuanceHistoryRepository).save(org.mockito.ArgumentMatchers.any());
        // coupons 왕복은 재고 행 X 락 밖이어야 한다.
        ordered.verify(v2StockRestorationService).restoreAfterCommit(10L, 1L);
        ordered.verify(couponStockRepository).release(
                10L,
                1,
                CANCELED_AT
        );
        verifyStateAndHistory(IssuanceStatus.EXPIRED);
    }

    @Test
    @DisplayName("취소 시각이 만료 시각과 같으면 ISSUED로 복원한다")
    void cancelAtExpirationBoundary() {
        prepareSuccessfulCancel(issuance(CANCELED_AT));

        CouponCancelUseResult result = cancelUseService.cancelUse(command());

        assertThat(result.status()).isEqualTo(IssuanceStatus.ISSUED);
        verifyNoInteractions(couponStockRepository);
    }

    @Test
    @DisplayName("다른 회원의 쿠폰은 사용 취소할 수 없다")
    void rejectCouponOwnedByAnotherMember() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance(expirationAfterCancel())));

        CouponCancelUseCommand command = new CouponCancelUseCommand(
                100L,
                21L,
                IDEMPOTENCY_KEY,
                CANCELED_AT
        );

        assertErrorCode(command, CouponUseErrorCode.NOT_COUPON_OWNER);
        verifyNoInteractions(
                issuanceUsageRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
    }

    @Test
    @DisplayName("활성 사용 실적이 없으면 상태와 재고를 변경하지 않는다")
    void rejectMissingActiveUsage() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance(expirationAfterCancel())));
        when(issuanceUsageRepository.findActiveByIssuanceId(100L))
                .thenReturn(Optional.empty());

        assertErrorCode(command(), CouponUseErrorCode.ACTIVE_USAGE_NOT_FOUND);
        verify(issuanceRepository, never()).updateStatusIfCurrent(
                any(), any(), any(), any(), any()
        );
        verifyNoInteractions(
                issuanceHistoryRepository,
                couponStockRepository
        );
    }

    @Test
    @DisplayName("취소 시각이 사용 시각보다 빠르면 INVALID_TRANSITION으로 거부한다")
    void rejectCancellationBeforeUsedAt() {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance(expirationAfterCancel())));
        when(issuanceUsageRepository.findActiveByIssuanceId(100L))
                .thenReturn(Optional.of(IssuanceUsage.restore(
                        200L,
                        100L,
                        30L,
                        5_000,
                        CANCELED_AT.plusSeconds(1),
                        null
                )));

        assertErrorCode(command(), CouponIssueErrorCode.INVALID_TRANSITION);
        verify(issuanceRepository, never()).updateStatusIfCurrent(
                any(), any(), any(), any(), any()
        );
        verify(issuanceUsageRepository, never()).cancelIfActive(any(), any());
        verifyNoInteractions(
                issuanceHistoryRepository,
                couponStockRepository
        );
    }

    @Test
    @DisplayName("동시 상태 변경으로 조건부 UPDATE가 실패하면 취소 실적과 이력을 남기지 않는다")
    void rejectLostStatusTransition() {
        Issuance issuance = issuance(expirationAfterCancel());
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance));
        when(issuanceUsageRepository.findActiveByIssuanceId(100L))
                .thenReturn(Optional.of(activeUsage()));
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.USED,
                IssuanceStatus.ISSUED,
                CANCELED_AT
        )).thenReturn(false);

        assertErrorCode(command(), CouponIssueErrorCode.INVALID_TRANSITION);
        verify(issuanceUsageRepository, never()).cancelIfActive(any(), any());
        verifyNoInteractions(issuanceHistoryRepository);
    }

    @Test
    @DisplayName("동시 실적 취소로 조건부 UPDATE가 실패하면 CANCEL_USE 이력을 남기지 않는다")
    void rejectLostUsageCancellation() {
        Issuance issuance = issuance(expirationAfterCancel());
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance));
        when(issuanceUsageRepository.findActiveByIssuanceId(100L))
                .thenReturn(Optional.of(activeUsage()));
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.USED,
                IssuanceStatus.ISSUED,
                CANCELED_AT
        )).thenReturn(true);
        when(issuanceUsageRepository.cancelIfActive(200L, CANCELED_AT))
                .thenReturn(false);

        assertErrorCode(command(), CouponIssueErrorCode.INVALID_TRANSITION);
        verifyNoInteractions(issuanceHistoryRepository);
    }

    private void prepareSuccessfulCancel(Issuance issuance) {
        when(issuanceRepository.findById(100L))
                .thenReturn(Optional.of(issuance));
        when(issuanceUsageRepository.findActiveByIssuanceId(100L))
                .thenReturn(Optional.of(activeUsage()));
        IssuanceStatus nextStatus = CANCELED_AT.isAfter(issuance.expiresAt())
                ? IssuanceStatus.EXPIRED
                : IssuanceStatus.ISSUED;
        when(issuanceRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.USED,
                nextStatus,
                CANCELED_AT
        )).thenReturn(true);
        when(issuanceUsageRepository.cancelIfActive(200L, CANCELED_AT))
                .thenReturn(true);
        if (nextStatus == IssuanceStatus.EXPIRED) {
            when(couponStockRepository.release(10L, 1, CANCELED_AT))
                    .thenReturn(true);
        }
    }

    private void verifyStateAndHistory(IssuanceStatus nextStatus) {
        verify(issuanceRepository).updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.USED,
                nextStatus,
                CANCELED_AT
        );
        verify(issuanceUsageRepository).cancelIfActive(200L, CANCELED_AT);
        ArgumentCaptor<IssuanceHistory> historyCaptor =
                ArgumentCaptor.forClass(IssuanceHistory.class);
        verify(issuanceHistoryRepository).save(historyCaptor.capture());
        assertThat(historyCaptor.getValue().eventType())
                .isEqualTo(IssuanceEventType.CANCEL_USE);
        assertThat(historyCaptor.getValue().fromStatus())
                .isEqualTo(IssuanceStatus.USED);
        assertThat(historyCaptor.getValue().toStatus())
                .isEqualTo(nextStatus);
        assertThat(historyCaptor.getValue().requestId())
                .isEqualTo(IDEMPOTENCY_KEY);
        assertThat(historyCaptor.getValue().reason())
                .isEqualTo("주문 취소로 사용 복원");
    }

    private CouponCancelUseCommand command() {
        return new CouponCancelUseCommand(
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
                IssuanceStatus.USED,
                Instant.parse("2026-08-18T05:00:00Z"),
                expiresAt,
                Instant.parse("2026-08-19T05:00:00Z")
        );
    }

    private IssuanceUsage activeUsage() {
        return IssuanceUsage.restore(
                200L,
                100L,
                30L,
                5_000,
                Instant.parse("2026-08-19T05:00:00Z"),
                null
        );
    }

    private Instant expirationBeforeCancel() {
        return CANCELED_AT.minusSeconds(1);
    }

    private Instant expirationAfterCancel() {
        return CANCELED_AT.plusSeconds(1);
    }

    private void assertErrorCode(
            CouponCancelUseCommand command,
            com.kafkick.core.support.exception.ErrorCode errorCode
    ) {
        assertThatThrownBy(() -> cancelUseService.cancelUse(command))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(errorCode)
                );
    }
}
