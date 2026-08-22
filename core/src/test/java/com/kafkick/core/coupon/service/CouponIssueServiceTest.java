package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.code.CouponCodeGenerator;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// 발급 검증 순서와 1인 1매 선점 후 재고·이력 처리 계약을 검증합니다.

@ExtendWith(MockitoExtension.class)
class CouponIssueServiceTest {

    private static final Instant ISSUED_AT =
            Instant.parse("2026-08-18T05:30:00Z");

    @Mock
    private CouponRoundRepository couponRoundRepository;

    @Mock
    private IssuanceRepository issuanceRepository;

    @Mock
    private CouponStockRepository couponStockRepository;

    @Mock
    private IssuanceHistoryRepository issuanceHistoryRepository;

    @Mock
    private CouponCodeGenerator couponCodeGenerator;

    private CouponIssueService couponIssueService;

    @BeforeEach
    void setUp() {
        couponIssueService = new CouponIssueService(
                couponRoundRepository,
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    @Test
    @DisplayName("재고를 잠근 뒤 발급건 선점과 재고 차감, ISSUE 이력을 저장한다")
    void issueCouponInRequiredOrder() {
        CouponRound couponRound = couponRound(CouponRoundStatus.OPEN);
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound));
        when(couponCodeGenerator.generate())
                .thenReturn("ABCDEFGHJKLM2345");
        when(couponStockRepository.lockForUpdate(10L)).thenReturn(true);
        when(issuanceRepository.save(any(Issuance.class)))
                .thenAnswer(invocation -> persisted(
                        invocation.getArgument(0)
                ));
        when(couponStockRepository.occupyAfterLock(10L, ISSUED_AT))
                .thenReturn(CouponStockOccupationResult.OCCUPIED);

        Issuance result = couponIssueService.issue(command(
                MembershipGrade.GOLD,
                ISSUED_AT
        ));

        InOrder order = inOrder(
                couponRoundRepository,
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository
        );
        order.verify(couponRoundRepository).findById(10L);
        order.verify(couponStockRepository).lockForUpdate(10L);
        order.verify(issuanceRepository).save(any(Issuance.class));
        order.verify(couponStockRepository).occupyAfterLock(10L, ISSUED_AT);
        order.verify(issuanceHistoryRepository)
                .save(any(IssuanceHistory.class));

        ArgumentCaptor<Issuance> issuanceCaptor =
                ArgumentCaptor.forClass(Issuance.class);
        verify(issuanceRepository).save(issuanceCaptor.capture());
        assertThat(issuanceCaptor.getValue().memberId()).isEqualTo(20L);
        assertThat(issuanceCaptor.getValue().issuedGrade())
                .isEqualTo(MembershipGrade.GOLD);
        assertThat(issuanceCaptor.getValue().expiresAt())
                .isEqualTo(Instant.parse("2026-08-25T05:30:00Z"));

        ArgumentCaptor<IssuanceHistory> historyCaptor =
                ArgumentCaptor.forClass(IssuanceHistory.class);
        verify(issuanceHistoryRepository).save(historyCaptor.capture());
        assertThat(historyCaptor.getValue().issuanceId()).isEqualTo(100L);
        assertThat(historyCaptor.getValue().eventType())
                .isEqualTo(IssuanceEventType.ISSUE);
        assertThat(historyCaptor.getValue().fromStatus()).isNull();
        assertThat(historyCaptor.getValue().toStatus())
                .isEqualTo(IssuanceStatus.ISSUED);
        assertThat(result.id()).isEqualTo(100L);
    }

    @Test
    @DisplayName("재고 점유 결과가 소진이면 core가 SOLD_OUT으로 판단한다")
    void rejectSoldOutStockInCore() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(CouponRoundStatus.OPEN)));
        when(couponStockRepository.lockForUpdate(10L)).thenReturn(true);
        when(couponCodeGenerator.generate()).thenReturn("ABCDEFGHJKLM2345");
        when(issuanceRepository.save(any(Issuance.class)))
                .thenAnswer(invocation -> persisted(invocation.getArgument(0)));
        when(couponStockRepository.occupyAfterLock(10L, ISSUED_AT))
                .thenReturn(CouponStockOccupationResult.SOLD_OUT);

        assertErrorCode(
                command(MembershipGrade.GOLD, ISSUED_AT),
                CouponIssueErrorCode.SOLD_OUT
        );

        verifyNoInteractions(issuanceHistoryRepository);
    }

    @Test
    @DisplayName("재고 행이 없으면 core가 재고 정보 없음으로 판단한다")
    void rejectMissingStockInCore() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(CouponRoundStatus.OPEN)));
        when(couponStockRepository.lockForUpdate(10L)).thenReturn(false);

        assertErrorCode(
                command(MembershipGrade.GOLD, ISSUED_AT),
                CouponIssueErrorCode.COUPON_STOCK_NOT_FOUND
        );

        verifyNoInteractions(
                issuanceRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    @Test
    @DisplayName("오픈 전에는 발급건과 재고를 변경하지 않는다")
    void rejectBeforeOpen() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(
                        CouponRoundStatus.SCHEDULED
                )));

        assertErrorCode(
                command(
                        MembershipGrade.GOLD,
                        Instant.parse("2026-08-18T04:59:59Z")
                ),
                CouponIssueErrorCode.NOT_OPENED
        );
        verifyNoInteractions(
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    @Test
    @DisplayName("마감 시각부터는 쿠폰을 발급하지 않는다")
    void rejectAtCloseTime() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(
                        CouponRoundStatus.OPEN
                )));

        assertErrorCode(
                command(
                        MembershipGrade.GOLD,
                        Instant.parse("2026-08-18T07:00:00Z")
                ),
                CouponIssueErrorCode.CAMPAIGN_CLOSED
        );
        verify(issuanceRepository, never()).save(any());
        verifyNoInteractions(
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    @Test
    @DisplayName("명시적으로 마감된 회차는 오픈 시각 전이어도 마감으로 거부한다")
    void rejectClosedRoundBeforeOpenTime() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(
                        CouponRoundStatus.CLOSED
                )));

        assertErrorCode(
                command(
                        MembershipGrade.GOLD,
                        Instant.parse("2026-08-18T04:59:59Z")
                ),
                CouponIssueErrorCode.CAMPAIGN_CLOSED
        );
        verifyNoInteractions(
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    @Test
    @DisplayName("대상 등급이 아니면 재고를 점유하지 않는다")
    void rejectIneligibleGrade() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.of(couponRound(
                        CouponRoundStatus.OPEN
                )));

        assertErrorCode(
                command(MembershipGrade.SILVER, ISSUED_AT),
                CouponIssueErrorCode.GRADE_NOT_ELIGIBLE
        );
        verify(issuanceRepository, never()).save(any());
        verifyNoInteractions(
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    @Test
    @DisplayName("존재하지 않는 회차는 404 오류로 거부한다")
    void rejectMissingCouponRound() {
        when(couponRoundRepository.findById(10L))
                .thenReturn(Optional.empty());

        assertErrorCode(
                command(MembershipGrade.GOLD, ISSUED_AT),
                CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND
        );
        verifyNoInteractions(
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    @Test
    @DisplayName("잘못된 발급 요청은 400 오류로 거부한다")
    void rejectInvalidIssueCommand() {
        assertErrorCode(
                new CouponIssueCommand(
                        0L,
                        20L,
                        MembershipGrade.GOLD,
                        "request-1",
                        ISSUED_AT
                ),
                CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST
        );
        verifyNoInteractions(
                couponRoundRepository,
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }

    private void assertErrorCode(
            CouponIssueCommand command,
            CouponIssueErrorCode expectedErrorCode
    ) {
        assertThatThrownBy(() -> couponIssueService.issue(command))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(expectedErrorCode)
                );
    }

    private CouponIssueCommand command(
            MembershipGrade membershipGrade,
            Instant issuedAt
    ) {
        return new CouponIssueCommand(
                10L,
                20L,
                membershipGrade,
                "request-1",
                issuedAt
        );
    }

    private CouponRound couponRound(CouponRoundStatus status) {
        return CouponRound.restore(
                10L,
                1L,
                1L,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                Instant.parse("2026-08-18T05:00:00Z"),
                Instant.parse("2026-08-18T07:00:00Z"),
                status,
                Instant.parse("2026-08-17T00:00:00Z")
        );
    }

    private Issuance persisted(Issuance issuance) {
        return Issuance.restore(
                100L,
                issuance.couponRoundId(),
                issuance.memberId(),
                issuance.code(),
                issuance.issuedGrade(),
                issuance.status(),
                issuance.issuedAt(),
                issuance.expiresAt(),
                issuance.updatedAt()
        );
    }
}
