package com.kafkick.core.coupon.service;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.support.exception.BusinessException;

/** 발급 사전검증과 권위 트랜잭션이 함께 사용하는 쿠폰 정책 규칙입니다. */
final class CouponIssuePolicy {

    private CouponIssuePolicy() {
    }

    /**
     * 발급 정책을 평가할 수 있는 필수 요청 값을 검증합니다.
     *
     * @param command 발급 요청
     * @throws BusinessException 필수 값이 없거나 식별자가 양수가 아닌 경우
     */
    static void validateCommand(CouponIssueCommand command) {
        if (command == null
                || command.couponRoundId() == null
                || command.couponRoundId() <= 0
                || command.memberId() == null
                || command.memberId() <= 0
                || command.membershipGrade() == null
                || command.issuedAt() == null) {
            throw new BusinessException(
                    CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST
            );
        }
    }

    /**
     * 회차의 오픈·종료·대상 등급 규칙을 동일한 우선순위로 검증합니다.
     *
     * @param couponRound 검증할 쿠폰 회차
     * @param command 회원 등급과 발급 시각을 담은 요청
     * @throws BusinessException 발급 시각 또는 등급 정책을 충족하지 못한 경우
     */
    static void validateIssuable(
            CouponRound couponRound,
            CouponIssueCommand command
    ) {
        Instant issuedAt = command.issuedAt();
        if (!issuedAt.isBefore(couponRound.closeAt())
                || couponRound.status() == CouponRoundStatus.CLOSED) {
            throw new BusinessException(
                    CouponIssueErrorCode.COUPON_ROUND_CLOSED,
                    "couponRoundId=" + couponRound.id()
            );
        }
        if (issuedAt.isBefore(couponRound.openAt())
                || couponRound.status() != CouponRoundStatus.OPEN) {
            throw new BusinessException(
                    CouponIssueErrorCode.NOT_OPENED,
                    "couponRoundId=" + couponRound.id()
            );
        }
        if (!couponRound.eligibleGrades().contains(
                command.membershipGrade()
        )) {
            throw new BusinessException(
                    CouponIssueErrorCode.GRADE_NOT_ELIGIBLE,
                    "couponRoundId=" + couponRound.id()
                            + ", memberId=" + command.memberId()
            );
        }
    }
}
