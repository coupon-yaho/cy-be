package com.kafkick.api.admin.couponround.dto;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponRoundStatus;

/**
 * 감사 대상인 쿠폰 회차 상태 전환의 결과입니다.
 *
 * @param couponId 전환된 쿠폰 회차 식별자
 * @param status 전환 후 상태
 * @param updatedBy 명령을 수행한 관리자 회원 식별자
 * @param updatedAt 상태 전환이 확정된 시각
 */
public record CouponRoundStatusTransitionResponse(
        Long couponId, CouponRoundStatus status, Long updatedBy, Instant updatedAt) {
}
