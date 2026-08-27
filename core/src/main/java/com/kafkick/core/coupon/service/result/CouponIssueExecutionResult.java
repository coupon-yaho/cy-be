package com.kafkick.core.coupon.service.result;

/**
 * 쿠폰 발급 응답과 완료 멱등 응답 재사용 여부를 함께 반환합니다.
 *
 * @param result 기존 쿠폰 발급 응답
 * @param replayed 이미 완료된 발급 응답을 복원했으면 {@code true}
 */
public record CouponIssueExecutionResult(
        CouponIssueResult result,
        boolean replayed
) {
}
