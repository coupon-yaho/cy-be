// 회차별 만료 요청 건수와 실제 상태 전이 성공 건수를 반환합니다.
package com.kafkick.core.coupon.service;

public record CouponExpirationResult(
        int requestedCount,
        int expiredCount
) {
}
