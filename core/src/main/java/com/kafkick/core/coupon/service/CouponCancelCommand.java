// 회원 소유 쿠폰의 발급 취소에 필요한 식별자와 단일 기준 시각을 전달합니다.
package com.kafkick.core.coupon.service;

import java.time.Instant;

public record CouponCancelCommand(
        Long issuanceId,
        Long memberId,
        String idempotencyKey,
        Instant canceledAt
) {
}
