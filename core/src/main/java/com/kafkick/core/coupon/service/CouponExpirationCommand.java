// 동일 회차의 만료 후보와 한 번 고정한 배치 기준 시각을 전달합니다.
package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupon.domain.Issuance;

public record CouponExpirationCommand(
        Long couponRoundId,
        List<Issuance> issuances,
        Instant asOf
) {
}
