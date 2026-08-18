// 쿠폰 상태 변경 멱등 요청의 재조회 간격과 실패 선점 회수 시간을 외부 설정으로 관리합니다.
package com.kafkick.api.coupon.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "coupon.idempotency")
public record CouponIdempotencyProperties(
        @DefaultValue("1s") Duration waitTimeout,
        @DefaultValue("50ms") Duration pollInterval,
        @DefaultValue("30s") Duration staleAfter
) {

    public CouponIdempotencyProperties {
        if (waitTimeout == null || waitTimeout.isNegative()) {
            throw new IllegalArgumentException(
                    "멱등 요청 대기 시간은 0 이상이어야 합니다."
            );
        }
        if (pollInterval == null || pollInterval.isZero()
                || pollInterval.isNegative()) {
            throw new IllegalArgumentException(
                    "멱등 요청 재조회 간격은 0보다 커야 합니다."
            );
        }
        if (staleAfter == null || staleAfter.isZero()
                || staleAfter.isNegative()) {
            throw new IllegalArgumentException(
                    "멱등 요청 회수 기준 시간은 0보다 커야 합니다."
            );
        }
    }
}
