package com.kafkick.batch.coupon.round;

import java.time.ZoneId;

import org.springframework.boot.context.properties.ConfigurationProperties;
@ConfigurationProperties(prefix = "coupon.round-generation")
public record CouponRoundGenerationProperties(
        int maxDays,
        String scheduleZone
) {

    public CouponRoundGenerationProperties {
        if (maxDays <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 예약 생성 범위는 0보다 커야 합니다."
            );
        }
        ZoneId.of(scheduleZone);
    }

    public ZoneId scheduleZoneId() {
        return ZoneId.of(scheduleZone);
    }
}
