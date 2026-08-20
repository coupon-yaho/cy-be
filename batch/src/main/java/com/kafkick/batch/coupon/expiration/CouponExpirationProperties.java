package com.kafkick.batch.coupon.expiration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "coupon.expiration")
public record CouponExpirationProperties(
        @DefaultValue("500") int chunkSize,
        @DefaultValue("100") int transactionSize
) {

    public CouponExpirationProperties {
        if (chunkSize <= 0 || transactionSize <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 만료 배치 크기는 0보다 커야 합니다."
            );
        }
    }
}
