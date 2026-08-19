// 쿠폰 만료 배치의 keyset 청크 크기를 외부 설정으로 관리합니다.
package com.kafkick.batch.coupon.expiration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "coupon.expiration")
public record CouponExpirationProperties(
        @DefaultValue("500") int chunkSize
) {

    public CouponExpirationProperties {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 만료 배치 청크 크기는 0보다 커야 합니다."
            );
        }
    }
}
