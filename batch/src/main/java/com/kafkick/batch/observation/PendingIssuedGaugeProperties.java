package com.kafkick.batch.observation;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("observation.pending-issued-gauge")
public record PendingIssuedGaugeProperties(
    Boolean enabled,
    Duration interval,
    Duration staleAfter,
    Integer scanCount,
    String issuedKey
) {

    private static final String COUPON_ROUND_ID = "couponRoundId";

    public PendingIssuedGaugeProperties {
        enabled = enabled != null ? enabled : false;
        interval = interval != null ? interval : Duration.ofSeconds(30);
        staleAfter = staleAfter != null ? staleAfter : Duration.ofMinutes(5);
        scanCount = scanCount != null ? scanCount : 200;
        issuedKey = issuedKey != null && !issuedKey.isBlank()
            ? issuedKey
            : "cy:v2:issued:{" + COUPON_ROUND_ID + "}";
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval은 양수여야 합니다.");
        }
        if (staleAfter.isNegative()) {
            throw new IllegalArgumentException("stale-after는 음수일 수 없습니다.");
        }
        if (scanCount <= 0) {
            throw new IllegalArgumentException("scan-count는 양수여야 합니다.");
        }
        if (!issuedKey.contains("{" + COUPON_ROUND_ID + "}")) {
            throw new IllegalArgumentException("issued-key에는 {couponRoundId}가 있어야 합니다.");
        }
    }

    public String issuedKey(long couponRoundId) {
        return issuedKey.replace(
            "{" + COUPON_ROUND_ID + "}", "{" + couponRoundId + "}");
    }
}
