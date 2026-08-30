package com.kafkick.api.observation.issuance;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Settings for the bounded, in-memory coupon round meter registry. */
@ConfigurationProperties(prefix = "observation.issuance.coupon-round")
public record CouponRoundMeterProperties(
        Integer maxActiveCouponRounds,
        Duration retireGracePeriod,
        Duration tombstoneRetention,
        Integer tombstoneMaxEntries
) {

    public static final int DEFAULT_MAX_ACTIVE_COUPON_ROUNDS = 100;
    public static final Duration DEFAULT_RETIRE_GRACE_PERIOD = Duration.ofMinutes(30);
    // Until operations supplies retention requirements, one day covers delayed retries while 1,000 entries
    // bounds memory to ten times the active coupon-round cap.
    public static final Duration DEFAULT_TOMBSTONE_RETENTION = Duration.ofDays(1);
    public static final int DEFAULT_TOMBSTONE_MAX_ENTRIES = 1_000;

    public int resolvedMaxActiveCouponRounds() {
        return positive(maxActiveCouponRounds, DEFAULT_MAX_ACTIVE_COUPON_ROUNDS, "maxActiveCouponRounds");
    }

    public Duration resolvedRetireGracePeriod() {
        return positive(retireGracePeriod, DEFAULT_RETIRE_GRACE_PERIOD, "retireGracePeriod");
    }

    public Duration resolvedTombstoneRetention() {
        return positive(tombstoneRetention, DEFAULT_TOMBSTONE_RETENTION, "tombstoneRetention");
    }

    public int resolvedTombstoneMaxEntries() {
        return positive(tombstoneMaxEntries, DEFAULT_TOMBSTONE_MAX_ENTRIES, "tombstoneMaxEntries");
    }

    private static int positive(Integer value, int fallback, String name) {
        int resolved = value == null ? fallback : value;
        if (resolved <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return resolved;
    }

    private static Duration positive(Duration value, Duration fallback, String name) {
        Duration resolved = value == null ? fallback : value;
        if (resolved.isZero() || resolved.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return resolved;
    }
}
