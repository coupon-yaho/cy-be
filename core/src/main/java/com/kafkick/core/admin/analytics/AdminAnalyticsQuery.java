package com.kafkick.core.admin.analytics;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Objects;

/** 관리자 브랜드 분석에 적용할 기간과 선택 필터를 기술 중립적으로 전달합니다. */
public record AdminAnalyticsQuery(
        LocalDate from,
        LocalDate to,
        Long brandId,
        Long couponId,
        ZoneId zoneId
) {

    /** 필수 기간·시간대와 식별자 범위를 검증합니다. */
    public AdminAnalyticsQuery {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(zoneId, "zoneId");
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("from은 to보다 늦을 수 없습니다.");
        }
        requirePositiveIfPresent(brandId, "brandId");
        requirePositiveIfPresent(couponId, "couponId");
    }

    /** 선택 식별자가 존재할 때만 양수 규칙을 적용합니다. */
    private static void requirePositiveIfPresent(Long value, String name) {
        if (value != null && value <= 0L) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
    }
}
