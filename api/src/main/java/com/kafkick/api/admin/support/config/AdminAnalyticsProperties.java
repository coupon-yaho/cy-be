package com.kafkick.api.admin.support.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** 관리자 분석 API의 조회 범위와 개발용 Mock 노출 및 최신성 설정입니다. */
@ConfigurationProperties(prefix = "admin.analytics")
public record AdminAnalyticsProperties(
        @DefaultValue("1") int maxRangeYears,
        @DefaultValue("true") boolean mockEnabled,
        Duration staleAfter
) {

    private static final Duration DEFAULT_STALE_AFTER = Duration.ofHours(24);

    /** 조회 범위와 분석 원천에 공통으로 적용할 최신성 기준을 검증합니다. */
    @ConstructorBinding
    public AdminAnalyticsProperties {
        if (maxRangeYears < 1) {
            throw new IllegalArgumentException("admin.analytics.max-range-years는 1 이상이어야 합니다.");
        }
        if (staleAfter != null && (staleAfter.isZero() || staleAfter.isNegative())) {
            throw new IllegalArgumentException("admin.analytics.stale-after는 양수여야 합니다.");
        }
        if (staleAfter == null) {
            // 실제 원천으로 교체돼도 최신성 판정이 비활성화되지 않도록 동일한 기본값을 적용합니다.
            staleAfter = DEFAULT_STALE_AFTER;
        }
    }

    /** 기존 조회 기간 검증 테스트가 Mock 활성 기본 설정을 간단히 구성하도록 제공합니다. */
    public AdminAnalyticsProperties(int maxRangeYears) {
        this(maxRangeYears, true, null);
    }
}
