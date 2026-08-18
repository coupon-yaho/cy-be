package com.kafkick.api.admin.support.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** 관리자 분석 API가 한 요청에서 조회할 수 있는 최대 달력 연도 수입니다. */
@ConfigurationProperties(prefix = "admin.analytics")
public record AdminAnalyticsProperties(@DefaultValue("1") int maxRangeYears) {

    public AdminAnalyticsProperties {
        if (maxRangeYears < 1) {
            throw new IllegalArgumentException("admin.analytics.max-range-years는 1 이상이어야 합니다.");
        }
    }
}
