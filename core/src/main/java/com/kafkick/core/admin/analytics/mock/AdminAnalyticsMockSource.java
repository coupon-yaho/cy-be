package com.kafkick.core.admin.analytics.mock;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset;
import com.kafkick.core.admin.analytics.AdminAnalyticsQuery;
import com.kafkick.core.admin.analytics.AdminAnalyticsSource;

/** 고정 집계 완료 시각의 Mock 행을 `load(query)` 계약으로 제공하는 Source입니다. */
public final class AdminAnalyticsMockSource implements AdminAnalyticsSource {

    private final AdminAnalyticsMockDataFactory factory;
    private final Instant observedAt;

    /** Mock Factory와 데모 응답의 집계 완료 시각을 고정합니다. */
    public AdminAnalyticsMockSource(AdminAnalyticsMockDataFactory factory, Instant observedAt) {
        this.factory = Objects.requireNonNull(factory, "factory");
        this.observedAt = Objects.requireNonNull(observedAt, "observedAt");
    }

    /** 요청 조건이 적용된 원천 집계 행을 반환합니다. */
    @Override
    public AdminAnalyticsDataset load(AdminAnalyticsQuery query) {
        return factory.create(query, observedAt);
    }
}
