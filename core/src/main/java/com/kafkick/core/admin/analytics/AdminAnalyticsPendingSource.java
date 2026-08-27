package com.kafkick.core.admin.analytics;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateObservation;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CatalogSnapshot;

/** 실제 집계 저장소가 연결되기 전 분석별 PENDING을 제공합니다. */
public final class AdminAnalyticsPendingSource implements AdminAnalyticsSource {

    /** ID 존재를 추측하지 않고 세 분석을 미집계 상태로 반환합니다. */
    @Override
    public AdminAnalyticsDataset load(AdminAnalyticsQuery query) {
        return pendingDataset();
    }

    /** 테스트와 기본 배선이 공유할 기술 중립 PENDING Dataset을 만듭니다. */
    public static AdminAnalyticsDataset pendingDataset() {
        return new AdminAnalyticsDataset(
                AnalyticsSourceType.NONE,
                CatalogSnapshot.pending(),
                AggregateObservation.pending(),
                AggregateObservation.pending(),
                AggregateObservation.pending());
    }
}
