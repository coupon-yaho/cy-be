package com.kafkick.core.admin.analytics;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CampaignRef;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/** 관리자 브랜드 분석의 원천 조회·필터 검증·계산 수명주기를 조립합니다. */
public final class AdminAnalyticsService {

    private final AdminAnalyticsSource source;
    private final TimeProvider timeProvider;
    private final AdminAnalyticsCalculator calculator;

    /** 조회 경계, 요청 기준 시각 공급자와 순수 계산기를 주입받습니다. */
    public AdminAnalyticsService(
            AdminAnalyticsSource source,
            TimeProvider timeProvider,
            AdminAnalyticsCalculator calculator
    ) {
        this.source = Objects.requireNonNull(source, "source");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.calculator = Objects.requireNonNull(calculator, "calculator");
    }

    /** 요청당 Source와 현재 시각을 한 번씩만 읽어 독립 분석 결과를 반환합니다. */
    public AdminAnalyticsResult getAnalytics(AdminAnalyticsQuery query) {
        Objects.requireNonNull(query, "query");
        AdminAnalyticsDataset dataset = source.load(query);
        validateRequestedCatalog(query, dataset);
        // 세 분석이 Freshness 임계점에서 서로 갈리지 않도록 기준 시각을 한 번만 확정합니다.
        Instant evaluatedAt = timeProvider.instant();
        return calculator.calculate(query, dataset, evaluatedAt);
    }

    /** 카탈로그가 확인된 경우에만 미존재와 브랜드·캠페인 소속 불일치를 404로 판정합니다. */
    private static void validateRequestedCatalog(
            AdminAnalyticsQuery query,
            AdminAnalyticsDataset dataset
    ) {
        if (dataset.catalog().availability() != AggregateAvailability.AVAILABLE) {
            // 카탈로그 미수집을 존재하지 않는 ID로 오판하지 않고 분석별 PENDING/UNAVAILABLE을 보존합니다.
            return;
        }
        if (query.brandId() != null && dataset.catalog().brands().stream()
                .noneMatch(brand -> brand.brandId() == query.brandId())) {
            throw notFound(
                    AdminAnalyticsErrorCode.BRAND_NOT_FOUND,
                    "브랜드를 찾을 수 없습니다: " + query.brandId());
        }
        CampaignRef requestedCampaign = null;
        if (query.couponId() != null) {
            requestedCampaign = dataset.catalog().campaigns().stream()
                    .filter(campaign -> campaign.couponId() == query.couponId())
                    .findFirst()
                    .orElseThrow(() -> notFound(
                            AdminAnalyticsErrorCode.CAMPAIGN_NOT_FOUND,
                            "캠페인을 찾을 수 없습니다: " + query.couponId()));
        }
        if (requestedCampaign != null
                && query.brandId() != null
                && requestedCampaign.brandId() != query.brandId()) {
            throw notFound(
                    AdminAnalyticsErrorCode.CAMPAIGN_BRAND_MISMATCH,
                    "캠페인이 요청 브랜드에 속하지 않습니다: " + query.couponId());
        }
    }

    /** 분석 도메인의 404 원인을 구분하고 상세 식별자는 예외 로그 메시지에 남깁니다. */
    private static BusinessException notFound(
            AdminAnalyticsErrorCode errorCode,
            String detail
    ) {
        return new BusinessException(errorCode, detail);
    }
}
