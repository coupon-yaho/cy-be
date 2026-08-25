package com.kafkick.core.admin.campaignsource;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 운영현황의 DB 캠페인 모집단과 해당 조회 상태입니다. */
public record AdminCampaignCatalog(
        SourceStatus status,
        Instant observedAt,
        List<CampaignData> campaigns
) {

    /** 카탈로그 상태와 값·관측 시각의 조합, 그리고 목록 불변성을 검증합니다. */
    public AdminCampaignCatalog {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(campaigns, "campaigns");
        campaigns = List.copyOf(campaigns);
        switch (status) {
            case VALID -> Objects.requireNonNull(observedAt, "observedAt");
            case PENDING, UNAVAILABLE -> {
                if (observedAt != null || !campaigns.isEmpty()) {
                    throw new IllegalArgumentException(status + " 카탈로그는 observedAt 없이 빈 목록이어야 합니다.");
                }
            }
            default -> throw new IllegalArgumentException("카탈로그에 지원하지 않는 상태입니다: " + status);
        }
    }

    /** 카탈로그의 한 캠페인 기본 정보와 독립 재고·준비 관측값입니다. */
    public record CampaignData(
            long couponId,
            String campaignName,
            String brandName,
            CouponRoundStatus status,
            Instant opensAt,
            Instant closesAt,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
            PreparationObservation preparation
    ) {

        /** 캠페인 메타데이터와 독립 관측값이 누락되지 않도록 검증합니다. */
        public CampaignData {
            Objects.requireNonNull(campaignName, "campaignName");
            Objects.requireNonNull(brandName, "brandName");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(opensAt, "opensAt");
            Objects.requireNonNull(closesAt, "closesAt");
            Objects.requireNonNull(stock, "stock");
            Objects.requireNonNull(preparation, "preparation");
        }
    }
}
