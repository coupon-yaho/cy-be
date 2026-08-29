package com.kafkick.core.admin.campaignsource;

import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.observation.EngineVersion;

/** 관리자 캠페인 상세 조회의 존재 여부와 DB 원천값입니다. */
public record AdminCampaignDetailData(DetailAvailability availability, DetailValue value) {

    /** 찾음·없음·DB 조회 불가 상태가 값을 잘못 동반하지 않도록 검증합니다. */
    public AdminCampaignDetailData {
        Objects.requireNonNull(availability, "availability");
        if (availability == DetailAvailability.AVAILABLE && value == null) {
            throw new IllegalArgumentException("AVAILABLE 상세 조회에는 value가 필요합니다.");
        }
        if (availability != DetailAvailability.AVAILABLE && value != null) {
            throw new IllegalArgumentException(availability + " 상세 조회에는 value가 없어야 합니다.");
        }
    }

    /** 한 캠페인의 기본 정보, 재고, 보유 상태와 전이 집계 원천입니다. */
    public record DetailValue(
            long couponId,
            String campaignName,
            String brandName,
            EngineVersion engineVersion,
            CouponMetricsSource.CampaignRuntime campaign,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
            CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdingCounts,
            CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> transitions
    ) {

        /** 상세 화면이 계산할 모든 독립 원천값을 명시적으로 요구합니다. */
        public DetailValue {
            Objects.requireNonNull(campaignName, "campaignName");
            Objects.requireNonNull(brandName, "brandName");
            Objects.requireNonNull(engineVersion, "engineVersion");
            Objects.requireNonNull(campaign, "campaign");
            Objects.requireNonNull(stock, "stock");
            Objects.requireNonNull(holdingCounts, "holdingCounts");
            Objects.requireNonNull(transitions, "transitions");
        }

        /**
         * 버전 필드 도입 전 호출부를 V1 계약으로 보존합니다.
         *
         * @deprecated 새 호출부는 회차 DB에서 읽은 {@link EngineVersion}을 명시해야 합니다.
         */
        @Deprecated
        public DetailValue(
                long couponId,
                String campaignName,
                String brandName,
                CouponMetricsSource.CampaignRuntime campaign,
                CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
                CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdingCounts,
                CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> transitions
        ) {
            this(couponId, campaignName, brandName, EngineVersion.V1,
                    campaign, stock, holdingCounts, transitions);
        }
    }
}
