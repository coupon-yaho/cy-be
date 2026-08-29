package com.kafkick.core.admin.campaignsource;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
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
            EngineVersion engineVersion,
            CouponRoundStatus status,
            Instant opensAt,
            Instant closesAt,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
            PreparationSource preparation
    ) {

        /** 캠페인 메타데이터와 독립 관측값이 누락되지 않도록 검증합니다. */
        public CampaignData {
            Objects.requireNonNull(campaignName, "campaignName");
            Objects.requireNonNull(brandName, "brandName");
            Objects.requireNonNull(engineVersion, "engineVersion");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(opensAt, "opensAt");
            Objects.requireNonNull(closesAt, "closesAt");
            Objects.requireNonNull(stock, "stock");
            Objects.requireNonNull(preparation, "preparation");
        }

        /**
         * 버전 필드 도입 전 호출부를 V1 계약으로 보존합니다.
         *
         * @deprecated 새 호출부는 회차 DB에서 읽은 {@link EngineVersion}을 명시해야 합니다.
         */
        @Deprecated
        public CampaignData(
                long couponId,
                String campaignName,
                String brandName,
                CouponRoundStatus status,
                Instant opensAt,
                Instant closesAt,
                CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
                PreparationSource preparation
        ) {
            this(couponId, campaignName, brandName, EngineVersion.V1, status,
                    opensAt, closesAt, stock, preparation);
        }

        /**
         * 이전 fixture의 최종 준비 관측을 DB 원천 계약으로 변환합니다.
         *
         * @param couponId 캠페인 식별자
         * @param campaignName 캠페인명
         * @param brandName 브랜드명
         * @param status 캠페인 상태
         * @param opensAt 오픈 시각
         * @param closesAt 종료 시각
         * @param stock 재고 관측값
         * @param preparation 이전 fixture의 최종 준비 관측값
         * @deprecated 새 생산 코드와 fixture는 {@link PreparationSource}를 전달해야 합니다.
         */
        @Deprecated
        public CampaignData(
                long couponId,
                String campaignName,
                String brandName,
                CouponRoundStatus status,
                Instant opensAt,
                Instant closesAt,
                CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
                PreparationObservation preparation
        ) {
            this(couponId, campaignName, brandName, EngineVersion.V1, status, opensAt, closesAt, stock,
                    preparationSource(preparation));
        }

        /** 이전 최종 fixture 값을 DB 원천 값으로만 보수적으로 옮깁니다. */
        private static PreparationSource preparationSource(PreparationObservation preparation) {
            Objects.requireNonNull(preparation, "preparation");
            if (!preparation.status().carriesValue()) {
                return new PreparationSource(null, null, null, preparation.status(), null);
            }
            boolean ready = Boolean.TRUE.equals(preparation.completed());
            return new PreparationSource(
                    ready,
                    ready,
                    CouponPolicyType.FIXED_AMOUNT,
                    preparation.status(),
                    preparation.observedAt());
        }
    }
}
