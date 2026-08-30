package com.kafkick.core.admin.couponroundsource;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 운영현황의 DB 쿠폰 회차 모집단과 해당 조회 상태입니다. */
public record AdminCouponRoundCatalog(
        SourceStatus status,
        Instant observedAt,
        List<CouponRoundData> couponRounds
) {

    /** 카탈로그 상태와 값·관측 시각의 조합, 그리고 목록 불변성을 검증합니다. */
    public AdminCouponRoundCatalog {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(couponRounds, "couponRounds");
        couponRounds = List.copyOf(couponRounds);
        switch (status) {
            case VALID -> Objects.requireNonNull(observedAt, "observedAt");
            case PENDING, UNAVAILABLE -> {
                if (observedAt != null || !couponRounds.isEmpty()) {
                    throw new IllegalArgumentException(status + " 카탈로그는 observedAt 없이 빈 목록이어야 합니다.");
                }
            }
            default -> throw new IllegalArgumentException("카탈로그에 지원하지 않는 상태입니다: " + status);
        }
    }

    /** 카탈로그의 한 쿠폰 회차 기본 정보와 독립 재고·준비 관측값입니다. */
    public record CouponRoundData(
            long couponId,
            String couponName,
            String brandName,
            EngineVersion engineVersion,
            CouponRoundStatus status,
            Instant opensAt,
            Instant closesAt,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
            PreparationSource preparation
    ) {

        /** 쿠폰 회차 메타데이터와 독립 관측값이 누락되지 않도록 검증합니다. */
        public CouponRoundData {
            Objects.requireNonNull(couponName, "couponName");
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
        public CouponRoundData(
                long couponId,
                String couponName,
                String brandName,
                CouponRoundStatus status,
                Instant opensAt,
                Instant closesAt,
                CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
                PreparationSource preparation
        ) {
            this(couponId, couponName, brandName, EngineVersion.V1, status,
                    opensAt, closesAt, stock, preparation);
        }

        /**
         * 이전 fixture의 최종 준비 관측을 DB 원천 계약으로 변환합니다.
         *
         * @param couponId 쿠폰 회차 식별자
         * @param couponName 쿠폰 회차명
         * @param brandName 브랜드명
         * @param status 쿠폰 회차 상태
         * @param opensAt 오픈 시각
         * @param closesAt 종료 시각
         * @param stock 재고 관측값
         * @param preparation 이전 fixture의 최종 준비 관측값
         * @deprecated 새 생산 코드와 fixture는 {@link PreparationSource}를 전달해야 합니다.
         */
        @Deprecated
        public CouponRoundData(
                long couponId,
                String couponName,
                String brandName,
                CouponRoundStatus status,
                Instant opensAt,
                Instant closesAt,
                CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
                PreparationObservation preparation
        ) {
            this(couponId, couponName, brandName, EngineVersion.V1, status, opensAt, closesAt, stock,
                    preparationSource(preparation));
        }

        /** 이전 최종 fixture 값을 DB 원천 값으로만 보수적으로 옮깁니다. */
        private static PreparationSource preparationSource(PreparationObservation preparation) {
            Objects.requireNonNull(preparation, "preparation");
            if (!preparation.status().carriesValue()) {
                return new PreparationSource(null, null, null, null, preparation.status(), null);
            }
            boolean ready = Boolean.TRUE.equals(preparation.completed());
            return new PreparationSource(
                    ready,
                    ready,
                    CouponPolicyType.FIXED_AMOUNT,
                    1,
                    preparation.status(),
                    preparation.observedAt());
        }
    }
}
