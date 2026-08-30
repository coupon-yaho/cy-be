package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.util.Objects;

/** 쿠폰 회차 상세 발급률 조회가 사용하는 Prometheus 미터·라벨·PromQL 계약입니다. */
public final class CouponMetricsPrometheusContract {

    private static final String FLOW_TOTAL = "app_issuance_flow_total";
    private static final String COUPON_ID = "coupon_id";
    private static final String STAGE = "stage";
    private static final String SUCCESS = "success";

    /** 인스턴스화를 막습니다. */
    private CouponMetricsPrometheusContract() { }

    /**
     * 한 쿠폰 회차의 성공 발급 초당 rate를 모든 API 인스턴스에 걸쳐 합산하는 질의를 만듭니다.
     *
     * @param couponId 조회할 양수 쿠폰 ID
     * @param rateWindow Prometheus {@code rate}가 사용할 양수 관측 창
     * @return Counter reset과 인스턴스 합산을 Prometheus에 위임한 rate PromQL
     */
    public static String successRate(long couponId, Duration rateWindow) {
        requirePositiveCouponId(couponId);
        return "sum(rate(" + successSelector(couponId) + "[" + promDuration(rateWindow) + "]))";
    }

    /**
     * 성공 Counter의 실제 마지막 scrape 시각을 보수적으로 읽는 질의를 만듭니다.
     *
     * @param couponId 조회할 양수 쿠폰 ID
     * @return 인스턴스 중 가장 오래된 성공 Counter scrape epoch 질의
     */
    public static String successFreshnessEpoch(long couponId) {
        requirePositiveCouponId(couponId);
        return "min(timestamp(" + successSelector(couponId) + "))";
    }

    /** 성공 발급 Counter의 쿠폰·stage 라벨 selector를 만듭니다. */
    private static String successSelector(long couponId) {
        return FLOW_TOTAL + "{" + COUPON_ID + "=\"" + couponId + "\","
                + STAGE + "=\"" + SUCCESS + "\"}";
    }

    /** rate 구간을 PromQL이 받는 정수 분·초 표기로 변환합니다. */
    private static String promDuration(Duration duration) {
        Objects.requireNonNull(duration, "rateWindow");
        if (duration.isZero() || duration.isNegative() || duration.getNano() != 0) {
            throw new IllegalArgumentException("rateWindow은 양의 정수 초여야 합니다.");
        }
        long seconds = duration.toSeconds();
        return seconds % 60L == 0L ? (seconds / 60L) + "m" : seconds + "s";
    }

    /** 쿠폰 ID가 양수인지 확인합니다. */
    private static void requirePositiveCouponId(long couponId) {
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
    }
}
