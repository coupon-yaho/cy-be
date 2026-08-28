package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class CouponMetricsPrometheusContractTest {

    @Test
    void buildsTheSuccessRateQueryWithTheFixedFiveSecondWindow() {
        assertThat(CouponMetricsPrometheusContract.successRate(10L, Duration.ofSeconds(5)))
                .isEqualTo("sum(rate(app_issuance_flow_total{coupon_id=\"10\",stage=\"success\"}[5s]))");
    }

    @Test
    void rejectsInvalidCouponIdsAndRateWindowsBeforeBuildingQueries() {
        assertThatThrownBy(() -> CouponMetricsPrometheusContract.successRate(0L, Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CouponMetricsPrometheusContract.successRate(10L, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
