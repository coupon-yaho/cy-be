package com.kafkick.core.admin.analytics;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.analytics.AdminAnalyticsResult.IssuanceStatusDistribution;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult.StatusCount;
import com.kafkick.core.coupon.IssuanceStatus;

/** 외부로 공개되는 분석 결과가 현재 상태 분포 불변식을 보존하는지 검증합니다. */
class AdminAnalyticsResultTest {

    /** total과 상태 합계가 다르면 정상 계산 결과로 생성되지 않는지 검증합니다. */
    @Test
    @DisplayName("현재 상태 분포는 네 상태 합계와 totalIssued가 같아야 한다")
    void statusDistributionRejectsBrokenTotal() {
        assertThatThrownBy(() -> new IssuanceStatusDistribution(
                10L,
                6L,
                List.of(
                        new StatusCount(IssuanceStatus.ISSUED, 6L, 0.6D),
                        new StatusCount(IssuanceStatus.USED, 2L, 0.2D),
                        new StatusCount(IssuanceStatus.CANCELLED, 1L, 0.1D),
                        new StatusCount(IssuanceStatus.EXPIRED, 0L, 0D))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("totalIssued");
    }
}
