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

    private static final List<StatusCount> VALID_STATUSES = List.of(
            new StatusCount(IssuanceStatus.ISSUED, 6L, 0.6D),
            new StatusCount(IssuanceStatus.USED, 2L, 0.2D),
            new StatusCount(IssuanceStatus.CANCELLED, 1L, 0.1D),
            new StatusCount(IssuanceStatus.EXPIRED, 1L, 0.1D));

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

    /** 동일한 현재 상태가 두 번 들어오면 분포 의미가 모호해지므로 거부하는지 검증합니다. */
    @Test
    @DisplayName("현재 상태 분포는 중복 상태를 허용하지 않는다")
    void statusDistributionRejectsDuplicateStatus() {
        assertThatThrownBy(() -> new IssuanceStatusDistribution(
                10L,
                6L,
                List.of(
                        VALID_STATUSES.get(0),
                        VALID_STATUSES.get(1),
                        VALID_STATUSES.get(2),
                        new StatusCount(IssuanceStatus.CANCELLED, 1L, 0.1D))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("중복");
    }

    /** 네 상태 중 하나라도 빠지면 완전한 현재 상태 분포가 아니므로 거부하는지 검증합니다. */
    @Test
    @DisplayName("현재 상태 분포에는 네 상태가 모두 필요하다")
    void statusDistributionRejectsMissingStatus() {
        assertThatThrownBy(() -> new IssuanceStatusDistribution(
                10L,
                6L,
                VALID_STATUSES.subList(0, 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("네 현재 상태");
    }

    /** 수량으로 다시 계산한 비율과 다른 입력은 화면 통계를 오염시키므로 거부하는지 검증합니다. */
    @Test
    @DisplayName("현재 상태 비율은 totalIssued 기준 계산과 같아야 한다")
    void statusDistributionRejectsInconsistentRatio() {
        assertThatThrownBy(() -> new IssuanceStatusDistribution(
                10L,
                6L,
                List.of(
                        new StatusCount(IssuanceStatus.ISSUED, 6L, 0.5D),
                        VALID_STATUSES.get(1),
                        VALID_STATUSES.get(2),
                        VALID_STATUSES.get(3))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("비율");
    }

    /** 현재 미사용 수량이 ISSUED 버킷과 다르면 같은 의미의 두 필드가 갈라지므로 거부하는지 검증합니다. */
    @Test
    @DisplayName("currentlyIssued는 ISSUED 상태 수량과 같아야 한다")
    void statusDistributionRejectsCurrentlyIssuedMismatch() {
        assertThatThrownBy(() -> new IssuanceStatusDistribution(
                10L,
                5L,
                VALID_STATUSES))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("currentlyIssued");
    }
}
