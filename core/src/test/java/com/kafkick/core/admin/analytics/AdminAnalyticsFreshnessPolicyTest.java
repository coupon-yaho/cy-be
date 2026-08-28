package com.kafkick.core.admin.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

/** 관리자 분석 최신성 정책의 임계 시각과 값 미보유 상태 변환을 검증합니다. */
class AdminAnalyticsFreshnessPolicyTest {

    private static final Instant EVALUATED_AT = Instant.parse("2026-08-23T02:00:00Z");

    /** 허용 시간과 정확히 같은 나이는 아직 VALID이고 그 직후부터 STALE인지 검증합니다. */
    @Test
    @DisplayName("Freshness 임계 시각까지 VALID이고 초과하면 STALE이다")
    void changesToStaleOnlyAfterThreshold() {
        AdminAnalyticsFreshnessPolicy policy =
                new AdminAnalyticsFreshnessPolicy(Duration.ofHours(1));

        assertThat(policy.evaluate(
                AggregateAvailability.AVAILABLE,
                EVALUATED_AT.minus(Duration.ofHours(1)),
                EVALUATED_AT)).isEqualTo(SourceStatus.VALID);
        assertThat(policy.evaluate(
                AggregateAvailability.AVAILABLE,
                EVALUATED_AT.minus(Duration.ofHours(1)).minusNanos(1L),
                EVALUATED_AT)).isEqualTo(SourceStatus.STALE);
    }

    /** 원천 미수집과 장애 상태는 관측 시각 없이 각각 보존되는지 검증합니다. */
    @Test
    @DisplayName("PENDING과 UNAVAILABLE은 Freshness 계산 없이 보존한다")
    void preservesUnavailableStates() {
        AdminAnalyticsFreshnessPolicy policy = AdminAnalyticsFreshnessPolicy.pendingOnly();

        assertThat(policy.evaluate(AggregateAvailability.PENDING, null, EVALUATED_AT))
                .isEqualTo(SourceStatus.PENDING);
        assertThat(policy.evaluate(AggregateAvailability.UNAVAILABLE, null, EVALUATED_AT))
                .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 임계값 없는 기본 배선이 실제 값을 정상으로 오인하지 못하도록 차단하는지 검증합니다. */
    @Test
    @DisplayName("Pending 전용 정책은 AVAILABLE 값을 판정하지 않는다")
    void pendingOnlyPolicyRejectsAvailableValue() {
        assertThatThrownBy(() -> AdminAnalyticsFreshnessPolicy.pendingOnly().evaluate(
                AggregateAvailability.AVAILABLE, EVALUATED_AT, EVALUATED_AT))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("ANALYTICS-001");
    }
}
