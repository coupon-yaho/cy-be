package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.SourceStatus;

/** 원천 상태를 전체 응답 완전성으로 축약하는 계산 경계를 검증합니다. */
class OverviewStatusCalculatorTest {

    private static final Instant NOW = Instant.parse("2026-08-21T03:00:00Z");

    @Test
    @DisplayName("적용 가능한 모든 원천이 값 있으면 COMPLETE로 판정한다")
    void returnsCompleteWhenEveryApplicableSourceCarriesAValue() {
        OverviewStatusCalculator calculator = new OverviewStatusCalculator();

        OverallStatus result = calculator.calculate(snapshotWithQueueRisk(valid(
                org.mockito.Mockito.mock(AdminOverviewSnapshot.QueueRiskSummary.class))));

        assertThat(result).isEqualTo(OverallStatus.COMPLETE);
    }

    @Test
    @DisplayName("일부 핵심 원천만 미수집이면 PARTIAL로 판정한다")
    void returnsPartialWhenOneApplicableSourceIsPending() {
        OverviewStatusCalculator calculator = new OverviewStatusCalculator();

        OverallStatus result = calculator.calculate(snapshotWithQueueRisk(
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null)));

        assertThat(result).isEqualTo(OverallStatus.PARTIAL);
    }

    /** 해석할 수 있는 핵심 원천이 없는데 부분 응답으로 표시하는 회귀를 방지합니다. */
    @Test
    @DisplayName("핵심 원천이 모두 미수집이면 UNAVAILABLE로 판정한다")
    void returnsUnavailableWhenEveryCoreSourceIsUnavailable() {
        OverviewStatusCalculator calculator = new OverviewStatusCalculator();
        AdminOverviewSnapshot snapshot = unavailableSnapshot();

        OverallStatus result = calculator.calculate(snapshot);

        assertThat(result).isEqualTo(OverallStatus.UNAVAILABLE);
    }

    /** 실제 관측값이 없는 Snapshot을 공통 상태 불변식에 맞춰 생성합니다. */
    private static AdminOverviewSnapshot unavailableSnapshot() {
        return new AdminOverviewSnapshot(
                Instant.parse("2026-08-21T03:00:00Z"),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable()
        );
    }

    private static AdminOverviewSnapshot snapshotWithQueueRisk(
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.QueueRiskSummary> queueRisk
    ) {
        return new AdminOverviewSnapshot(
                NOW,
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.ActionRequiredSummary.class)),
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.OpeningSoonSummary.class)),
                queueRisk,
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.StockRiskSummary.class)),
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.AggregateIssuanceRate.class)),
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.AggregateQueue.class)),
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.LatencySummary.class)),
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.CouponRoundStatusSummary.class)),
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.ActionItemSnapshot.class)),
                valid(java.util.List.of()),
                valid(org.mockito.Mockito.mock(AdminOverviewSnapshot.CustomerOutcomeSummary.class)));
    }

    private static <T> AdminOverviewSnapshot.Observation<T> valid(T value) {
        return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, NOW);
    }

    /** 실제 값과 관측 시각이 없는 미수집 상태를 대상 계약 타입에 맞춰 생성합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> unavailable() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }
}
