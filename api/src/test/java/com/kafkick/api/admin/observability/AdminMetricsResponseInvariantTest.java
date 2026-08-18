package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.Severity;

/** 관리자 Metrics 응답이 범위와 정합성 단계의 생성 불변식을 지키는지 검증합니다. */
class AdminMetricsResponseInvariantTest {

    /** GLOBAL은 쿠폰과 Benchmark 실행 식별자를 가질 수 없습니다. */
    @Test
    void globalScopeRejectsIdentifiers() {
        assertThatCode(() -> scope(AdminMetricsResponse.MetricsScopeType.GLOBAL, null, null))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.GLOBAL, 1L, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.GLOBAL, null, 1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** COUPON은 양수 couponId 하나만 요구합니다. */
    @Test
    void couponScopeRequiresOnlyPositiveCouponId() {
        assertThatCode(() -> scope(AdminMetricsResponse.MetricsScopeType.COUPON, 1L, null))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.COUPON, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.COUPON, 0L, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.COUPON, -1L, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.COUPON, 1L, 2L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** BENCHMARK_RUN은 양수 benchmarkRunId 하나만 요구합니다. */
    @Test
    void benchmarkScopeRequiresOnlyPositiveRunId() {
        assertThatCode(() -> scope(AdminMetricsResponse.MetricsScopeType.BENCHMARK_RUN, null, 1L))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.BENCHMARK_RUN, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.BENCHMARK_RUN, null, -1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.BENCHMARK_RUN, null, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> scope(AdminMetricsResponse.MetricsScopeType.BENCHMARK_RUN, 1L, 2L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 범위 유형이 없으면 식별자를 해석할 수 없으므로 생성에 실패합니다. */
    @Test
    void scopeRejectsNullType() {
        assertThatThrownBy(() -> scope(null, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    /** LIVE 단계는 verdict 없이 현재 severity를 선택적으로 표현할 수 있습니다. */
    @Test
    void liveConsistencyRejectsVerdict() {
        assertThatCode(() -> consistency(ConsistencyPhase.LIVE, null, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> consistency(ConsistencyPhase.LIVE, null, Severity.WARN))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> consistency(ConsistencyPhase.LIVE, Verdict.PASS, Severity.NONE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** FINAL 단계는 verdict와 severity가 모두 있어야 합니다. */
    @Test
    void finalConsistencyRequiresVerdictAndSeverity() {
        assertThatCode(() -> consistency(ConsistencyPhase.FINAL, Verdict.PASS, Severity.NONE))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> consistency(ConsistencyPhase.FINAL, null, Severity.NONE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> consistency(ConsistencyPhase.FINAL, Verdict.FAIL, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 정합성 단계를 생략하면 verdict와 severity 조합을 해석할 수 없습니다. */
    @Test
    void consistencyRejectsNullPhase() {
        assertThatThrownBy(() -> consistency(null, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    private AdminMetricsResponse.MetricsScope scope(
            AdminMetricsResponse.MetricsScopeType type, Long couponId, Long benchmarkRunId) {
        return new AdminMetricsResponse.MetricsScope(type, couponId, benchmarkRunId);
    }

    private AdminMetricsResponse.ConsistencyResponse consistency(
            ConsistencyPhase phase, Verdict verdict, Severity severity) {
        return new AdminMetricsResponse.ConsistencyResponse(
                phase, verdict, severity, null, null, null, null, null);
    }
}
