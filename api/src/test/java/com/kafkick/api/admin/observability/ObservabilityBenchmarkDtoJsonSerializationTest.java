package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.benchmark.dto.BenchmarkListResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** 관측 지표와 Benchmark 목록 DTO의 독립 상태·nullable 판정 JSON 구조를 검증합니다. */
class ObservabilityBenchmarkDtoJsonSerializationTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-16T00:00:00Z");

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 범위·정합성 단계와 각 트래픽 원천 상태가 독립적으로 직렬화되는지 검증합니다. */
    @Test
    void metricsSerializesScopeConsistencyAndIndependentTrafficStates() throws Exception {
        ObservedValue<Double> pendingRate = new ObservedValue<>(null, SourceStatus.PENDING, null);
        AdminMetricsResponse response = new AdminMetricsResponse(
                new AdminMetricsResponse.MetricsScope(AdminMetricsResponse.MetricsScopeType.GLOBAL, null, null),
                OBSERVED_AT,
                MetricsWindow.ONE_MINUTE,
                new AdminMetricsResponse.ConsistencyResponse(
                        ConsistencyPhase.LIVE, null, Severity.NONE, null, null, null, null, null
                ),
                new AdminMetricsResponse.TrafficMetrics(
                        pendingRate, pendingRate, pendingRate, pendingRate, pendingRate
                ),
                new AdminMetricsResponse.LatencyMetrics(null, null, null),
                new AdminMetricsResponse.DependencyMetrics(null, null, null),
                null,
                List.of()
        );

        String json = objectMapper.writeValueAsString(response);

        assertThat(json)
                .contains("\"type\":\"GLOBAL\"")
                .contains("\"phase\":\"LIVE\"")
                .contains("\"verdict\":null")
                .contains("\"issueAttemptRps\":{\"value\":null,\"state\":\"PENDING\"")
                .contains("\"circuitBreakers\":[]");
    }

    /** 최종 판정과 네 ConsistencyGapType의 관리자 공개 필드 매핑을 JSON 이름으로 고정합니다. */
    @Test
    void finalMetricsSerializesCanonicalVerdictAndMappedGapFields() throws Exception {
        ObservedValue<Long> zeroGap = new ObservedValue<>(0L, SourceStatus.VALID, OBSERVED_AT);
        AdminMetricsResponse response = new AdminMetricsResponse(
                new AdminMetricsResponse.MetricsScope(AdminMetricsResponse.MetricsScopeType.GLOBAL, null, null),
                OBSERVED_AT,
                MetricsWindow.FIVE_MINUTES,
                new AdminMetricsResponse.ConsistencyResponse(
                        ConsistencyPhase.FINAL,
                        Verdict.PASS,
                        Severity.NONE,
                        zeroGap,
                        zeroGap,
                        zeroGap,
                        zeroGap,
                        zeroGap),
                new AdminMetricsResponse.TrafficMetrics(null, null, null, null, null),
                new AdminMetricsResponse.LatencyMetrics(null, null, null),
                new AdminMetricsResponse.DependencyMetrics(null, null, null),
                null,
                List.of());

        assertThat(objectMapper.writeValueAsString(response))
                .contains("\"phase\":\"FINAL\"")
                .contains("\"verdict\":\"PASS\"")
                .contains("\"severity\":\"NONE\"")
                .contains("\"overIssued\":{\"value\":0")
                .contains("\"luaGap\":{\"value\":0")
                .contains("\"activeDbGap\":{\"value\":0")
                .contains("\"dbCounterGap\":{\"value\":0")
                .contains("\"persistGap\":{\"value\":0");
    }

    /** 실행 중 Benchmark의 nullable verdict와 원천별 관측값이 그대로 유지되는지 검증합니다. */
    @Test
    void benchmarkSerializesObservedValuesAndNullableVerdict() throws Exception {
        BenchmarkListResponse response = new BenchmarkListResponse(
                List.of(new BenchmarkListResponse.BenchmarkSummary(
                        9L,
                        EngineVersion.V1,
                        "baseline",
                        OBSERVED_AT,
                        null,
                        BenchmarkRunState.RUNNING,
                        (VerdictType) null,
                        new ObservedValue<>(10.5, SourceStatus.VALID, OBSERVED_AT),
                        new ObservedValue<>(null, SourceStatus.PENDING, null),
                        new ObservedValue<>(0.0, SourceStatus.VALID, OBSERVED_AT),
                        new ObservedValue<>(0L, SourceStatus.VALID, OBSERVED_AT)
                )),
                null,
                false
        );

        assertThat(objectMapper.writeValueAsString(response))
                .contains("\"benchmarkRunId\":9")
                .contains("\"verdict\":null")
                .contains("\"issueAttemptRps\":{\"value\":10.5,\"state\":\"VALID\"")
                .contains("\"overIssuedCount\":{\"value\":0,\"state\":\"VALID\"");
    }
}
