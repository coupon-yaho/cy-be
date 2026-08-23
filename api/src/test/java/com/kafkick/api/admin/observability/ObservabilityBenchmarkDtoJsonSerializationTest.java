package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.benchmark.dto.BenchmarkListResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** 관측 지표와 Benchmark 목록 DTO의 독립 상태·nullable 판정 JSON 구조를 검증합니다. */
@AdminJsonTest
class ObservabilityBenchmarkDtoJsonSerializationTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-16T00:00:00Z");

    private final ObjectMapper objectMapper;

    @Autowired
    ObservabilityBenchmarkDtoJsonSerializationTest(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** 범위·정합성 단계와 각 트래픽 원천 상태가 독립적으로 직렬화되는지 검증합니다. */
    @Test
    void metricsSerializesScopeConsistencyAndIndependentTrafficStates() throws Exception {
        ObservedValue<Double> pendingRate = new ObservedValue<>(null, SourceStatus.PENDING, null);
        AdminMetricsResponse response = new AdminMetricsResponse(
                AdminMetricsResponse.Meta.of(OBSERVED_AT, MetricsWindow.ONE_MINUTE, 0L),
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
                .contains("\"window\":\"ONE_MINUTE\"")
                .contains("\"phase\":\"LIVE\"")
                .doesNotContain("\"verdict\":")
                .contains("\"issueAttemptRps\":{\"state\":\"PENDING\"")
                .contains("\"circuitBreakers\":[]")
                .contains("\"meta\":{\"schemaVersion\":1");
    }

    /**
     * meta의 여섯 필드가 모두 직렬화되고, 창 경계가 집계 구간만큼 벌어지며,
     * sources가 null이 아니라 빈 객체로 남는지 검증합니다.
     */
    @Test
    void metaSerializesSchemaVersionWindowBoundsAndEmptySources() throws Exception {
        AdminMetricsResponse.Meta meta =
                AdminMetricsResponse.Meta.of(OBSERVED_AT, MetricsWindow.FIFTEEN_MINUTES, 217L);

        assertThat(meta.windowStart()).isEqualTo(OBSERVED_AT.minus(Duration.ofMinutes(15)));
        assertThat(meta.windowEnd()).isEqualTo(OBSERVED_AT);
        assertThat(objectMapper.writeValueAsString(meta))
                .contains("\"schemaVersion\":1")
                .contains("\"snapshotAt\":\"2026-08-16T00:00:00Z\"")
                .contains("\"windowStart\":\"2026-08-15T23:45:00Z\"")
                .contains("\"windowEnd\":\"2026-08-16T00:00:00Z\"")
                .contains("\"collectionDurationMs\":217")
                .contains("\"sources\":{}");
    }

    /** 집계 구간이 바뀌면 창 시작 시각도 함께 바뀌는지 검증합니다. */
    @Test
    void metaWindowStartFollowsWindow() {
        assertThat(AdminMetricsResponse.Meta.of(OBSERVED_AT, MetricsWindow.ONE_MINUTE, 0L).windowStart())
                .isEqualTo(OBSERVED_AT.minus(Duration.ofMinutes(1)))
                .isNotEqualTo(AdminMetricsResponse.Meta
                        .of(OBSERVED_AT, MetricsWindow.FIFTEEN_MINUTES, 0L).windowStart());
    }

    /** 최종 판정과 네 ConsistencyGapType의 관리자 공개 필드 매핑을 JSON 이름으로 고정합니다. */
    @Test
    void finalMetricsSerializesCanonicalVerdictAndMappedGapFields() throws Exception {
        ObservedValue<Long> zeroGap = new ObservedValue<>(0L, SourceStatus.VALID, OBSERVED_AT);
        AdminMetricsResponse response = new AdminMetricsResponse(
                AdminMetricsResponse.Meta.of(OBSERVED_AT, MetricsWindow.FIVE_MINUTES, 0L),
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

    /** 실행 중 Benchmark의 미확정 verdict는 생략하고 원천별 관측값은 유지합니다. */
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
                .doesNotContain("\"verdict\":")
                .contains("\"issueAttemptRps\":{\"value\":10.5,\"state\":\"VALID\"")
                .contains("\"overIssuedCount\":{\"value\":0,\"state\":\"VALID\"");
    }
}
