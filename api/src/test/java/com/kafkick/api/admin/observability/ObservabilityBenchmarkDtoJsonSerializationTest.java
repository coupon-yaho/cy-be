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
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClass;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClassKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TopReason;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficKey;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.ReasonCode;
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
                new AdminMetricsResponse.LatencyMetrics(null, null, null, List.of()),
                new AdminMetricsResponse.DependencyMetrics(null, null, null),
                null,
                List.of(),
                AdminMetricsResponse.ErrorMetrics.draft(),
                AdminMetricsResponse.SaturationPanel.draft()
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
     * 실패 분류 키가 <b>화면 계약의 camelCase 그대로</b> 나가는지 봅니다.
     *
     * <p>이 단언이 없으면 {@code @JsonValue} 가 빠져도 아무것도 안 깨집니다 — 계약 테스트는
     * {@code jsonValue()} 메서드만 부르고, 목 매퍼 대조 테스트는 두 매퍼의 출력이 같은지만
     * 보므로 <b>둘 다 대문자로 나가도 통과합니다.</b> 실제 JSON 문자열을 보는 곳은 여기뿐입니다.</p>
     */
    @Test
    void errorsSerializesScreenContractKeysAsCamelCase() throws Exception {
        ObservedValue<Double> rate = new ObservedValue<>(1.5, SourceStatus.VALID, OBSERVED_AT);
        ErrorMetrics errors = new ErrorMetrics(
                TrafficKey.ISSUE_ATTEMPT_RPS,
                List.of(
                        new ErrorClass(ErrorClassKey.DEPENDENCY_FAILURE, "의존성 실패",
                                "httpStatus >= 500 && dependency != NONE", false, rate),
                        new ErrorClass(ErrorClassKey.CLIENT_INVALID, "클라이언트 요청 오류",
                                "4xx 중 403·409 를 뺀 나머지", true,
                                new ObservedValue<>(null, SourceStatus.N_A, null))),
                new ObservedValue<>(List.of(new TopReason(ReasonCode.INTERNAL_ERROR, 0.25)),
                        SourceStatus.VALID, OBSERVED_AT));

        String json = objectMapper.writeValueAsString(errors);

        assertThat(json)
                .contains("\"denominator\":\"issueAttemptRps\"")
                .contains("\"key\":\"dependencyFailure\"")
                .contains("\"key\":\"clientInvalid\"")
                .contains("\"excludedFromNumerator\":true")
                // 사유 코드는 원천 라벨 값이라 대문자 그대로다. 키와 표기 규약이 다르다.
                .contains("\"reasonCode\":\"INTERNAL_ERROR\"")
                .contains("\"rps\":0.25")
                // 상수 이름이 그대로 나가면 화면이 못 읽는다.
                .doesNotContain("ISSUE_ATTEMPT_RPS")
                .doesNotContain("DEPENDENCY_FAILURE")
                .doesNotContain("CLIENT_INVALID")
                // 원천이 없는 분류는 키 자체가 없어야 한다.
                .doesNotContain("clientObservedFailure");
    }

    /**
     * saturation 블록의 <b>키 이름이 화면 계약</b>입니다(cy-fe {@code types.ts:571-601}). 이름이
     * 어긋나면 예외 없이 화면만 빕니다 — 프론트는 {@code saturation?} 을 optional 로 두고 있어
     * 블록이 통째로 없는 것과 이름이 틀린 것을 구분하지 못합니다.
     */
    @Test
    void saturationSerializesContractFieldNames() throws Exception {
        String json = objectMapper.writeValueAsString(
                AdminMetricsResponse.SaturationPanel.draft());

        assertThat(json)
                .contains("\"resources\":[")
                .contains("\"name\":\"Hikari\"")
                .contains("\"warnAt\":80")
                .contains("\"warnAt\":75")
                .contains("\"utilization\":{\"state\":\"PENDING\"")
                .contains("\"inFlight\":{")
                .contains("\"globalSum\":")
                .contains("\"instanceMax\":")
                .contains("\"activeInstances\":0")
                .contains("\"admitThreshold\":0")
                .contains("\"releaseThreshold\":0")
                .contains("\"zone\":\"Admission\"")
                .contains("\"zone\":\"Persistence\"")
                .contains("\"zone\":\"Telemetry\"")
                .contains("\"thresholds\":{\"warn\":60,\"high\":75,\"critical\":85}");
        // 값이 없는 자리를 0 으로 채우면 화면이 '여유' 를 그린다.
        assertThat(json).doesNotContain("\"value\":0");
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
                new AdminMetricsResponse.LatencyMetrics(null, null, null, List.of()),
                new AdminMetricsResponse.DependencyMetrics(null, null, null),
                null,
                List.of(),
                AdminMetricsResponse.ErrorMetrics.draft(),
                AdminMetricsResponse.SaturationPanel.draft());

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
