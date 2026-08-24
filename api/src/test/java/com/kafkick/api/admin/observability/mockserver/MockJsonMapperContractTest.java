package com.kafkick.api.admin.observability.mockserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.CircuitBreakerState;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.CircuitBreakerSummary;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ConsistencyResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.DependencyMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.DependencySnapshot;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClass;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClassKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyPercentiles;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.Meta;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScope;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScopeType;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.PersistenceLagSummary;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TopReason;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficMetrics;
import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

@AdminJsonTest
class MockJsonMapperContractTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-23T00:00:00Z");

    private final ObjectMapper springMapper;

    @Autowired
    MockJsonMapperContractTest(ObjectMapper springMapper) {
        this.springMapper = springMapper;
    }

    @Test
    void mockMapperMatchesSpringMapperForAllScopeTypesAndSourceStatuses() throws Exception {
        AdminMetricsResponse comprehensive = comprehensiveResponse();
        List<AdminMetricsResponse> responses = List.of(
                comprehensive,
                withScope(comprehensive, new MetricsScope(MetricsScopeType.COUPON, 7L, null)),
                withScope(comprehensive, new MetricsScope(MetricsScopeType.BENCHMARK_RUN, null, 9L)));

        String springOut = springMapper.writeValueAsString(responses);
        String mockOut = MockJsonMapper.create().writeValueAsString(responses);

        assertThat(mockOut).isEqualTo(springOut);
    }

    private static AdminMetricsResponse comprehensiveResponse() {
        ObservedValue<Double> valid = observed(11.0, SourceStatus.VALID);
        ObservedValue<Double> warming = observed(7.0, SourceStatus.WARMING_UP);
        ObservedValue<Double> stale = observed(5.0, SourceStatus.STALE);
        ObservedValue<Double> noTraffic = observed(0.0, SourceStatus.NO_TRAFFIC);
        ObservedValue<Long> unavailable = absent(SourceStatus.UNAVAILABLE);
        ObservedValue<Long> notApplicable = absent(SourceStatus.N_A);
        ObservedValue<Long> pending = absent(SourceStatus.PENDING);

        return new AdminMetricsResponse(
                Meta.of(OBSERVED_AT, MetricsWindow.ONE_MINUTE, 217L),
                new MetricsScope(MetricsScopeType.GLOBAL, null, null),
                OBSERVED_AT,
                MetricsWindow.ONE_MINUTE,
                new ConsistencyResponse(
                        ConsistencyPhase.LIVE,
                        null,
                        Severity.WARN,
                        pending,
                        unavailable,
                        notApplicable,
                        observed(3L, SourceStatus.VALID),
                        observed(2L, SourceStatus.STALE)),
                new TrafficMetrics(valid, warming, stale, noTraffic, absent(SourceStatus.PENDING)),
                new LatencyMetrics(
                        observed(new LatencyPercentiles(10, 20, 30), SourceStatus.VALID),
                        absent(SourceStatus.UNAVAILABLE),
                        absent(SourceStatus.N_A)),
                new DependencyMetrics(
                        observed(new DependencySnapshot(4, 8, 0.01), SourceStatus.WARMING_UP),
                        absent(SourceStatus.PENDING),
                        absent(SourceStatus.UNAVAILABLE)),
                observed(new PersistenceLagSummary(10, 4, 3, 5, 2, null), SourceStatus.VALID),
                List.of(
                        new CircuitBreakerSummary("redis", CircuitBreakerState.CLOSED, null),
                        new CircuitBreakerSummary("kafka", CircuitBreakerState.OPEN, OBSERVED_AT)),
                new ErrorMetrics(
                        TrafficKey.ISSUE_ATTEMPT_RPS,
                        List.of(
                                new ErrorClass(ErrorClassKey.DEPENDENCY_FAILURE, "의존성 실패",
                                        "httpStatus >= 500 && dependency != NONE", false, valid),
                                new ErrorClass(ErrorClassKey.CLIENT_INVALID, "클라이언트 요청 오류",
                                        "4xx 중 403·409 를 뺀 나머지", true, absent(SourceStatus.N_A))),
                        observed(List.of(new TopReason(ReasonCode.INTERNAL_ERROR, 1.5)),
                                SourceStatus.VALID)),
                AdminMetricsResponse.SaturationPanel.draft());
    }

    private static AdminMetricsResponse withScope(AdminMetricsResponse response, MetricsScope scope) {
        return new AdminMetricsResponse(
                response.meta(),
                scope,
                response.snapshotAt(),
                response.window(),
                new ConsistencyResponse(
                        ConsistencyPhase.FINAL,
                        Verdict.PASS,
                        Severity.NONE,
                        response.consistency().overIssued(),
                        response.consistency().luaGap(),
                        response.consistency().activeDbGap(),
                        response.consistency().dbCounterGap(),
                        response.consistency().persistGap()),
                response.traffic(),
                response.latency(),
                response.dependencies(),
                response.persistence(),
                List.of(),
                response.errors(),
                response.saturation());
    }

    private static <T> ObservedValue<T> observed(T value, SourceStatus status) {
        return new ObservedValue<>(value, status, OBSERVED_AT);
    }

    private static <T> ObservedValue<T> absent(SourceStatus status) {
        return new ObservedValue<>(null, status, null);
    }
}
