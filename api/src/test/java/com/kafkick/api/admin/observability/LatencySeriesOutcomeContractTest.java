package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.observation.http.HttpMetrics.LatencyOutcome;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;
import com.kafkick.core.support.TimeProvider;

/**
 * 지연 <b>시계열</b>이 어느 outcome 축을 보는지 고정합니다.
 *
 * <p><b>이 테스트가 없으면 축을 늘리는 순간 값의 뜻이 바뀌는데 아무것도 안 깨집니다.</b>
 * 두 질의 모두 {@code outcome} 셀렉터 없이 {@code max()} · {@code topk()} 로 집계하고 있어서,
 * OBS-31 이 Timer 를 넷으로 가르자 <b>가장 느린 축</b>을 집게 됐습니다. 프로브 실측으로 같은
 * 부하에서 <b>243.3ms → 2952.8ms(12.1배)</b> 가 나왔습니다 — 성공 경로는 그대로인데 화면 숫자만
 * 튀는 상태입니다.</p>
 *
 * <p><b>{@code PromQueryClient} 쪽은 되돌릴 수 없습니다.</b> 그 질의 결과는
 * {@link Metric#LATENCY_P99} 로 {@code run_timeseries} 에 <b>영구 적재</b>되고 완료 회차의
 * archive 는 불변입니다. 회차 간 비교가 이 시점에서 끊기면 소급 정정이 안 됩니다.</p>
 *
 * <p>축을 실제로 나누는 것은 OBS-46 입니다. 여기서는 <b>기존 계열이 성공 경로를 가리킨다</b>는
 * 것만 못박습니다.</p>
 */
class LatencySeriesOutcomeContractTest {

    private static final TimeProvider FIXED_TIME =
            new TimeProvider(Clock.fixed(Instant.parse("2026-08-25T00:00:00Z"), ZoneOffset.UTC));

    /** 계측이 붙이는 라벨 값을 그대로 쓴다. 손으로 적으면 이 테스트가 오타를 같이 물려받는다. */
    private static final String SUCCESS = LatencyOutcome.SUCCESS.tagValue();

    @Test
    @DisplayName("추세선 지연 질의는 성공 축만 본다")
    void seriesLatencyQuerySelectsSuccessOnly() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        new PromSeriesAssembler(source, FIXED_TIME, PrometheusSeriesProperties.defaults())
                .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, null, null));

        String latencyQuery = source.issued().stream()
                .filter(promQl -> promQl.contains(MetricAggregation.HTTP_LATENCY_SECONDS))
                .findFirst()
                .orElseThrow(() -> new AssertionError("지연 질의가 나가지 않았습니다."));

        assertThat(latencyQuery)
                .as("outcome 셀렉터가 없으면 실패 축이 섞여 값의 뜻이 조용히 바뀝니다")
                .contains("outcome=\"" + SUCCESS + "\"");
        for (LatencyOutcome other : LatencyOutcome.values()) {
            if (other == LatencyOutcome.SUCCESS) {
                continue;
            }
            assertThat(latencyQuery)
                    .as("%s 축이 추세선에 섞여 있습니다", other)
                    .doesNotContain(other.tagValue());
        }
    }

    /**
     * archive 경로입니다. 이쪽이 틀리면 화면이 아니라 <b>완료 회차 데이터</b>가 오염됩니다.
     */
    @Test
    @DisplayName("archive 지연 질의도 성공 축만 본다")
    void archiveLatencyQuerySelectsSuccessOnly() {
        String archiveQuery = PromQueryClient.rangeQueryFor(Metric.LATENCY_P99);

        assertThat(archiveQuery)
                .as("run_timeseries 는 DONE 불변이라 뜻이 바뀌면 소급 정정이 안 됩니다")
                .contains("outcome=\"" + SUCCESS + "\"");
        for (LatencyOutcome other : LatencyOutcome.values()) {
            if (other == LatencyOutcome.SUCCESS) {
                continue;
            }
            assertThat(archiveQuery)
                    .as("%s 축이 archive 에 섞여 있습니다", other)
                    .doesNotContain(other.tagValue());
        }
    }
}
