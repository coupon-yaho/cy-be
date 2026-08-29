package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

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
 * <p>OBS-46 이 축을 실제로 나눴습니다. 이제 이 테스트가 못박는 것은 둘입니다 — <b>기존 계열이
 * 여전히 성공 경로를 가리킨다</b>는 것(과거 회차와 비교 축을 잇기 위해)과, <b>새 축이 성공에
 * 섞이지 않는다</b>는 것입니다.</p>
 */
class LatencySeriesOutcomeContractTest {

    private static final TimeProvider FIXED_TIME =
            new TimeProvider(Clock.fixed(Instant.parse("2026-08-25T00:00:00Z"), ZoneOffset.UTC));

    /** 계측이 붙이는 라벨 값을 그대로 쓴다. 손으로 적으면 이 테스트가 오타를 같이 물려받는다. */
    private static final String SUCCESS = LatencyOutcome.SUCCESS.tagValue();

    private static final String SYSTEM_FAILURE = LatencyOutcome.SYSTEM_FAILURE.tagValue();

    @Test
    @DisplayName("추세선 지연 질의는 축마다 하나씩, 셀렉터가 서로 겹치지 않는다")
    void seriesLatencyQueriesAreSplitByOutcome() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        new PromSeriesAssembler(source, FIXED_TIME, PrometheusSeriesProperties.defaults())
                .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, null, null));

        List<String> latencyQueries = source.issued().stream()
                .filter(promQl -> promQl.contains(MetricAggregation.HTTP_LATENCY_SECONDS))
                .toList();

        // 축마다 정확히 하나다. 하나면 축이 안 갈린 것이고, 셋이면 정책 거절이 섞여 들어온
        // 것이다 — 후자는 재고 소진 폭주 때 실패 지연을 1ms 아래로 희석한다.
        assertThat(latencyQueries)
                .as("지연 계열은 성공·시스템 실패 두 축이다")
                .hasSize(2);
        assertLooksAtOnly(latencyQueries.get(0), SUCCESS);
        assertLooksAtOnly(latencyQueries.get(1), SYSTEM_FAILURE);
    }

    /**
     * 축 하나만 보는지 확인합니다.
     *
     * <p>셀렉터가 없으면 {@code max()} 가 가장 느린 축을 집고, 둘이 함께 있으면 같은 계열이
     * 상황에 따라 다른 것을 가리킵니다.</p>
     */
    private static void assertLooksAtOnly(String promQl, String outcome) {
        assertThat(promQl)
                .as("outcome 셀렉터가 없으면 다른 축이 섞여 값의 뜻이 조용히 바뀝니다")
                .contains("outcome=\"" + outcome + "\"");
        for (LatencyOutcome other : LatencyOutcome.values()) {
            if (other.tagValue().equals(outcome)) {
                continue;
            }
            assertThat(promQl)
                    .as("%s 축이 %s 축 질의에 섞여 있습니다", other, outcome)
                    .doesNotContain(other.tagValue());
        }
    }

    /**
     * archive 경로입니다. 이쪽이 틀리면 화면이 아니라 <b>완료 회차 데이터</b>가 오염됩니다.
     */
    /**
     * 빈 축의 이유를 가르는 질의입니다. 이 답이 회차 행의 상태가 되고 그 행은 불변입니다.
     *
     * <p><b>백분위로 물으면 순환입니다.</b> 백분위 계열은 표본이 있어야 나오므로 "비었냐" 를
     * 두 번 묻는 것과 같습니다. {@code _count} 는 기동 시점에 축까지 전부 등록되므로 0건이어도
     * 나옵니다 — 그래서 "계측돼 있었나" 를 물을 수 있습니다.</p>
     *
     * <p><b>자기 축을 걸어야 합니다.</b> 안 걸면 축 하나만 계측 안 된 배포가 "0건" 으로
     * 기록됩니다 — 실측으로 그 상태를 확인했습니다.</p>
     */
    @Test
    @DisplayName("원천 존재 질의는 자기 축의 _count 를 본다")
    void sourceProbeAsksWhetherItsOwnAxisWasInstrumented() {
        assertProbeAsksAbout(Metric.LATENCY_P99, LatencyOutcome.SUCCESS);
        assertProbeAsksAbout(Metric.LATENCY_P99_SYSTEM_FAILURE, LatencyOutcome.SYSTEM_FAILURE);
    }

    private static void assertProbeAsksAbout(Metric metric, LatencyOutcome axis) {
        String probe = PromQueryClient.sourceProbeFor(metric, 65L);

        assertThat(probe)
                .as("미터 이름을 옮겨 적으면 archive 만 조용히 빈 결과가 됩니다")
                .contains(MetricAggregation.HTTP_LATENCY_SECONDS + "_count");
        assertThat(probe)
                .as("백분위로 물으면 '비었냐' 를 두 번 묻는 순환입니다")
                .doesNotContain("quantile");
        assertThat(probe)
                .as("%s 는 자기 축을 걸어야 계측 부재와 0 건을 가릅니다", metric)
                .contains("outcome=\"" + axis.tagValue() + "\"");
        for (LatencyOutcome other : LatencyOutcome.values()) {
            if (other == axis) {
                continue;
            }
            assertThat(probe)
                    .as("%s 의 원천 존재 질의에 %s 축이 섞여 있습니다", metric, other)
                    .doesNotContain(other.tagValue());
        }
    }

    /**
     * 원천 존재 질의가 회차 구간을 <b>남김없이</b> 덮는지 봅니다.
     *
     * <p>회차 시각은 {@code datetime(6)} 이라 소수초가 있습니다. {@code getEpochSecond()} 끼리
     * 빼면 그게 잘려 구간이 최대 1초 짧아지고, 앞쪽에만 표본이 있던 축을 못 보고 "재지
     * 못했다" 로 <b>영구히</b> 적습니다. archive 는 불변이라 소급 정정이 안 됩니다.</p>
     */
    @Test
    @DisplayName("원천 존재 질의 구간은 회차 구간을 남김없이 덮는다")
    void sourceProbeWindowCoversTheWholeRun() {
        assertCovers("2026-08-25T00:00:00.100Z", "2026-08-25T00:00:10.900Z", 11L);
        assertCovers("2026-08-25T00:00:00.900Z", "2026-08-25T00:00:10.100Z", 10L);
        assertCovers("2026-08-25T00:00:00Z", "2026-08-25T00:01:05Z", 65L);

        // 길이가 0 이거나 뒤집힌 구간. PromQL 구간은 양수여야 한다.
        assertThat(PromQueryClient.probeWindowSeconds(
                Instant.parse("2026-08-25T00:00:00Z"), Instant.parse("2026-08-25T00:00:00Z")))
                .isEqualTo(1L);
        assertThat(PromQueryClient.probeWindowSeconds(
                Instant.parse("2026-08-25T00:00:10Z"), Instant.parse("2026-08-25T00:00:00Z")))
                .isEqualTo(1L);
    }

    private static void assertCovers(String start, String end, long expectedSeconds) {
        Instant from = Instant.parse(start);
        Instant to = Instant.parse(end);
        long window = PromQueryClient.probeWindowSeconds(from, to);

        assertThat(window)
                .as("%s ~ %s", start, end)
                .isEqualTo(expectedSeconds);
        assertThat(from.plusSeconds(window))
                .as("질의 구간이 회차 끝에 못 미치면 앞쪽 표본을 못 본다")
                .isAfterOrEqualTo(to);
    }

    @Test
    @DisplayName("archive 지연 질의도 축마다 하나씩, 성공 축의 뜻은 그대로다")
    void archiveLatencyQueriesAreSplitByOutcome() {
        assertLooksAtOnly(PromQueryClient.rangeQueryFor(Metric.LATENCY_P99), SUCCESS);
        assertLooksAtOnly(
                PromQueryClient.rangeQueryFor(Metric.LATENCY_P99_SYSTEM_FAILURE), SYSTEM_FAILURE);
    }
}
