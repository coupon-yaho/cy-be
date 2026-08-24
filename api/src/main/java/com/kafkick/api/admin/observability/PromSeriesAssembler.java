package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.Meta;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesEntry;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesPoint;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScope;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScopeType;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * Prometheus range 결과를 {@link AdminMetricsSeriesResponse} 한 장으로 조립합니다.
 *
 * <p><b>계열마다 질의를 따로 보냅니다.</b> 한 질의에 여러 계열을 담으면 계열 하나가 해석되지 않을
 * 때 그래프 전체가 사라집니다 — 화면 요구는 그 반대입니다. 왕복이 계열 수만큼 늘지만 이 경로는
 * 전용 예산에 5~10초 주기라 감당 범위이고, {@link PromMetricsAssembler} 가 instant 질의 다섯 개를
 * 각각 격리하는 구조를 그대로 따릅니다.</p>
 *
 * <p><b>예산을 넘긴 계열은 보내지 않습니다.</b> 순서가 우선순위입니다 — 잘리는 것은 항상 뒤입니다.
 * 이 예산은 {@link PrometheusSeriesProperties} 가 정하며 {@code /metrics} 의 1초 폴링 예산과
 * 무관합니다. 이 경로가 아무리 느려도 {@code /metrics} 응답 시간은 바뀌지 않습니다.</p>
 */
public class PromSeriesAssembler {

    private static final Logger log = LoggerFactory.getLogger(PromSeriesAssembler.class);

    private static final String TAG_URI_GROUP = "uri_group";
    private static final String TAG_RESULT = "result";
    private static final String TAG_QUANTILE = "quantile";

    /**
     * 실패 비율의 분자가 되는 결과 분류의 라벨 정규식입니다.
     *
     * <p>정의는 {@link ResultClass#systemFailures()} 하나뿐입니다 — 여기에 옮겨 적으면 분류가
     * 늘어날 때 스냅샷과 추세선의 숫자가 조용히 갈립니다.</p>
     */
    private static final String FAILURE_RESULTS =
            ResultClass.promLabelAlternation(ResultClass.systemFailures());

    private final PromRangeQuery rangeQuery;
    private final TimeProvider timeProvider;
    private final PrometheusSeriesProperties properties;

    public PromSeriesAssembler(
            PromRangeQuery rangeQuery, TimeProvider timeProvider, PrometheusSeriesProperties properties) {
        this.rangeQuery = Objects.requireNonNull(rangeQuery, "rangeQuery");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    /**
     * 요청 창의 시계열 네 종을 조립합니다.
     *
     * @param window 조회 구간이자 rate 집계 창을 정하는 기준
     * @return 계열마다 상태가 붙은 시계열 응답
     */
    public AdminMetricsSeriesResponse assemble(MetricsWindow window) {
        Objects.requireNonNull(window, "window");
        Duration step = properties.step();
        Instant end = timeProvider.instant().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        Instant start = end.minus(window.duration());

        long startedAtNanos = System.nanoTime();
        Deadline deadline = new Deadline(startedAtNanos + properties.totalBudget().toNanos());

        List<SeriesEntry> series = new ArrayList<>();
        // 순서가 우선순위다. 예산이 모자라면 뒤가 잘리므로 합격 판정을 가르는 정합성을 먼저 받는다
        // — /metrics 가 정합성을 KPI 첫 칸에 두는 것과 같은 이유다.
        series.addAll(collect(SeriesKey.CONSISTENCY_GAP, consistencyGapQuery(),
                start, end, step, deadline));
        series.addAll(collect(SeriesKey.THROUGHPUT, throughputQuery(step),
                start, end, step, deadline));
        series.addAll(collect(SeriesKey.FAILURE_RATE, failureRateQuery(step),
                start, end, step, deadline));
        // 마지막이다. 지연 백분위는 창과 무관하게 각 점이 독립이라 잘려도 다른 값의 해석을
        // 바꾸지 않는 유일한 계열이다.
        series.addAll(collect(SeriesKey.LATENCY_P99, latencyQuery(),
                start, end, step, deadline));

        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAtNanos).toMillis();
        return new AdminMetricsSeriesResponse(
                new Meta(Meta.SCHEMA_VERSION, start, end, step.toSeconds(), elapsedMillis),
                // 이 경로는 아직 범위 셀렉터를 넣지 않는다(OBS-34). GLOBAL 을 명시해 화면이
                // 좁혀진 값으로 오해하지 않게 한다.
                new MetricsScope(MetricsScopeType.GLOBAL, null, null),
                window,
                series);
    }

    // ── 질의 ────────────────────────────────────────────────────────────────────

    /**
     * 발급 경로 전체 처리량입니다.
     *
     * <p>집계 창은 {@code step} 과 같습니다 — 평가 간격보다 넓으면 이웃한 점이 같은 표본을 겹쳐
     * 세고, 좁으면 표본이 없는 점이 생깁니다.</p>
     */
    private static String throughputQuery(Duration step) {
        return "sum(rate(" + MetricAggregation.HTTP_RESULT_TOTAL
                + "{" + TAG_URI_GROUP + "=\"" + UriGroup.ISSUE.tagValue() + "\"}"
                + "[" + step.toSeconds() + "s]))";
    }

    /**
     * 발급 시도 대비 실패 비율입니다.
     *
     * <p>분자와 분모를 <b>한 질의 안에서</b> 나눕니다. 따로 부르면 두 응답이 다른 시점을 가리켜
     * 비율이 1 을 넘거나 음수가 되는 점이 생깁니다 — {@link PromMetricsAssembler} 가 같은 표본을
     * 접어 쓰는 것과 같은 이유입니다.</p>
     */
    private static String failureRateQuery(Duration step) {
        String selector = "{" + TAG_URI_GROUP + "=\"" + UriGroup.ISSUE.tagValue() + "\"";
        String range = "[" + step.toSeconds() + "s]";
        return "sum(rate(" + MetricAggregation.HTTP_RESULT_TOTAL
                + selector + "," + TAG_RESULT + "=~\"" + FAILURE_RESULTS + "\"}" + range + "))"
                + " / sum(rate(" + MetricAggregation.HTTP_RESULT_TOTAL + selector + "}" + range + "))";
    }

    /**
     * 발급 경로 응답시간 p99(ms)입니다.
     *
     * <p>인스턴스별로 계산된 값이라 합칠 수 없어 최댓값을 씁니다({@link MetricAggregation#MAX}).
     * 화면은 '인스턴스 최댓값' 표식을 붙여야 합니다.</p>
     *
     * <p>⚠️ 이 질의에는 창이 없습니다. 백분위의 관측 창은 Micrometer expiry 가 정하고 PromQL 로는
     * 바꿀 수 없습니다 — 창은 rate 계열에만 걸립니다.</p>
     */
    private static String latencyQuery() {
        return "max(" + MetricAggregation.HTTP_LATENCY_SECONDS
                + "{" + TAG_QUANTILE + "=\"0.99\"," + TAG_URI_GROUP + "=\"" + UriGroup.ISSUE.tagValue()
                + "\"}) * 1000";
    }

    /**
     * 정합성 gap 입니다.
     *
     * <p>집계하지 않습니다. gap 은 종류({@code type} 라벨)로만 나뉘고 원천이 batch 한 곳뿐이라
     * ({@link MetricAggregation#SINGLE}) 합치면 서로 다른 종류가 한 숫자로 뭉개집니다.</p>
     */
    private static String consistencyGapQuery() {
        return MetricAggregation.CONSISTENCY_GAP;
    }

    // ── 실행 ────────────────────────────────────────────────────────────────────

    /**
     * 계열 하나를 조회합니다. 실패는 이 계열 안에 가둡니다.
     */
    private List<SeriesEntry> collect(
            SeriesKey key, String promQl, Instant start, Instant end, Duration step,
            Deadline deadline) {
        if (!deadline.allows()) {
            log.warn("series 예산을 넘겨 계열을 보내지 않고 UNAVAILABLE 로 내려보냅니다: {}", key);
            return List.of(SeriesEntry.unavailable(key));
        }
        List<PromRangeSeries> raw;
        try {
            raw = rangeQuery.query(promQl, start, end, step);
        } catch (PromQueryException | IllegalArgumentException failure) {
            // 500 으로 올리지 않는다. 이 계열만 UNAVAILABLE 로 나가고 나머지는 그려진다.
            // 스택트레이스는 DEBUG 에만 싣는다 — 원천이 죽은 동안 매 폴링마다 쌓인다.
            log.warn("Prometheus 구간 질의 실패로 계열을 UNAVAILABLE 로 내려보냅니다: {} ({})",
                    key, failure.getMessage());
            log.debug("Prometheus 구간 질의 실패 상세: {}", promQl, failure);
            return List.of(SeriesEntry.unavailable(key));
        }
        if (raw.isEmpty()) {
            // 원천이 죽은 것이 아니라 아직 표본이 없는 것이다. 둘은 운영자가 취할 행동이 반대다.
            return List.of(new SeriesEntry(key, Map.of(), SourceStatus.PENDING, List.of()));
        }
        List<SeriesEntry> entries = new ArrayList<>();
        for (PromRangeSeries series : raw) {
            entries.add(new SeriesEntry(key, displayLabels(series), SourceStatus.VALID, points(series)));
        }
        return List.copyOf(entries);
    }

    /**
     * 남은 시간을 세는 조립 1회분 상태입니다.
     *
     * <p><b>첫 계열은 예산과 무관하게 보냅니다.</b> 예산을 아무리 짧게 잡아도 응답이 통째로 비면
     * 화면에 아무것도 안 남습니다.</p>
     *
     * <p>조립 하나에만 살고 스레드를 넘지 않습니다 — 계열 조회를 병렬로 바꾸려면 이 상태부터
     * 다시 설계해야 합니다.</p>
     */
    private static final class Deadline {

        private final long expiresAtNanos;
        private boolean anyIssued;

        private Deadline(long expiresAtNanos) {
            this.expiresAtNanos = expiresAtNanos;
        }

        boolean allows() {
            if (!anyIssued) {
                anyIssued = true;
                return true;
            }
            return System.nanoTime() - expiresAtNanos < 0;
        }
    }

    /** Prometheus 의 {@code __name__} 은 계열 종류가 이미 담고 있으므로 라벨에서 뺍니다. */
    private static Map<String, String> displayLabels(PromRangeSeries series) {
        Map<String, String> labels = new LinkedHashMap<>(series.labels());
        labels.remove("__name__");
        return Map.copyOf(labels);
    }

    /** NaN·무한대는 숫자 관측이 아니므로 null 로 내보냅니다. 0 으로 바꾸면 화면이 거짓을 그립니다. */
    private static List<SeriesPoint> points(PromRangeSeries series) {
        List<SeriesPoint> points = new ArrayList<>(series.points().size());
        for (PromRangePoint point : series.points()) {
            points.add(new SeriesPoint(point.observedAt(),
                    point.hasNumericValue() ? point.value() : null));
        }
        return List.copyOf(points);
    }
}
