package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.Marker;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.Meta;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesEntry;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesPoint;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScope;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScopeType;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.DomainMeterNames;
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
    private static final String TAG_JOB = "job";
    private static final String JOB_API = "api";
    private static final String TAG_OUTCOME = OverviewPrometheusContract.OUTCOME;
    private static final String OUTCOME_SUCCESS = OverviewPrometheusContract.SUCCESS;

    /**
     * 실패 비율의 분자가 되는 결과 분류의 라벨 정규식입니다.
     *
     * <p>정의는 {@link ResultClass#systemFailures()} 하나뿐입니다 — 여기에 옮겨 적으면 분류가
     * 늘어날 때 스냅샷과 추세선의 숫자가 조용히 갈립니다.</p>
     */
    private static final String FAILURE_RESULTS =
            ResultClass.promLabelAlternation(ResultClass.systemFailures());

    /**
     * 실패 분류 계열이 펴는 결과 분류의 라벨 정규식입니다.
     *
     * <p>{@link #FAILURE_RESULTS} 와 <b>다른 집합</b>입니다 — 저쪽은 실패율의 분자가 되는 두
     * 분류뿐이고 이쪽은 성공이 아닌 <b>네 분류 전부</b>입니다. 목록을 옮겨 적지 않고
     * {@link ResultClass#isSuccess()} 에서 만듭니다. 분류가 늘면 여기도 함께 늘어야 표에서
     * 한 줄이 예외 없이 사라지는 일이 없습니다.</p>
     */
    private static final String ERROR_CLASS_RESULTS = ResultClass.promLabelAlternation(
            EnumSet.copyOf(Arrays.stream(ResultClass.values())
                    .filter(resultClass -> !resultClass.isSuccess())
                    .toList()));

    /** 실패 분류 비율의 단위. 스냅샷 {@code errors.classes[].rate} 와 같은 0~100 퍼센트다. */
    private static final String PERCENT = " * 100";

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
     * 요청 범위와 창의 시계열 열 종을 조립합니다.
     *
     * @param query 조회 구간과 선택적 관측 범위
     * @return 계열마다 상태가 붙은 시계열 응답
     */
    public AdminMetricsSeriesResponse assemble(MetricsQuery query) {
        Objects.requireNonNull(query, "query");
        MetricsWindow window = query.window();
        Long couponId = query.couponId();
        Duration step = properties.step();
        Instant end = timeProvider.instant().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        Instant start = end.minus(window.duration());

        Deadline deadline = Deadline.startingNow(properties.totalBudget());

        // ── 순서가 우선순위다. 예산이 모자라면 뒤가 잘린다 ──────────────────────────
        //
        // 판정 기준은 "잘렸을 때 무엇을 잃는가" 하나다. 스냅샷(/metrics)이 1초마다 현재값을
        // 주므로 여기서 잘려도 '지금 얼마인가' 는 남는다 — 이 경로에서만 얻는 것은 부하가
        // 오르던 구간의 모양이고, 그래서 부하 중에만 값이 생기는 계열을 앞에 둔다.
        // 새로 더한 계열을 뒤에 두면 평소엔 되다가 부하 때만 조용히 비는데, 그때가 정확히
        // 이 화면이 필요한 순간이다. 이 순서를 지키는 것은 priorityOrderIsPinned 다 — 아래
        // 주석은 근거일 뿐 보증이 아니다.
        List<SeriesEntry> series = new ArrayList<>();

        // 합격 판정을 가르는 값이다. /metrics 가 정합성을 KPI 첫 칸에 두는 것과 같은 이유다.
        series.addAll(collect(SeriesKey.CONSISTENCY_GAP, consistencyGapQuery(couponId),
                start, end, step, deadline));
        // 대기열은 부하 중에만 존재한다. 잘리면 대기열이 언제 얼마나 쌓였는지를 되짚을
        // 원천이 아예 없다 — batch 가 1초마다 덮어쓰는 게이지라 과거가 남지 않는다.
        series.addAll(collect(SeriesKey.QUEUE_ADMISSION, admissionQueueQuery(couponId),
                start, end, step, deadline));
        // 포화의 선행 지표다. 처리량이 꺾이기 전에 먼저 오르므로 사후 분석의 시작점이 된다.
        series.addAll(collect(SeriesKey.IN_FLIGHT, inFlightQuery(),
                start, end, step, deadline));
        // 처리량과 실패율. 화면의 주 그래프 두 줄이다.
        series.addAll(collect(SeriesKey.THROUGHPUT, throughputQuery(step),
                start, end, step, deadline));
        series.addAll(collect(SeriesKey.FAILURE_RATE, failureRateQuery(step),
                start, end, step, deadline));
        // 실패의 내역이다. 바로 위 실패율이 살아 있으면 이것이 잘려도 "실패가 있었다" 는
        // 사실은 남는다 — 그래서 실패율보다 뒤다.
        series.addAll(collect(SeriesKey.ERROR_CLASS_RATE, errorClassRateQuery(step),
                start, end, step, deadline));
        // 사유별 내역. 바로 위 분류보다 뒤다 — 라벨 카디널리티가 가장 커 계열 수가 가장 많고,
        // 잘려도 분류 표가 어느 갈래의 실패인지는 이미 말해 준다.
        series.addAll(collect(SeriesKey.FAILURE_REASON_RATE,
                // 발급 결과 사유 미터에는 회차 식별자 짝이 없어 좁힐 수 없다.
                ScopedQuery.global(OverviewPrometheusContract.failureReasonRates(step)),
                start, end, step, deadline));
        // 마지막이다. 지연 백분위는 창과 무관하게 각 점이 독립이라 잘려도 다른 값의 해석을
        // 바꾸지 않는 유일한 계열이다.
        series.addAll(collect(SeriesKey.LATENCY_P99, latencyQuery(),
                start, end, step, deadline));
        // 원천이 없는 계열이다. 질의를 보내지 않으므로 예산을 쓰지 않고 절단 순서와도 무관하다.
        series.add(sourceMissing(SeriesKey.QUEUE_PERSISTENCE));
        series.add(sourceMissing(SeriesKey.QUEUE_TELEMETRY));

        // 계열을 다 보낸 뒤다. 기준선은 계열이 있어야 의미가 있고, 둘 중 하나만 남길 수 있다면
        // 남길 것은 계열이다.
        MarkerResult markers = markers(start, end, step, couponId, deadline);

        return new AdminMetricsSeriesResponse(
                new Meta(Meta.SCHEMA_VERSION, start, end, step.toSeconds(), deadline.elapsedMillis()),
                scope(query),
                window,
                series,
                markers.markers(),
                markers.state());
    }

    /**
     * 요청 범위를 응답에 되비칩니다.
     *
     * <p><b>COUPON 이 모든 계열에 걸리지는 않습니다.</b> 회차로 좁힐 수 있는 것은 회차 식별자
     * 미터를 함께 내는 도메인 계열(정합성 gap · 대기열 · 재고)뿐이고, HTTP 미터에서 나오는
     * 계열(처리량 · 실패율 · 지연 · in-flight · 실패 분류·사유)에는 회차 라벨이 아예 없어
     * 전역 값이 그대로 나갑니다 — {@code /metrics} 도 같습니다. 두 경로가 같은 조건에서 같은
     * 숫자를 내는 쪽을 택한 것이고, 어느 계열이 좁혀졌는지는 화면이 표식을 붙여야 합니다.</p>
     */
    private static MetricsScope scope(MetricsQuery query) {
        if (query.couponId() != null) {
            return new MetricsScope(MetricsScopeType.COUPON, query.couponId(), null);
        }
        // BENCHMARK_RUN 은 컨트롤러가 400 으로 막는다. 회차 경계 원천이 DB(BenchmarkRun)라
        // 이 조립기가 Prometheus 하나만 읽는다는 계약을 깨야 열린다 — 후속 티켓이다.
        return new MetricsScope(MetricsScopeType.GLOBAL, null, null);
    }

    // ── 질의 ────────────────────────────────────────────────────────────────────

    /**
     * 발급 경로 전체 처리량입니다.
     *
     * <p>집계 창은 {@code step} 과 같습니다 — 평가 간격보다 넓으면 이웃한 점이 같은 표본을 겹쳐
     * 세고, 좁으면 표본이 없는 점이 생깁니다.</p>
     */
    private static ScopedQuery throughputQuery(Duration step) {
        return ScopedQuery.global("sum(rate(" + MetricAggregation.HTTP_RESULT_TOTAL
                + "{" + TAG_URI_GROUP + "=\"" + UriGroup.ISSUE.tagValue() + "\"}"
                + "[" + step.toSeconds() + "s]))");
    }

    /**
     * 발급 시도 대비 실패 비율입니다.
     *
     * <p>분자와 분모를 <b>한 질의 안에서</b> 나눕니다. 따로 부르면 두 응답이 다른 시점을 가리켜
     * 비율이 1 을 넘거나 음수가 되는 점이 생깁니다 — {@link PromMetricsAssembler} 가 같은 표본을
     * 접어 쓰는 것과 같은 이유입니다.</p>
     */
    private static ScopedQuery failureRateQuery(Duration step) {
        String selector = "{" + TAG_URI_GROUP + "=\"" + UriGroup.ISSUE.tagValue() + "\"";
        String range = "[" + step.toSeconds() + "s]";
        return ScopedQuery.global("sum(rate(" + MetricAggregation.HTTP_RESULT_TOTAL
                + selector + "," + TAG_RESULT + "=~\"" + FAILURE_RESULTS + "\"}" + range + "))"
                + " / sum(rate(" + MetricAggregation.HTTP_RESULT_TOTAL + selector + "}" + range + "))");
    }

    /**
     * 발급 경로 <b>성공</b> 응답시간 p99(ms)입니다.
     *
     * <p>인스턴스별로 계산된 값이라 합칠 수 없어 최댓값을 씁니다({@link MetricAggregation#MAX}).
     * 화면은 '인스턴스 최댓값' 표식을 붙여야 합니다.</p>
     *
     * <p>⚠️ 이 질의에는 창이 없습니다. 백분위의 관측 창은 Micrometer expiry 가 정하고 PromQL 로는
     * 바꿀 수 없습니다 — 창은 rate 계열에만 걸립니다.</p>
     *
     * <p><b>성공 경로만 봅니다.</b> ⚠️ <b>{@code outcome} 셀렉터를 빼면 안 됩니다.</b> OBS-31 이 Timer 를 outcome 넷으로 가른
     * 뒤로, 셀렉터가 없으면 {@code max()} 가 <b>가장 느린 축</b>을 집습니다 — 성공 경로는 그대로인데
     * 이 계열만 튑니다(프로브 실측 {@code 243.3ms → 2952.8ms}). 정책 거절이 쏟아지는 구간에서는
     * 반대로 실패 축이 희석돼, 같은 계열이 상황에 따라 다른 것을 가리킵니다.</p>
     *
     * <p>시스템 실패의 지연 추세는 <b>이 계열에 섞지 않고 축을 따로 냅니다</b>(OBS-46). 여기서
     * 합치면 재고 소진 폭주 때 장애가 묻힙니다 — 실측으로 {@code failure.p99} 가 정책 거절
     * 50건일 때 3087ms, 5000건일 때 1.0ms 였습니다.</p>
     */
    private static ScopedQuery latencyQuery() {
        return ScopedQuery.global("max(" + MetricAggregation.HTTP_LATENCY_SECONDS
                + "{" + TAG_QUANTILE + "=\"0.99\"," + TAG_URI_GROUP + "=\"" + UriGroup.ISSUE.tagValue()
                + "\"," + TAG_OUTCOME + "=\"" + OUTCOME_SUCCESS + "\"}) * 1000");
    }

    /**
     * 정합성 gap 입니다.
     *
     * <p>집계하지 않습니다. gap 은 종류({@code type} 라벨)로만 나뉘고 원천이 batch 한 곳뿐이라
     * ({@link MetricAggregation#SINGLE}) 합치면 서로 다른 종류가 한 숫자로 뭉개집니다.</p>
     *
     * <p><b>LIVE 평가만 고릅니다.</b> 합격/불합격을 가르는 FINAL 판정이 같은 미터 이름으로 오면
     * 같은 종류가 계열 둘로 그려지고 화면이 부하 중 추세와 최종 판정을 섞어 읽습니다. 스냅샷
     * 경로가 {@code live()} 로 가르는 것과 같은 이유입니다.</p>
     *
     * <p><b>반대 방향 실패</b> — 이 라벨이 없는 gap 시계열이 생기면 셀렉터가 조용히 떨어뜨립니다.
     * 스냅샷 경로가 셀렉터 대신 Java 쪽에서 거르는 이유가 그것인데, 저쪽은 라벨 없는 미터(회차
     * ID·신선도)를 한 질의에 같이 담기 때문입니다. 이 질의는 gap 하나만 받으므로 셀렉터로
     * 거르는 편이 평가점을 덜 만듭니다.</p>
     */
    private static ScopedQuery consistencyGapQuery(Long couponId) {
        return scoped(MetricAggregation.CONSISTENCY_GAP
                + "{" + DomainMeterNames.TAG_PHASE + "=\"" + DomainMeterNames.PHASE_LIVE + "\"}",
                MetricAggregation.CONSISTENCY_COUPON_ID, couponId);
    }

    /**
     * 대기열 대기 인원입니다.
     *
     * <p>집계하지 않습니다 — 원천이 batch 한 곳뿐이고({@link MetricAggregation#SINGLE})
     * {@code job} 라벨을 걸면 표본이 통째로 사라집니다.</p>
     *
     * <p><b>NaN 점은 값이 아니라 "이유는 상태 미터가 낸다" 는 표시입니다.</b> 점에는 상태를 실을
     * 자리가 없어 null 로 나갑니다 — 스냅샷은 짝이 되는 상태 미터로 이유까지 말하지만 이 경로는
     * "그 시각에는 값이 없다" 까지만 말합니다.</p>
     */
    private static ScopedQuery admissionQueueQuery(Long couponId) {
        return scoped(MetricAggregation.QUEUE_LENGTH,
                MetricAggregation.OBSERVED_COUPON_ID, couponId);
    }

    /**
     * 처리 중인 요청 수입니다. <b>스레드가 아니라 요청</b>이라 tomcat busy 와 다른 값입니다.
     *
     * <p>{@code job="api"} 를 겁니다 — 스냅샷의 {@code saturation.inFlight.globalSum} 이 같은
     * 필터로 접은 값이라, 빼면 같은 화면의 현재값과 추세선이 다른 모집단을 가리킵니다.</p>
     */
    private static ScopedQuery inFlightQuery() {
        return ScopedQuery.global("sum(" + MetricAggregation.HTTP_IN_FLIGHT
                + "{" + TAG_JOB + "=\"" + JOB_API + "\"})");
    }

    /**
     * 실패 분류별 비율(0~100%)입니다.
     *
     * <p>분자와 분모를 <b>한 질의 안에서</b> 나눕니다 — {@link #failureRateQuery} 와 같은 이유입니다.
     * {@code on() group_left} 는 라벨이 없는 분모 하나를 분류마다 붙여 나누라는 뜻입니다.</p>
     *
     * <p><b>네 분류를 모두 폅니다.</b> 정책 거절과 클라이언트 요청 오류는 실패율 분자에서는 빠지지만
     * 표에서 지우지는 않습니다 — 안 보이면 그 트래픽이 어디로 갔는지 아무도 설명하지 못합니다.
     * 어느 분류가 분자에서 빠지는지는 스냅샷의 {@code excludedFromNumerator} 가 냅니다.</p>
     *
     * <p><b>반대 방향 실패</b> — 한 건도 없던 분류는 시계열 자체가 없어 계열이 아예 빠집니다.
     * 화면은 빠진 분류를 "0%" 가 아니라 "그 실패가 없었다" 로 읽어야 하고, 네 줄이 언제나
     * 있어야 하는 표는 스냅샷 쪽을 써야 합니다.</p>
     */
    private static ScopedQuery errorClassRateQuery(Duration step) {
        String selector = "{" + TAG_URI_GROUP + "=\"" + UriGroup.ISSUE.tagValue() + "\"";
        String range = "[" + step.toSeconds() + "s]";
        return ScopedQuery.global("sum by (" + TAG_RESULT + ") (rate("
                + MetricAggregation.HTTP_RESULT_TOTAL
                + selector + "," + TAG_RESULT + "=~\"" + ERROR_CLASS_RESULTS + "\"}" + range + "))"
                + " / on() group_left sum(rate(" + MetricAggregation.HTTP_RESULT_TOTAL
                + selector + "}" + range + "))" + PERCENT);
    }

    /**
     * 남은 재고입니다. 기준선({@code 재고 소진})을 만드는 데만 씁니다.
     *
     * <p>이 이름은 {@link MetricAggregation} 규칙표에 없습니다 — 규칙표는 Java 가 인스턴스 표본을
     * 줄일 때 쓰는 표이고, 이 경로는 줄이지 않기 때문입니다(원천이 batch 하나).</p>
     */
    private static ScopedQuery stockRemainingQuery(Long couponId) {
        return scoped(MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING),
                MetricAggregation.OBSERVED_COUPON_ID, couponId);
    }

    /**
     * 회차 범위를 질의에 겁니다.
     *
     * <p><b>회차는 라벨이 아니라 값입니다.</b>({@link DomainMeterNames}) 그래서 셀렉터로 자를 수
     * 없고 {@code and on() (식별자 미터 == 회차)} 로 <b>평가점마다</b> 거릅니다 — batch 가 회차를
     * 바꾼 순간 앞뒤가 정확히 갈립니다. 라벨 필터였다면 회차마다 시계열이 통째로 갈려 회차 간
     * 비교 질의를 쓸 수 없습니다.</p>
     *
     * <p><b>식별자 미터를 값 미터마다 따로 지정합니다.</b> 재고·대기열은 1초, 정합성은 그보다 느린
     * 주기로 갱신되므로 회차가 바뀌는 순간 둘이 잠시 어긋납니다 — 한쪽 미터로 다른 경로의 값을
     * 판별하면 그 창에서 범위 판정이 틀립니다.</p>
     *
     * <p><b>반대 방향 실패</b> — 회차가 한 번도 일치하지 않으면 점이 하나도 남지 않아 계열이
     * {@code PENDING} 입니다. "요청한 회차의 표본이 이 구간에 없다" 는 뜻이라 거짓은 아니지만,
     * "batch 가 다른 회차를 보고 있다" 와 "그 회차가 아직 시작되지 않았다" 를 구분하지는
     * 못합니다 — 구분하려면 걸지 않은 질의를 한 번 더 보내야 하고, 그 값은 요청한 범위의 값이
     * 아닙니다. 스냅샷은 한 시점이라 {@code N_A} 로 구분합니다.</p>
     */
    private static ScopedQuery scoped(String promQl, String scopeMetric, Long couponId) {
        if (couponId == null) {
            return ScopedQuery.global(promQl);
        }
        return new ScopedQuery(
                "(" + promQl + ") and on() (" + scopeMetric + " == " + couponId + ")", true);
    }

    /**
     * 질의와 <b>그 질의에 요청 범위가 실제로 걸렸는지</b>를 함께 담습니다.
     *
     * <p>둘을 따로 두면 갈립니다 — 게이트를 붙이는 자리와 플래그를 세우는 자리가 다르면 계열을
     * 하나 더할 때 한쪽만 하고도 컴파일이 되고, 화면은 전역 값에 회차 표식을 답니다. 여기서 함께
     * 만들면 그 상태를 표현할 자리가 없습니다.</p>
     *
     * @param promQl 실행할 PromQL
     * @param scoped 요청한 관측 범위가 이 질의에 걸렸으면 true
     */
    private record ScopedQuery(String promQl, boolean scoped) {

        /** @return 좁힐 수 있는 원천이 없어 언제나 전역인 질의 */
        static ScopedQuery global(String promQl) {
            return new ScopedQuery(promQl, false);
        }
    }

    // ── 실행 ────────────────────────────────────────────────────────────────────

    /**
     * 원천이 아직 없는 계열입니다. 질의를 보내지 않고 자리만 냅니다.
     *
     * <p><b>{@code UNAVAILABLE} 이 아니라 {@code PENDING} 입니다.</b> 원천이 죽은 것이 아니라
     * 아직 만들어지지 않은 것이고, 둘은 운영자가 취할 행동이 반대입니다. 0 은 더 나쁩니다 —
     * "큐가 비었다" 는 거짓말이 됩니다.</p>
     */
    private static SeriesEntry sourceMissing(SeriesKey key) {
        // 원천이 없으니 좁힐 것도 없다. 원천이 생기면 그때 회차 식별자 짝이 있는지부터 본다.
        return new SeriesEntry(key, Map.of(), false, SourceStatus.PENDING, List.of());
    }

    /**
     * 세로 기준선입니다. 시계열이 아니라 사건 시각이라 계열 배열에 섞지 않습니다.
     *
     * <p>지금 내는 것은 <b>재고 소진</b> 하나입니다. '부하 종료' 는 원천이 {@code BenchmarkRun}
     * 의 {@code loadStoppedAt} 즉 DB 라서, 이 조립기가 Prometheus 하나만 읽는다는 계약을 깨야
     * 열립니다 — 후속 티켓입니다.</p>
     *
     * <p><b>구간 안에서 일어난 전환만 냅니다.</b> 조회 구간이 시작될 때 이미 0 이면 소진 시각은
     * 이 구간 밖이라 날짜를 붙일 수 없습니다 — 첫 점에 기준선을 그으면 없는 사건을 만듭니다.</p>
     */
    private MarkerResult markers(
            Instant start, Instant end, Duration step, Long couponId, Deadline deadline) {
        if (!deadline.allows()) {
            // 빈 목록을 그냥 내려보내면 "소진이 없었다" 와 구분되지 않는다. 그리고 잘리는 것은
            // 부하가 걸린 회차 — 재고 소진 시각이 가장 궁금한 바로 그 회차다.
            log.warn("series 예산을 넘겨 기준선을 UNAVAILABLE 로 내려보냅니다 (couponId={})", couponId);
            return MarkerResult.unavailable();
        }
        ScopedQuery query = stockRemainingQuery(couponId);
        List<PromRangeSeries> raw;
        try {
            raw = rangeQuery.query(query.promQl(), start, end, step);
        } catch (PromQueryException failure) {
            // collect() 와 같은 이유로 IllegalArgumentException 은 잡지 않는다.
            log.warn("Prometheus 구간 질의 실패로 기준선을 UNAVAILABLE 로 내려보냅니다: {} (couponId={})",
                    failure.getMessage(), couponId);
            log.debug("Prometheus 구간 질의 실패 상세: {}", query.promQl(), failure);
            return MarkerResult.unavailable();
        }
        if (raw.size() > 1) {
            // 원천이 batch 하나여야 하는 미터다. 표본이 갈렸다면 어느 쪽 소진 시각인지 고를
            // 근거가 없다 — 하나를 골라 그으면 다른 회차의 시각을 이 회차에 붙인다.
            log.warn("재고 미터의 시계열이 {}개라 기준선을 고를 수 없습니다 (couponId={})",
                    raw.size(), couponId);
            return MarkerResult.unavailable();
        }
        if (raw.isEmpty()) {
            // 원천이 죽은 것이 아니라 아직 재고 표본이 없는 것이다. collect() 와 같은 판정이다.
            return MarkerResult.pending();
        }
        return MarkerResult.of(raw.stream()
                .map(PromSeriesAssembler::stockExhaustedAt)
                .flatMap(Optional::stream)
                .map(at -> new Marker(at, Marker.STOCK_EXHAUSTED))
                .toList());
    }

    /**
     * 기준선 목록과 <b>그 목록이 사실인지</b>를 함께 담습니다.
     *
     * <p>빈 목록만으로는 "소진이 없었다" 와 "물어보지 못했다" 가 같은 모양입니다. 계열이
     * {@code state} 로 그 둘을 가르는 것과 같은 이유로 기준선에도 상태를 붙입니다 — 원천이
     * 재고 미터 하나뿐이라 상태도 목록 전체에 하나입니다.</p>
     */
    private record MarkerResult(List<Marker> markers, SourceStatus state) {

        /** @return 물어봤고 답을 받았다. 목록이 비었으면 구간 안에 소진이 없었다는 뜻이다 */
        static MarkerResult of(List<Marker> markers) {
            return new MarkerResult(List.copyOf(markers), SourceStatus.VALID);
        }

        /** @return 재고 표본이 아직 없다. 원천이 죽은 것과 다르다 */
        static MarkerResult pending() {
            return new MarkerResult(List.of(), SourceStatus.PENDING);
        }

        /** @return 예산 절단 · 질의 실패 · 원천 다중으로 물어보지 못했다 */
        static MarkerResult unavailable() {
            return new MarkerResult(List.of(), SourceStatus.UNAVAILABLE);
        }
    }

    /** @return 남은 재고가 <b>양수에서 0 이하로</b> 처음 바뀐 점의 시각 */
    private static Optional<Instant> stockExhaustedAt(PromRangeSeries series) {
        boolean sawPositive = false;
        for (PromRangePoint point : series.points()) {
            if (!point.hasNumericValue()) {
                // NaN 은 "재고가 0" 이 아니라 "이유는 상태 미터가 낸다" 는 표시다. 여기서 소진으로
                // 읽으면 수집이 끊긴 순간마다 없는 기준선이 생긴다.
                continue;
            }
            if (point.value() > 0d) {
                sawPositive = true;
            } else if (sawPositive) {
                return Optional.of(point.observedAt());
            }
        }
        return Optional.empty();
    }

    /**
     * 계열 하나를 조회합니다. 실패는 이 계열 안에 가둡니다.
     */
    private List<SeriesEntry> collect(
            SeriesKey key, ScopedQuery query, Instant start, Instant end, Duration step,
            Deadline deadline) {
        boolean scoped = query.scoped();
        if (!deadline.allows()) {
            log.warn("series 예산을 넘겨 계열을 보내지 않고 UNAVAILABLE 로 내려보냅니다: {}", key);
            return List.of(SeriesEntry.unavailable(key, scoped));
        }
        List<PromRangeSeries> raw;
        try {
            raw = rangeQuery.query(query.promQl(), start, end, step);
        } catch (PromQueryException failure) {
            // 500 으로 올리지 않는다. 이 계열만 UNAVAILABLE 로 나가고 나머지는 그려진다.
            // 스택트레이스는 DEBUG 에만 싣는다 — 원천이 죽은 동안 매 폴링마다 쌓인다.
            //
            // ⚠️ IllegalArgumentException 을 함께 잡지 않는다. PromRangeQueryClient 가 그것을
            //    던지는 경우는 빈 PromQL · end<=start · step<=0 뿐이고 셋 다 우리 쪽 버그다.
            //    원천이 낼 수 있는 거절(범위·평가점 상한)과 응답 파싱 실패는 전부
            //    PromQueryException 이다. 버그를 여기서 삼키면 화면이 UNAVAILABLE 을 그려
            //    "Prometheus 가 죽었다" 고 말하고, 운영자는 멀쩡한 Prometheus 를 들여다본다 —
            //    이 파일이 다른 모든 자리에서 거부하는 바로 그 거짓이다. 500 으로 올려야
            //    GlobalExceptionHandler 가 스택트레이스를 남기고 눈에 띈다.
            log.warn("Prometheus 구간 질의 실패로 계열을 UNAVAILABLE 로 내려보냅니다: {} ({})",
                    key, failure.getMessage());
            log.debug("Prometheus 구간 질의 실패 상세: {}", query.promQl(), failure);
            return List.of(SeriesEntry.unavailable(key, scoped));
        }
        if (raw.isEmpty()) {
            // 원천이 죽은 것이 아니라 아직 표본이 없는 것이다. 둘은 운영자가 취할 행동이 반대다.
            return List.of(new SeriesEntry(key, Map.of(), scoped, SourceStatus.PENDING, List.of()));
        }
        List<SeriesEntry> entries = new ArrayList<>();
        for (PromRangeSeries series : raw) {
            entries.add(new SeriesEntry(
                    key, displayLabels(series), scoped, SourceStatus.VALID, points(series)));
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

        private final long startedAtNanos;
        private final long expiresAtNanos;
        private boolean anyIssued;

        private Deadline(long startedAtNanos, long expiresAtNanos) {
            this.startedAtNanos = startedAtNanos;
            this.expiresAtNanos = expiresAtNanos;
        }

        static Deadline startingNow(Duration budget) {
            long now = System.nanoTime();
            return new Deadline(now, now + budget.toNanos());
        }

        /** 예산 판정과 같은 시계로 잰 경과 시간. 두 값이 갈리면 화면이 읽는 근접도가 거짓이 된다. */
        long elapsedMillis() {
            return Duration.ofNanos(System.nanoTime() - startedAtNanos).toMillis();
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
