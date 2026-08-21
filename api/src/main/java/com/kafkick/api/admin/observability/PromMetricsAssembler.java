package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ConsistencyResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.DependencyMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.DependencySnapshot;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyPercentiles;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScope;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScopeType;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.PersistenceLagSummary;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficMetrics;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;

/**
 * Prometheus 표본을 {@link AdminMetricsResponse} 한 장으로 조립합니다.
 *
 * <p>질의는 셀렉터로 묶어 네 번만 보냅니다. 지표마다 한 번씩 부르면 화면의 1 초 폴링이
 * Prometheus 를 초당 수십 번 두드립니다.</p>
 *
 * <p><b>원천은 서로 독립적으로 죽습니다.</b> 질의 실패든 표본 이상이든 예외를 밖으로 올리지 않고
 * 그것이 채우려던 값만 {@code UNAVAILABLE} 로 내려보냅니다 — 화면 전체가 죽으면 안 됩니다.</p>
 *
 * <p><b>관측 시각은 따로 묻습니다.</b> instant query 가 표본에 붙여 주는 시각은 질의 평가 시각이라
 * 언제나 "지금" 입니다. 그대로 쓰면 원천이 죽어도 관측 시각이 매 폴링마다 갱신되어 STALE 이
 * 구조적으로 나올 수 없습니다. 그래서 batch 는 수집 성공 시각 미터를, HTTP 는 {@code timestamp()}
 * 를 함께 읽어 실제 신선도를 판정합니다.</p>
 *
 * <p>이 안에서 Redis·DB 같은 원천을 다시 읽지 않습니다. 관리자 다섯 명이 보면 초당 다섯 번
 * 재수집이 됩니다. 읽는 곳은 Prometheus 하나뿐입니다.</p>
 */
public class PromMetricsAssembler {

    private static final Logger log = LoggerFactory.getLogger(PromMetricsAssembler.class);

    private static final String TAG_URI_GROUP = "uri_group";
    private static final String TAG_RESULT = "result";
    private static final String TAG_OUTCOME = "outcome";
    private static final String TAG_QUANTILE = "quantile";
    private static final String OUTCOME_SUCCESS = "success";

    /**
     * LIVE 평가 라벨. batch 는 부하 중 추세를 보는 이 값들에만 이 라벨을 답니다
     * ({@code DomainGaugeRegistrar.evaluationGauge}). 합격/불합격을 가르는 FINAL 판정은 조용해진
     * 뒤 검증 배치가 따로 냅니다 — 걸지 않으면 읽는 쪽이 둘을 같은 것으로 읽습니다.
     *
     * <p><b>셀렉터에 넣지 않습니다.</b> 회차 ID 와 신선도 미터에는 이 라벨이 없어서
     * ({@code gauge} 로 등록) 질의에 박으면 그 둘이 통째로 사라집니다.</p>
     */
    private static final String PHASE_LIVE_LABEL = DomainMeterNames.TAG_PHASE;

    private static final String Q_P50 = "0.5";
    private static final String Q_P95 = "0.95";
    private static final String Q_P99 = "0.99";

    private final PromQuery client;
    private final TimeProvider timeProvider;
    private final Duration staleAfter;

    public PromMetricsAssembler(PromQuery client, TimeProvider timeProvider, Duration staleAfter) {
        this.client = Objects.requireNonNull(client, "client");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.staleAfter = Objects.requireNonNull(staleAfter, "staleAfter");
    }

    /**
     * 요청 범위와 집계 창의 지표 스냅샷을 만듭니다.
     *
     * @param query 집계 창과 선택적 관측 범위
     * @return 값마다 상태가 붙은 한 시점 스냅샷
     */
    public AdminMetricsResponse assemble(MetricsQuery query) {
        MetricsWindow window = query.window();

        QueryResult latency = run(latencyQuery());
        QueryResult results = run(resultRateQuery(window));
        QueryResult httpAge = run(httpFreshnessQuery());
        QueryResult consistency = run(consistencyQuery());

        Instant evaluatedAt = evaluatedAt(latency, results, httpAge, consistency)
                .orElseGet(timeProvider::instant);
        Freshness http = httpFreshness(httpAge, evaluatedAt);
        Freshness domain = domainFreshness(consistency, evaluatedAt);

        return new AdminMetricsResponse(
                scope(query),
                evaluatedAt,
                window,
                consistency(consistency, domain, query),
                traffic(results, http),
                latency(latency, http),
                dependencies(),
                notWiredYet(),
                List.of());
    }

    // ── 질의 ────────────────────────────────────────────────────────────────────

    private static String latencyQuery() {
        // uri 그룹·결과·인스턴스 라벨을 그대로 받아 온다. 여기서 합치면 집계 규칙을 Java 가
        // 적용할 수 없고, 그룹을 합친 전체 p99 는 애초에 만들지 않는다.
        //
        // ⚠️ 이 질의에는 window 가 없다. 백분위의 관측 창은 Micrometer expiry(observation.yml,
        //    10s)가 정하고 PromQL 로는 바꿀 수 없다 — window 는 rate 계열에만 걸린다.
        return MetricAggregation.HTTP_LATENCY_SECONDS + "{" + TAG_QUANTILE + "!=\"\"}";
    }

    private static String resultRateQuery(MetricsWindow window) {
        // window 는 되돌아볼 범위가 아니라 비율을 계산할 집계 창이다. 그래서 질의 안에 들어간다.
        return "rate(" + MetricAggregation.HTTP_RESULT_TOTAL + "[" + window.seconds() + "s])";
    }

    /**
     * HTTP 미터가 마지막으로 스크레이프된 뒤 흐른 시간(초) 중 <b>가장 오래된</b> 값입니다.
     *
     * <p>{@code timestamp()} 는 표본 자신의 시각을 돌려주므로 평가 시각과 빼면 나이가 됩니다.
     * 최댓값을 쓰는 이유는 인스턴스 하나가 스크레이프에서 빠졌을 때 그것이 드러나야 하기
     * 때문입니다 — 최솟값을 쓰면 살아 있는 인스턴스가 죽은 인스턴스를 가립니다.</p>
     */
    private static String httpFreshnessQuery() {
        return "max(time() - timestamp({__name__=~\""
                + MetricAggregation.HTTP_RESULT_TOTAL + "|"
                + MetricAggregation.HTTP_LATENCY_SECONDS + "\"}))";
    }

    private static String consistencyQuery() {
        // 정합성 값 미터와 상태 미터를 한 번에 받는다. 둘을 따로 부르면 그 사이에 batch 가
        // 값을 갱신해 값과 상태가 다른 순간을 가리키게 된다. 수집 성공 시각도 같이 받는다 —
        // 그것이 이 값들의 진짜 관측 시각이다.
        return "{__name__=~\"" + String.join("|",
                MetricAggregation.CONSISTENCY_GAP,
                MetricAggregation.CONSISTENCY_GAP_STATE,
                MetricAggregation.OVER_ISSUED,
                MetricAggregation.OVER_ISSUED_STATE,
                MetricAggregation.CONSISTENCY_SEVERITY,
                MetricAggregation.CONSISTENCY_SEVERITY_STATE,
                MetricAggregation.CONSISTENCY_COUPON_ID,
                MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH) + "\"}";
    }

    private QueryResult run(String promQl) {
        try {
            return QueryResult.of(client.query(promQl));
        } catch (PromQueryException failure) {
            // 500 으로 올리지 않는다. 이 질의가 채우려던 값만 UNAVAILABLE 로 나간다.
            //
            // ⚠️ 스택트레이스는 DEBUG 에만 싣는다. 화면이 1 초마다 부르고 한 응답에 질의가 넷이라,
            //    Prometheus 가 죽으면 관리자 수 × 초당 4 건이 쌓인다 — 정작 장애 원인을 담은
            //    다른 로그가 묻힌다. 조용히 삼키지는 않으므로 한 줄은 WARN 으로 남긴다.
            log.warn("Prometheus 질의 실패로 해당 지표를 UNAVAILABLE 로 내려보냅니다: {} ({})",
                    promQl, failure.getMessage());
            log.debug("Prometheus 질의 실패 상세: {}", promQl, failure);
            return QueryResult.failed();
        }
    }

    // ── 신선도 ──────────────────────────────────────────────────────────────────

    /**
     * 원천이 값을 관측한 시각과 그것이 낡았는지 여부입니다.
     *
     * <p>{@code observedAt} 을 모르면 {@link Optional#empty()} 입니다 — 평가 시각으로 채우지
     * 않습니다. 모르는 시각을 "지금" 이라고 적는 것이 값 없음을 0 으로 적는 것과 같은 종류의
     * 거짓말이기 때문입니다.</p>
     */
    private record Freshness(Instant observedAt, boolean stale, boolean sourceDown) {

        /** 아직 관측된 적이 없거나 신선도 미터가 없다. 값이 나올 수는 있지만 시각을 모른다. */
        static Freshness observationTimeUnknown() {
            return new Freshness(null, false, false);
        }

        /** 신선도를 물을 수 없었다. 미수집(PENDING)과 구분해야 할 장애다. */
        static Freshness querySourceDown() {
            return new Freshness(null, false, true);
        }

        boolean known() {
            return observedAt != null;
        }
    }

    /**
     * 신선도를 모를 때 값이 어떤 상태로 나가야 하는지 정합니다.
     *
     * <p>PENDING("아직 안 나옴, 기다려라")과 UNAVAILABLE("원천이 죽었다, 조치하라")은 운영자가
     * 취할 행동이 정반대라 뭉개면 안 됩니다.</p>
     */
    private static <T> ObservedValue<T> absent(Freshness freshness) {
        return freshness.sourceDown() ? unavailable() : pending();
    }

    private Freshness httpFreshness(QueryResult ageResult, Instant evaluatedAt) {
        if (ageResult.unavailable()) {
            return Freshness.querySourceDown();
        }
        OptionalDouble ageSeconds = reduceOrUnknown(MetricAggregation.HTTP_FRESHNESS_AGE_SECONDS,
                ageResult.filter(any()));
        if (ageSeconds.isEmpty()) {
            return Freshness.observationTimeUnknown();
        }
        return freshness(evaluatedAt.minusMillis(Math.round(ageSeconds.getAsDouble() * 1000)), evaluatedAt);
    }

    private Freshness domainFreshness(QueryResult samples, Instant evaluatedAt) {
        if (samples.unavailable()) {
            return Freshness.querySourceDown();
        }
        try {
            OptionalDouble lastSuccessEpoch = reduceOrUnknown(
                    MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                    samples.filter(named(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH)
                            .and(label(DomainMeterNames.TAG_COLLECT_PATH,
                                    DomainMeterNames.PATH_CONSISTENCY))));
            if (lastSuccessEpoch.isEmpty()) {
                // 한 번도 성공하지 못했거나 신선도 미터가 없다. 어느 쪽이든 관측 시각을 모른다.
                return Freshness.observationTimeUnknown();
            }
            return freshness(
                    Instant.ofEpochMilli(Math.round(lastSuccessEpoch.getAsDouble() * 1000)), evaluatedAt);
        } catch (IllegalStateException ruleViolation) {
            // 여기서 밖으로 올리면 batch 를 두 대로 늘리는 순간 응답 전체가 500 이 된다.
            // 어느 batch 의 시각인지 못 고르면 관측 시각을 모르는 것이다.
            // 규칙 위반은 배선이 고쳐질 때까지 매 폴링 반복된다. 스택트레이스는 DEBUG 로 내린다.
            log.warn("신선도 미터 집계 규칙을 지킬 수 없어 관측 시각을 모르는 것으로 처리합니다: {} ({})",
                    MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH, ruleViolation.getMessage());
            log.debug("신선도 미터 집계 규칙 위반 상세", ruleViolation);
            return Freshness.observationTimeUnknown();
        }
    }

    private Freshness freshness(Instant observedAt, Instant evaluatedAt) {
        return new Freshness(observedAt,
                Duration.between(observedAt, evaluatedAt).compareTo(staleAfter) > 0, false);
    }

    // ── 조립 ────────────────────────────────────────────────────────────────────

    private static MetricsScope scope(MetricsQuery query) {
        if (query.couponId() != null) {
            return new MetricsScope(MetricsScopeType.COUPON, query.couponId(), null);
        }
        if (query.benchmarkRunId() != null) {
            return new MetricsScope(MetricsScopeType.BENCHMARK_RUN, null, query.benchmarkRunId());
        }
        return new MetricsScope(MetricsScopeType.GLOBAL, null, null);
    }

    /**
     * 결과 분류별 처리량입니다.
     *
     * <p>발급 경로 지표는 {@code uri_group=issue} 로 먼저 쪼갠 뒤 인스턴스만 합칩니다. 그룹을
     * 합치면 순번 폴링이 발급 처리량을 덮습니다. 대기열 수락만 {@code entry} 경로에서 나오므로
     * 결과 분류로 집습니다.</p>
     *
     * <p><b>이 값은 언제나 전역입니다.</b> {@code scope} 가 COUPON 이어도 회차로 좁혀지지 않습니다 —
     * HTTP 미터에는 회차 라벨이 없고(카디널리티 방어) 이 API 안에서 만들 수도 없습니다. 화면은
     * COUPON 범위에서 트래픽·지연 패널에 '전역' 이라고 밝혀야 합니다.</p>
     *
     * <p><b>{@code NO_TRAFFIC} 은 그룹 전체 처리량에만 붙입니다.</b> 하위 분류가 0 인 것은
     * "요청이 없었다" 가 아니라 "그 결과가 한 건도 없었다" 입니다 — 초당 5,000 건이 전부 성공
     * 중일 때 시스템 실패 0 을 NO_TRAFFIC 으로 내보내면 화면이 그 패널만 회색으로 죽입니다.</p>
     */
    private static TrafficMetrics traffic(QueryResult results, Freshness freshness) {
        Predicate<PromSample> issue = inGroup(UriGroup.ISSUE);
        ObservedValue<Double> attempts = rate(results, issue, freshness, SourceStatus.NO_TRAFFIC);
        SourceStatus zeroState =
                attempts.state() == SourceStatus.NO_TRAFFIC ? SourceStatus.NO_TRAFFIC : SourceStatus.VALID;
        return new TrafficMetrics(
                attempts,
                rate(results, issue.and(result(ResultClass.SUCCESS)), freshness, zeroState),
                rate(results, result(ResultClass.QUEUE_ACCEPTED), freshness, zeroState),
                rate(results, issue.and(result(ResultClass.POLICY_REJECT)), freshness, zeroState),
                rate(results, issue.and(
                        result(ResultClass.DEPENDENCY_FAILURE).or(result(ResultClass.APPLICATION_FAILURE))),
                        freshness, zeroState));
    }

    private static Predicate<PromSample> inGroup(UriGroup group) {
        return label(TAG_URI_GROUP, group.tagValue());
    }

    private static Predicate<PromSample> result(ResultClass resultClass) {
        return label(TAG_RESULT, resultClass.name().toLowerCase(Locale.ROOT));
    }

    private static Predicate<PromSample> label(String name, String value) {
        return sample -> value.equals(sample.label(name));
    }

    private static ObservedValue<Double> rate(
            QueryResult results, Predicate<PromSample> filter, Freshness freshness, SourceStatus zeroState) {
        if (results.unavailable()) {
            return unavailable();
        }
        OptionalDouble reduced =
                reduceOrUnknown(MetricAggregation.HTTP_RESULT_TOTAL, results.filter(filter));
        if (reduced.isEmpty()) {
            return pending();
        }
        if (!freshness.known()) {
            return absent(freshness);
        }
        SourceStatus state = freshness.stale()
                ? SourceStatus.STALE
                : (reduced.getAsDouble() == 0d ? zeroState : SourceStatus.VALID);
        return new ObservedValue<>(reduced.getAsDouble(), state, freshness.observedAt());
    }

    /**
     * 결과 경로별 지연입니다.
     *
     * <p>백분위는 인스턴스별로 계산된 값이라 평균·합산할 수 없습니다({@code publishPercentiles} 가
     * {@code quantile} 라벨만 내보내고 {@code _bucket} 시계열이 0 개라 재계산도 불가능합니다).
     * <b>인스턴스 최댓값</b>을 씁니다(DEC-02).</p>
     *
     * <p>⚠️ 응답 계약의 {@code LatencyPercentiles} 필드 이름({@code p99Millis})은 전역값처럼
     * 읽힙니다. 이름에 집계 방식을 박을 자리가 계약에 없으므로 <b>화면이 '인스턴스 최댓값'
     * 라벨을 붙여야 합니다</b> — 그 자리를 계약에 만드는 것은 A 와 합의할 항목입니다.</p>
     *
     * <p>정책 거절과 시스템 실패 지연은 원천이 없어 PENDING 입니다. OBS-4 의 Timer 는
     * {@code outcome} 을 success · failure 둘로만 등록해 실패 안에서 두 경로가 섞여 있고,
     * 1ms 미만인 정책 거절을 시스템 실패로 실으면 화면이 정확히 반대로 읽습니다.</p>
     */
    private static LatencyMetrics latency(QueryResult latency, Freshness freshness) {
        return new LatencyMetrics(
                percentiles(latency, inGroup(UriGroup.ISSUE).and(label(TAG_OUTCOME, OUTCOME_SUCCESS)),
                        freshness),
                pending(),
                pending());
    }

    private static ObservedValue<LatencyPercentiles> percentiles(
            QueryResult latency, Predicate<PromSample> filter, Freshness freshness) {
        if (latency.unavailable()) {
            return unavailable();
        }
        List<PromSample> matched = latency.filter(filter);
        OptionalDouble p50 = quantile(matched, Q_P50);
        OptionalDouble p95 = quantile(matched, Q_P95);
        OptionalDouble p99 = quantile(matched, Q_P99);
        if (p50.isEmpty() || p95.isEmpty() || p99.isEmpty()) {
            return pending();
        }
        if (!freshness.known()) {
            return absent(freshness);
        }
        // expiry 로 관측 창이 비면 백분위가 0 으로 읽힌다(실측). 0 을 내보내면 화면이
        // "지연 0ms" 를 그린다 — "트래픽 없음" 보다 나쁜 거짓말이다.
        //
        // 같은 규칙이 MeterValueReader.percentileNanos 에도 있다(test 소스셋). 저쪽은 자기 JVM
        // 레지스트리를, 이쪽은 Prometheus 를 읽어 원천이 달라 코드를 공유할 수 없다. 한쪽만
        // 고치면 두 경로가 다른 값을 그리므로 함께 고친다.
        if (p99.getAsDouble() <= 0d) {
            return pending();
        }
        return new ObservedValue<>(
                new LatencyPercentiles(millis(p50), millis(p95), millis(p99)),
                freshness.stale() ? SourceStatus.STALE : SourceStatus.VALID,
                freshness.observedAt());
    }

    private static OptionalDouble quantile(List<PromSample> samples, String quantile) {
        return reduceOrUnknown(MetricAggregation.HTTP_LATENCY_SECONDS,
                samples.stream().filter(label(TAG_QUANTILE, quantile)).toList());
    }

    private static double millis(OptionalDouble seconds) {
        return seconds.getAsDouble() * 1000d;
    }

    /**
     * 정합성 판정과 gap 입니다. 값 미터와 상태 미터를 <b>짝으로 읽어</b> 조립합니다 — batch 는
     * 값이 없을 때 값 미터에 NaN 을 싣고 이유는 상태 미터가 냅니다.
     */
    private ConsistencyResponse consistency(
            QueryResult samples, Freshness freshness, MetricsQuery query) {
        return new ConsistencyResponse(
                // FINAL 판정은 조용해진 뒤 검증 배치가 따로 한다. 이 API 가 내는 것은 LIVE 뿐이다.
                ConsistencyPhase.LIVE,
                null,
                severity(samples, freshness, query),
                pairedLong(samples, MetricAggregation.OVER_ISSUED,
                        MetricAggregation.OVER_ISSUED_STATE, live(), freshness, query),
                gap(samples, ConsistencyGapType.LUA_GAP, freshness, query),
                gap(samples, ConsistencyGapType.ACTIVE_DB_GAP, freshness, query),
                gap(samples, ConsistencyGapType.DB_COUNTER_GAP, freshness, query),
                gap(samples, ConsistencyGapType.PERSIST_GAP, freshness, query));
    }

    private ObservedValue<Long> gap(
            QueryResult samples, ConsistencyGapType gapType, Freshness freshness, MetricsQuery query) {
        Predicate<PromSample> filter = live().and(label(
                DomainMeterNames.TAG_GAP_TYPE, DomainMeterNames.gapTagValue(gapType)));
        return pairedLong(samples, MetricAggregation.CONSISTENCY_GAP,
                MetricAggregation.CONSISTENCY_GAP_STATE, filter, freshness, query);
    }

    /**
     * LIVE 심각도입니다.
     *
     * <p>⚠️ 응답 계약의 {@code severity} 는 {@code ObservedValue} 로 감싸이지 않은 맨 enum 이라
     * 값이 없는 이유를 실을 자리가 없습니다. Prometheus 다운 · batch 미계산 · 다른 회차 관측 ·
     * 상태 미터 부재가 모두 {@code null} 하나로 뭉개집니다. <b>화면은 severity 가 null 이면
     * '위험 없음' 이 아니라 '판정 없음' 으로 그려야 합니다.</b> 이유를 싣는 자리를 만드는 것은
     * A 와 합의할 계약 변경 항목입니다.</p>
     */
    private Severity severity(QueryResult samples, Freshness freshness, MetricsQuery query) {
        ObservedValue<Long> observed = pairedLong(samples, MetricAggregation.CONSISTENCY_SEVERITY,
                MetricAggregation.CONSISTENCY_SEVERITY_STATE, live(), freshness, query);
        if (observed.state() != SourceStatus.VALID) {
            // 못 읽은 것도, 낡은 것도 NONE 으로 채우면 "정상" 으로 읽힌다. 5분 전 판정을 지금
            // 판정처럼 내보내는 것은 화면이 임계치를 다시 구현하는 것보다 나쁘다 — 틀린 값을
            // 권위 있게 내보낸다.
            return null;
        }
        try {
            return SourceStatusCode.severityOf(observed.value().intValue());
        } catch (IllegalArgumentException unknownCode) {
            log.warn("정의되지 않은 심각도 코드입니다: {}", observed.value());
            return null;
        }
    }

    /**
     * 값 미터와 상태 미터를 한 쌍으로 읽습니다.
     *
     * <p>상태 미터가 없으면 PENDING 입니다. 상태가 값을 요구하는데 값 미터가 NaN 이면 둘이
     * 어긋난 것이므로 조작하지 않고 표시 없음(PENDING)으로 내려보냅니다.</p>
     *
     * <p>집계 규칙 위반({@code SINGLE} 인데 표본이 여럿)은 <b>여기서 잡아 이 값만</b>
     * {@code UNAVAILABLE} 로 만듭니다. 밖으로 올리면 batch 를 두 대로 늘리거나 scrape 대상을
     * 추가한 순간 응답 전체가 500 이 됩니다.</p>
     */
    private ObservedValue<Long> pairedLong(
            QueryResult samples, String valueMetric, String stateMetric,
            Predicate<PromSample> filter, Freshness freshness, MetricsQuery query) {
        if (samples.unavailable()) {
            return unavailable();
        }
        try {
            if (outOfScope(samples, query)) {
                // 다른 회차를 보고 있는 값이다. 0 도 아니고 장애도 아니라 '해당 없음' 이다.
                return new ObservedValue<>(null, SourceStatus.N_A, null);
            }

            Optional<SourceStatus> state =
                    readState(stateMetric, samples.filter(named(stateMetric).and(filter)));
            if (state.isEmpty()) {
                return pending();
            }
            SourceStatus resolved = state.get();
            if (!resolved.carriesValue()) {
                // UNAVAILABLE · N_A · PENDING 은 batch 가 실어 보낸 이유 그대로 나간다.
                return new ObservedValue<>(null, resolved, null);
            }

            OptionalDouble value =
                    reduceOrUnknown(valueMetric, samples.filter(named(valueMetric).and(filter)));
            if (!freshness.known()) {
                return absent(freshness);
            }
            if (value.isEmpty()) {
                // 상태 VALID + 값 NaN 은 장애가 아니다. scrape 가 미터를 하나씩 읽어 가는 사이에
                // batch 가 스냅샷을 갈아 끼우면 둘이 한 틱 어긋난다 — DomainGaugeRegistrar 가
                // 남는 창으로 문서화한 상태다. gap 은 GapValue 불변식이 STALE 에도 값을 요구하므로
                // "이유 있는 값 없음" 이 이 분기로 오는 일은 없다(그건 위 carriesValue 분기다).
                log.debug("값 미터에 숫자가 없어 표시 없음으로 내려보냅니다: {} (상태 {})", valueMetric, resolved);
                return pending();
            }
            SourceStatus reported = freshness.stale() ? SourceStatus.STALE : resolved;
            return new ObservedValue<>(Math.round(value.getAsDouble()), reported, freshness.observedAt());
        } catch (IllegalStateException ruleViolation) {
            log.warn("집계 규칙을 지킬 수 없어 이 값만 UNAVAILABLE 로 내려보냅니다: {} ({})",
                    valueMetric, ruleViolation.getMessage());
            log.debug("집계 규칙 위반 상세: {}", valueMetric, ruleViolation);
            return unavailable();
        }
    }

    private static Optional<SourceStatus> readState(String stateMetric, List<PromSample> stateSamples) {
        OptionalDouble code = reduceOrUnknown(stateMetric, stateSamples);
        if (code.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(SourceStatusCode.statusOf((int) Math.round(code.getAsDouble())));
        } catch (IllegalArgumentException unknownCode) {
            log.warn("정의되지 않은 상태 코드입니다: {} = {}", stateMetric, code.getAsDouble());
            return Optional.of(SourceStatus.UNAVAILABLE);
        }
    }

    /**
     * COUPON 범위 요청인데 batch 가 다른 회차를 보고 있으면 true 입니다.
     *
     * <p>BENCHMARK_RUN 은 라벨이 없어 한 시점 질의로 자를 수 없습니다 — 회차 경계는 시간 범위로
     * 자르는 것이고 그건 시계열 API 의 몫입니다. 여기서는 범위만 응답에 되비칩니다.</p>
     */
    private static boolean outOfScope(QueryResult samples, MetricsQuery query) {
        if (query.couponId() == null) {
            return false;
        }
        OptionalDouble observedCoupon = reduceOrUnknown(MetricAggregation.CONSISTENCY_COUPON_ID,
                samples.filter(named(MetricAggregation.CONSISTENCY_COUPON_ID)));
        // 어느 회차를 본 값인지 모르면 그 회차의 값이라고 말할 수 없다. 확인된 일치만 in-scope 다.
        return observedCoupon.isEmpty() || Math.round(observedCoupon.getAsDouble()) != query.couponId();
    }

    /**
     * Redis·HikariCP·Kafka 의존성 지연입니다.
     *
     * <p>세 원천 모두 아직 미터가 없습니다. Redis 지연은 OBS-10 의 명시 wiring 이 있어야 나오고
     * (없으면 영원히 0 입니다), Kafka 는 OBS-17 의 AdminClient 배선이 필요합니다. 0 을 채우면
     * 화면이 "지연 없음" 을 그리므로 PENDING 으로 둡니다.</p>
     */
    private static DependencyMetrics dependencies() {
        return new DependencyMetrics(
                PromMetricsAssembler.<DependencySnapshot>pending(),
                PromMetricsAssembler.<DependencySnapshot>pending(),
                PromMetricsAssembler.<DependencySnapshot>pending());
    }

    /** Kafka persist lag 미터는 OBS-17 이 연다. 그전까지 0 이 아니라 빈 값이다. */
    private static ObservedValue<PersistenceLagSummary> notWiredYet() {
        return pending();
    }

    // ── 공통 ────────────────────────────────────────────────────────────────────

    /** 규칙표에서 집계 방식을 찾아 적용합니다. 규칙 위반은 호출자가 값 단위로 처리합니다. */
    private static OptionalDouble reduceOrUnknown(String metricName, List<PromSample> samples) {
        return MetricAggregation.of(metricName).reduce(samples);
    }

    private static Predicate<PromSample> named(String metricName) {
        return sample -> metricName.equals(sample.metricName());
    }

    /** LIVE 평가 미터만 고릅니다. FINAL 판정이 같은 이름으로 오면 여기서 갈립니다. */
    private static Predicate<PromSample> live() {
        return label(PHASE_LIVE_LABEL, DomainMeterNames.PHASE_LIVE);
    }

    private static Predicate<PromSample> any() {
        return sample -> true;
    }

    private static <T> ObservedValue<T> pending() {
        return new ObservedValue<>(null, SourceStatus.PENDING, null);
    }

    private static <T> ObservedValue<T> unavailable() {
        return new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null);
    }

    private static Optional<Instant> evaluatedAt(QueryResult... results) {
        Instant latest = null;
        for (QueryResult result : results) {
            Optional<Instant> candidate = result.filter(any()).stream()
                    .map(PromSample::evaluatedAt)
                    .max(Comparator.naturalOrder());
            if (candidate.isPresent() && (latest == null || candidate.get().isAfter(latest))) {
                latest = candidate.get();
            }
        }
        return Optional.ofNullable(latest);
    }

    /** 질의 하나의 결과. 실패는 빈 목록이 아니라 별도 상태다 — 빈 목록은 PENDING 이다. */
    private record QueryResult(List<PromSample> samples, boolean unavailable) {

        static QueryResult of(List<PromSample> samples) {
            return new QueryResult(samples, false);
        }

        static QueryResult failed() {
            return new QueryResult(List.of(), true);
        }

        List<PromSample> filter(Predicate<PromSample> filter) {
            return samples.stream().filter(filter).toList();
        }
    }
}
