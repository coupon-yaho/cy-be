package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClass;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorClassKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ErrorMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyGroupStat;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyMetrics;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyPercentiles;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.Meta;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScope;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScopeType;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.InFlightSummary;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.PersistenceLagSummary;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.QueueGateMode;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.QueueMetric;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.QueueZone;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.QueueZoneSummary;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ResourceRow;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.ResourceRowSpec;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.SaturationPanel;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TopReason;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficKey;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.TrafficMetrics;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;

/**
 * Prometheus 표본을 {@link AdminMetricsResponse} 한 장으로 조립합니다.
 *
 * <p>질의는 셀렉터로 묶어 여섯 번만 보냅니다. 지표마다 한 번씩 부르면 화면의 1 초 폴링이
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
    private static final String TAG_OUTCOME = OverviewPrometheusContract.OUTCOME;
    private static final String TAG_QUANTILE = "quantile";
    // ── 지연 Timer 의 outcome 라벨 값 ──────────────────────────────────────────
    //
    // 정의는 LatencyOutcome 이고 여기는 그 라벨을 읽는 쪽이다. 계측이 쓰는 enum 을 조립기가
    // 직접 참조하면 서블릿 필터 내부 타입이 관리자 응답 조립기까지 끌려온다 — 대신 값을 여기
    // 적고, 세 상수와 저쪽 enum 이 갈리지 않는 것은 HttpLatencyOutcomeContractTest 가 지킨다.
    //
    // ⚠️ 그 테스트가 없으면 라벨 오타가 빈 결과 = PENDING 으로만 보인다. 화면에서 그것은
    //    "아직 안 만들었다" 와 똑같아 예외도 로그도 남지 않는다. 셋 다 같은 위험을 진다.
    //
    // success 는 이미 계약 상수가 있으므로 사본을 만들지 않는다 — 시계열 조립기도 같은 것을
    // 읽어, 스냅샷과 추세선이 서로 다른 문자열을 보는 일이 구조적으로 생기지 않는다.
    private static final String OUTCOME_SUCCESS = OverviewPrometheusContract.SUCCESS;
    private static final String OUTCOME_POLICY_REJECT = "policy_reject";
    private static final String OUTCOME_SYSTEM_FAILURE = "system_failure";

    /**
     * 자원 행이 볼 인스턴스. <b>batch 도 같은 미터를 냅니다</b> — CPU · heap · HikariCP 를 함께
     * 스크레이프하고 있어(실측) 라벨로 자르지 않으면 화면의 '자원 포화' 에 관측기 자신의 수치가
     * 섞입니다. 특히 DB 풀은 batch 의 관측 전용 2 커넥션 풀이 더해져 사용률이 조용히 틀립니다.
     */
    private static final String TAG_JOB = "job";
    private static final String JOB_API = "api";
    private static final String TAG_INSTANCE = "instance";

    /** 관측 전용 풀. 발급 경로 자원이 아니라 관측기 자신의 커넥션이다. */
    private static final String TAG_POOL = "pool";
    private static final String POOL_OBSERVATION = "obs-pool";

    private static final String TAG_AREA = "area";
    private static final String AREA_HEAP = "heap";

    /** 사용률 소수 자릿수. 화면이 그대로 {@code %} 로 그린다. */
    private static final double PERCENT_SCALE = 10d;

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
    private final Duration totalBudget;

    public PromMetricsAssembler(
            PromQuery client, TimeProvider timeProvider, Duration staleAfter, Duration totalBudget) {
        this.client = Objects.requireNonNull(client, "client");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.staleAfter = Objects.requireNonNull(staleAfter, "staleAfter");
        this.totalBudget = Objects.requireNonNull(totalBudget, "totalBudget");
    }

    /**
     * 요청 범위와 집계 창의 지표 스냅샷을 만듭니다.
     *
     * @param query 집계 창과 선택적 관측 범위
     * @return 값마다 상태가 붙은 한 시점 스냅샷
     */
    public AdminMetricsResponse assemble(MetricsQuery query) {
        MetricsWindow window = query.window();

        // 응답 한 장의 예산이다. 질의별 타임아웃만 두면 최악의 경우 6 × (connect + read) 가 걸려
        // 화면의 1초 폴링을 넘긴다 — 그러면 다음 폴링이 앞 요청을 따라잡아 관제가 스스로 부하가 된다.
        Deadline deadline = Deadline.startingNow(totalBudget);

        // 순서가 우선순위다. 예산이 모자라면 뒤가 잘리므로, 합격 판정을 가르는 정합성을 먼저 받는다
        // — 화면도 정합성을 KPI 첫 칸에 두어 최소한 그것만은 항상 보이게 한다.
        QueryResult consistency = run(consistencyQuery(), deadline);
        QueryResult latency = run(latencyQuery(), deadline);
        QueryResult results = run(resultRateQuery(window), deadline);
        QueryResult httpAge = run(httpFreshnessQuery(), deadline);
        // 아래 둘은 다른 값들이 다 나온 뒤에 채워도 되므로 뒤에 둔다. 예산이 모자라면
        // 이 둘부터 잘리고 정합성·지연·트래픽은 남는다.
        //
        // 자원·큐가 먼저다. 잘렸을 때 잃는 것이 더 크기 때문이다 — 이 질의가 빠지면 자원 6행 ·
        // in-flight · 큐 3영역이 통째로 UNAVAILABLE 이 되지만, 실패 원인 질의가 빠져도 실패
        // 분류 표는 살아 있다(그쪽 분자는 결과 rate 질의에서 나온다). 마지막 자리는 잃는 것이
        // 가장 적은 질의의 것이다.
        QueryResult saturation = run(saturationQuery(), deadline);
        QueryResult failureReasons =
                run(OverviewPrometheusContract.failureReasonRates(window.duration()), deadline);

        Instant evaluatedAt = evaluatedAt(latency, results, httpAge, consistency, saturation)
                .orElseGet(timeProvider::instant);
        Freshness http = httpFreshness(httpAge, evaluatedAt);
        Freshness domain = domainFreshness(consistency, evaluatedAt);
        Freshness stock = collectFreshness(consistency, DomainMeterNames.PATH_STOCK, evaluatedAt);

        // 분모는 한 번만 만들어 트래픽과 실패율이 나눠 쓴다. 각자 만들면 같은 스냅샷 안에서
        // 분자와 분모가 다른 시점을 가리킬 수 있다.
        ObservedValue<Double> attempts =
                rate(results, inGroup(UriGroup.ISSUE), http, SourceStatus.NO_TRAFFIC);
        // 처리량도 한 번만 만든다. 큐 패널의 '입장 처리' 가 이 안의 값을 그대로 다시 쓴다.
        TrafficMetrics traffic = traffic(attempts, results, http);

        return new AdminMetricsResponse(
                // 예산에 얼마나 근접했는지가 이 값으로만 보인다. 잘려서 UNAVAILABLE 이 나오기
                // 시작하면 화면이 그 원인을 여기서 읽는다.
                Meta.of(evaluatedAt, window, deadline.elapsedMillis()),
                scope(query),
                evaluatedAt,
                window,
                consistency(consistency, domain, query),
                traffic,
                latency(latency, http),
                dependencies(),
                notWiredYet(),
                List.of(),
                errors(attempts, results, http, failureReasons),
                saturation(saturation, http, stock, traffic, query));
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

    /**
     * 자원 6행 · in-flight · 큐가 쓰는 표본을 한 번에 받습니다.
     *
     * <p><b>앞쪽 셀렉터에만 {@code job="api"} 를 겁니다.</b> batch 도 CPU · heap · HikariCP 를
     * 내므로(실측) 라벨이 없으면 관측기 자신의 수치가 발급 경로 자원 행에 섞입니다. 반대로 큐
     * 길이는 batch 가 유일한 원천이라 그 라벨을 걸면 표본이 통째로 사라집니다 — 그래서 두
     * 셀렉터를 {@code or} 로 합칩니다.</p>
     *
     * <p>{@code up} 은 미터가 아니라 Prometheus 가 만드는 시계열이지만 같은 셀렉터로 함께
     * 옵니다. 값을 더하면 살아 있는 인스턴스 수가 됩니다.</p>
     */
    private static String saturationQuery() {
        return "{__name__=~\"" + String.join("|",
                MetricAggregation.CPU_USAGE,
                MetricAggregation.JVM_MEMORY_USED,
                MetricAggregation.JVM_MEMORY_MAX,
                MetricAggregation.HIKARI_ACTIVE,
                MetricAggregation.HIKARI_PENDING,
                MetricAggregation.HIKARI_MAX,
                MetricAggregation.TOMCAT_BUSY,
                MetricAggregation.TOMCAT_MAX,
                MetricAggregation.HTTP_IN_FLIGHT,
                MetricAggregation.UP) + "\"," + TAG_JOB + "=\"" + JOB_API + "\"}"
                + " or {__name__=~\"" + String.join("|",
                MetricAggregation.QUEUE_LENGTH,
                MetricAggregation.QUEUE_LENGTH_STATE,
                MetricAggregation.OBSERVED_COUPON_ID) + "\"}";
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

    /**
     * 응답 한 장에 쓸 수 있는 남은 시간. 경과는 벽시계가 아니라 단조 시계로 잰다.
     *
     * <p><b>첫 질의는 예산과 무관하게 보낸다.</b> 예산을 아무리 짧게 잡아도 응답이 통째로 비면
     * 화면에 아무것도 안 남는다 — 그래서 우선순위가 가장 높은 질의 하나는 언제나 나간다.</p>
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

    private QueryResult run(String promQl, Deadline deadline) {
        if (!deadline.allows()) {
            // 보내지 않는다. 늦게라도 값을 채우는 것보다 이 값만 비우는 편이 낫다 —
            // 응답이 폴링 간격을 넘기면 다음 폴링이 앞 요청을 따라잡는다.
            log.warn("응답 예산을 넘겨 질의를 보내지 않고 UNAVAILABLE 로 내려보냅니다: {}", promQl);
            return QueryResult.failed();
        }
        try {
            return QueryResult.of(client.query(promQl));
        } catch (PromQueryException failure) {
            // 500 으로 올리지 않는다. 이 질의가 채우려던 값만 UNAVAILABLE 로 나간다.
            //
            // ⚠️ 스택트레이스는 DEBUG 에만 싣는다. 화면이 1 초마다 부르고 한 응답에 질의가 다섯이라,
            //    Prometheus 가 죽으면 관리자 수 × 초당 5 건이 쌓인다 — 정작 장애 원인을 담은
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
        return collectFreshness(samples, DomainMeterNames.PATH_CONSISTENCY, evaluatedAt);
    }

    /**
     * 수집 경로 하나의 신선도입니다.
     *
     * <p>경로마다 주기가 다릅니다(재고 1초 · 정합성 30초). 한 경로의 시각으로 다른 경로를
     * 판정하면 느린 쪽이 늘 STALE 이거나 빠른 쪽이 늘 신선해 보입니다.</p>
     *
     * @param samples 수집 성공 시각 미터를 담고 있는 질의 결과
     * @param collectPath 수집 경로 라벨 값
     * @param evaluatedAt 응답 기준 시각
     * @return 그 경로의 관측 시각과 낡음 여부
     */
    private Freshness collectFreshness(QueryResult samples, String collectPath, Instant evaluatedAt) {
        if (samples.unavailable()) {
            return Freshness.querySourceDown();
        }
        try {
            OptionalDouble lastSuccessEpoch = reduceOrUnknown(
                    MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                    samples.filter(named(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH)
                            .and(label(DomainMeterNames.TAG_COLLECT_PATH, collectPath))));
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
    private static TrafficMetrics traffic(
            ObservedValue<Double> attempts, QueryResult results, Freshness freshness) {
        Predicate<PromSample> issue = inGroup(UriGroup.ISSUE);
        SourceStatus zeroState =
                attempts.state() == SourceStatus.NO_TRAFFIC ? SourceStatus.NO_TRAFFIC : SourceStatus.VALID;
        return new TrafficMetrics(
                attempts,
                rate(results, issue.and(result(ResultClass.SUCCESS)), freshness, zeroState),
                rate(results, result(ResultClass.QUEUE_ACCEPTED), freshness, zeroState),
                rate(results, issue.and(result(ResultClass.POLICY_REJECT)), freshness, zeroState),
                rate(results, issue.and(
                        systemFailure()),
                        freshness, zeroState));
    }

    private static Predicate<PromSample> inGroup(UriGroup group) {
        return label(TAG_URI_GROUP, group.tagValue());
    }

    /**
     * 실패 비율의 분자가 되는 결과 분류입니다. 정의는 {@link ResultClass#systemFailures()} 하나뿐이라
     * 시계열 경로({@code PromSeriesAssembler})와 같은 값을 셉니다.
     */
    private static Predicate<PromSample> systemFailure() {
        return sample -> ResultClass.systemFailures().stream()
                .anyMatch(failure -> result(failure).test(sample));
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
        if (reduced.getAsDouble() < 0d) {
            // counter 의 rate 는 음수가 될 수 없다. 음수가 왔다면 원천이 망가진 것이지 관측이 아니다.
            //
            // 여기서 막는다 — 처리량과 실패율이 같은 값을 나눠 쓰므로, 한쪽에만 걸면 같은 스냅샷에서
            // 처리량은 음수 값을 VALID 로 그리고 실패율만 UNAVAILABLE 이 되는 모순이 생긴다.
            //
            // ⚠️ 로그를 남기지 않는다. 화면이 1 초마다 부르고 이 헬퍼는 응답 한 장에 아홉 번
            //    불리므로, 원천이 망가진 동안 관리자 수 × 초당 아홉 줄이 쌓인다 — 정작 원인을 담은
            //    다른 로그가 묻힌다. 값이 UNAVAILABLE 로 나가는 것 자체가 신호다.
            return unavailable();
        }
        SourceStatus state = freshness.stale()
                ? SourceStatus.STALE
                : (reduced.getAsDouble() == 0d ? zeroState : SourceStatus.VALID);
        return new ObservedValue<>(reduced.getAsDouble(), state, freshness.observedAt());
    }

    /**
     * 발급 시도 대비 실패 비율과 실패 사유별 발생률입니다.
     *
     * <p><b>{@code traffic()} 과 같은 표본을 접습니다.</b> 질의를 따로 보내면 분자와 분모가 다른
     * 시점을 보고, 그러면 실패율이 100% 를 넘거나 음수가 되는 순간이 생깁니다. 분모도 조립기가
     * 한 번 만든 {@code issueAttemptRps} 를 그대로 받습니다.</p>
     *
     * <p><b>{@code QUEUE_ACCEPTED} 와 달리 모든 분류에 {@code uri_group=issue} 를 겁니다.</b>
     * 대기열 경로는 발급 시도가 아니므로 분모에도 분자에도 들어가면 안 됩니다.</p>
     *
     * <p>상태는 블록이 아니라 값마다 붙습니다. 원인 질의가 죽어도 분류 표는 삽니다.</p>
     */
    private static ErrorMetrics errors(
            ObservedValue<Double> attempts,
            QueryResult results,
            Freshness freshness,
            QueryResult failureReasons) {
        Predicate<PromSample> issue = inGroup(UriGroup.ISSUE);
        return new ErrorMetrics(
                TrafficKey.ISSUE_ATTEMPT_RPS,
                List.of(
                        errorClass(ErrorClassKey.DEPENDENCY_FAILURE, "의존성 실패",
                                "httpStatus >= 500 && dependency != NONE", false,
                                results, issue.and(result(ResultClass.DEPENDENCY_FAILURE)),
                                freshness, attempts),
                        errorClass(ErrorClassKey.APPLICATION_FAILURE, "애플리케이션 실패",
                                "httpStatus >= 500 && dependency == NONE", false,
                                results, issue.and(result(ResultClass.APPLICATION_FAILURE)),
                                freshness, attempts),
                        // 요청 계약 위반이다. 서버가 실패한 것이 아니므로 분자에서 뺀다.
                        errorClass(ErrorClassKey.CLIENT_INVALID, "클라이언트 요청 오류",
                                "4xx 중 403·409 를 뺀 나머지 (400·401·404·422·429)", true,
                                results, issue.and(result(ResultClass.CLIENT_INVALID)),
                                freshness, attempts),
                        // 설계된 동작이다. 분자에 넣으면 재고 소진 구간에서 실패율이 100% 에 붙는다.
                        errorClass(ErrorClassKey.POLICY_REJECT, "정책 거절",
                                "의도된 403 · 409", true,
                                results, issue.and(result(ResultClass.POLICY_REJECT)),
                                freshness, attempts)),
                topReasons(failureReasons, freshness));
    }

    private static ErrorClass errorClass(
            ErrorClassKey key,
            String label,
            String definition,
            boolean excludedFromNumerator,
            QueryResult results,
            Predicate<PromSample> filter,
            Freshness freshness,
            ObservedValue<Double> attempts) {
        // 분자가 0 인 것은 "요청이 없었다" 가 아니라 "그 실패가 한 건도 없었다" 이므로 VALID 다.
        ObservedValue<Double> numerator = rate(results, filter, freshness, SourceStatus.VALID);
        return new ErrorClass(key, label, definition, excludedFromNumerator,
                failureRate(numerator, attempts));
    }

    /**
     * 분모 대비 백분율입니다.
     *
     * <p><b>분모가 0 이면 비율이 없습니다.</b> 0 으로 내려보내면 "실패가 없다" 로 읽히고
     * {@code NO_TRAFFIC} 으로 내려보내면 값이 0 인 것으로 읽힙니다 — 요청이 0 건일 때 비율은
     * 정의되지 않는 것이라 {@code N_A} 입니다.</p>
     *
     * <p><b>음수 표본은 여기까지 오지 않습니다.</b> {@link #rate} 가 이미 {@code UNAVAILABLE} 로
     * 바꿔 놓으므로 분자·분모 어느 쪽이 음수든 첫 분기에서 걸립니다 — 계약({@code 0~100})을
     * 지킬 수 없는 입력을 나누는 자리가 아예 없습니다.</p>
     *
     * <p><b>자르는 코드가 없는 것은 자를 일이 없기 때문입니다.</b> 분자와 분모는 같은
     * {@code QueryResult} 를 같은 {@code uri_group} 필터로 접은 것이라 분자가 분모의 부분집합이고,
     * 네 분류는 {@code ResultClass} 로 서로 겹치지 않습니다 — 넷을 다 더해도 100 을 넘지 않습니다.
     * 그 성질이 깨지면(필터가 그룹 밖을 집거나 두 분류가 겹치면) 100 을 넘는 값이 그대로
     * 드러나야 합니다. 잘라 두면 그 배선 오류가 화면에서 정상으로 보입니다.</p>
     */
    private static ObservedValue<Double> failureRate(
            ObservedValue<Double> numerator, ObservedValue<Double> attempts) {
        if (numerator.state() == SourceStatus.UNAVAILABLE
                || attempts.state() == SourceStatus.UNAVAILABLE) {
            return unavailable();
        }
        // 분모부터 본다. 분모가 0 이면 분자를 알든 모르든 비율은 정의되지 않는다.
        if (!attempts.state().carriesValue()) {
            return pending();
        }
        if (attempts.value() == 0d) {
            return notApplicable();
        }
        if (!numerator.state().carriesValue()) {
            // 그 결과의 시계열이 아직 없다. 0 이 아니라 모르는 것이다.
            return pending();
        }
        double percent = round(numerator.value() / attempts.value() * 100d);
        SourceStatus state = numerator.state() == SourceStatus.STALE
                || attempts.state() == SourceStatus.STALE
                ? SourceStatus.STALE
                : SourceStatus.VALID;
        return new ObservedValue<>(percent, state, numerator.observedAt());
    }

    /**
     * 사유별 초당 실패 건수입니다. 원천이 {@code traffic()} 과 다릅니다 — 저쪽은 응답 상태로
     * 나눈 HTTP 결과이고 이쪽은 업무 사유({@code ReasonCode})입니다.
     *
     * <p><b>0 인 행을 여기서 거르지 않고 상위 N 개로 자르지도 않습니다.</b> 무엇이 0 인지 보이는
     * 편이 나은지, 몇 줄까지 보일지는 패널이 정할 문제이고, 서버가 걸러 버리면 화면에는 고를
     * 자유가 남지 않습니다.</p>

     * <p>⚠️ <b>0 인 행과 목록에 아예 없는 행은 다릅니다.</b> 카운터가 한 번도 등록되지 않아
     * 시계열 자체가 없으면 그 사유는 행으로도 안 나옵니다 — 등록은 기동 시점에
     * {@code CampaignMeterRegistry} 가 사유 전종에 대해 하므로 정상 배선에서는 안 생깁니다.</p>
     *
     * <p><b>음수 표본이 하나라도 있으면 표를 통째로 비웁니다({@code UNAVAILABLE}).</b> 그 행만
     * 빼면 남은 행의 순위가 멀쩡한 것처럼 보이는데, 원천이 음수를 낼 정도면 나머지 값도 믿을
     * 근거가 없습니다. {@link #rate} 가 처리량·실패율에 거는 것과 같은 판정입니다.</p>
     *
     * <p>신선도는 HTTP 미터의 것을 씁니다. 같은 JVM 의 같은 scrape 로 나오는 미터라 나이가
     * 같습니다 — 따로 물으면 질의만 하나 더 늘고 답은 같습니다.</p>
     */
    private static ObservedValue<List<TopReason>> topReasons(
            QueryResult failureReasons, Freshness freshness) {
        if (failureReasons.unavailable()) {
            return unavailable();
        }
        List<TopReason> rows = new ArrayList<>();
        for (ReasonCode reasonCode : OverviewPrometheusContract.FAILURE_REASONS) {
            OptionalDouble reduced = reduceOrUnknown(MetricAggregation.ISSUANCE_OUTCOME_TOTAL,
                    failureReasons.filter(label(TAG_OUTCOME, reasonCode.name())));
            if (reduced.isEmpty()) {
                continue;
            }
            if (reduced.getAsDouble() < 0d) {
                // 실패율과 같은 판정이다. counter 의 rate 는 음수일 수 없으므로 원천이 망가진 것이고,
                // 한 행만 빼면 나머지 순위가 그대로인 것처럼 보인다 — 표를 통째로 비운다.
                return unavailable();
            }
            rows.add(new TopReason(reasonCode, round(reduced.getAsDouble())));
        }
        if (rows.isEmpty()) {
            // 시계열이 아직 하나도 없다. "실패 없음" 과 다르다 — 빈 목록으로 내려보내면 둘이 같아진다.
            return pending();
        }
        if (!freshness.known()) {
            return absent(freshness);
        }
        rows.sort(Comparator.comparingDouble(TopReason::rps).reversed()
                .thenComparing(row -> row.reasonCode().name()));
        return new ObservedValue<>(List.copyOf(rows),
                freshness.stale() ? SourceStatus.STALE : SourceStatus.VALID, freshness.observedAt());
    }

    /** 화면이 세 자리까지 읽습니다. 그 아래는 폴링마다 흔들리는 잡음이라 값이 아닙니다. */
    private static double round(double value) {
        return Math.round(value * 1000d) / 1000d;
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
     * <p>{@code groups} 는 같은 성공 경로를 URI 그룹 다섯 종으로 편 것입니다. 미터는 이미 그룹마다
     * Timer 를 등록하고 있어(HttpMetrics) 계측도 질의도 늘지 않습니다 — 조립기가 ISSUE 하나만
     * 골라 오던 것을 그만둘 뿐입니다. <b>표본이 없는 그룹도 목록에서 빼지 않습니다</b>: 이유는
     * 값이 아니라 상태가 냅니다.</p>
     *
     * <p><b>[OBS-31] 정책 거절과 시스템 실패가 실값을 냅니다.</b> {@code HttpMetrics} 가
     * {@code outcome} 을 넷으로 등록하게 되어(success · policy_reject · client_invalid ·
     * system_failure) 실패 안에서 두 경로가 더는 섞이지 않습니다. 질의는 늘지 않았습니다 —
     * 같은 질의가 받아 오던 표본을 라벨로 갈라 볼 뿐입니다.</p>
     *
     * <p><b>{@code client_invalid} 는 등록만 하고 여기서 내보내지 않습니다.</b> 응답 계약에
     * 자리가 없고, 자리를 만드는 것은 프론트와 합의할 항목입니다. 계측을 먼저 해 두는 이유는
     * 세지 않은 지연은 소급해서 만들 수 없기 때문입니다.</p>
     *
     * <p><b>세 필드 모두 ISSUE 그룹입니다.</b> {@code groups} 가 그룹 축을 이미 펴고 있어
     * 여기서 그룹을 합치면 같은 값이 두 축으로 나가고, 합친 백분위는 애초에 만들 수 없습니다
     * (인스턴스별로 계산된 값이라 평균·합산 불가).</p>
     */
    private static LatencyMetrics latency(QueryResult latency, Freshness freshness) {
        List<LatencyGroupStat> groups = new ArrayList<>();
        for (UriGroup group : UriGroup.values()) {
            groups.add(new LatencyGroupStat(group.tagValue(), successPercentiles(latency, group, freshness)));
        }
        return new LatencyMetrics(
                successPercentiles(latency, UriGroup.ISSUE, freshness),
                outcomePercentiles(latency, UriGroup.ISSUE, OUTCOME_POLICY_REJECT, freshness),
                outcomePercentiles(latency, UriGroup.ISSUE, OUTCOME_SYSTEM_FAILURE, freshness),
                List.copyOf(groups));
    }

    /**
     * 한 URI 그룹의 성공 경로 백분위입니다.
     *
     * <p><b>{@code latency.success} 와 {@code groups} 의 issue 항목이 같은 헬퍼를 부릅니다.</b>
     * 필터를 두 번 쓰면 한쪽만 고쳐질 수 있고, 그러면 화면이 같은 수치를 두 자리에서 다르게
     * 읽습니다.</p>
     */
    private static ObservedValue<LatencyPercentiles> successPercentiles(
            QueryResult latency, UriGroup group, Freshness freshness) {
        return outcomePercentiles(latency, group, OUTCOME_SUCCESS, freshness);
    }

    /**
     * 한 URI 그룹의 한 지연 축 백분위입니다.
     *
     * <p><b>네 축이 전부 이 헬퍼 하나를 지납니다.</b> 실패 경로가 자기 필터를 따로 만들면 성공
     * 경로와 어긋날 수 있고, 그러면 같은 화면의 네 자리가 서로 다른 규칙으로 표본을 고릅니다 —
     * 그게 {@link #successPercentiles} 가 처음부터 경고하던 것과 같은 사고입니다.</p>
     */
    private static ObservedValue<LatencyPercentiles> outcomePercentiles(
            QueryResult latency, UriGroup group, String outcome, Freshness freshness) {
        return percentiles(
                latency, inGroup(group).and(label(TAG_OUTCOME, outcome)), freshness);
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
        // TODO(OBS-44): 표본 없음과 트래픽 없음이 여기서 같은 PENDING 으로 합쳐진다. 가르려면
        // 결과 Counter 의 rate 가 필요한데 이 메서드는 지연 원천만 본다. NO_TRAFFIC 은
        // carriesValue()==true 라 값 없이 낼 수 없고, 실을 값이 (0,0,0) 뿐이라 바로 위 금지
        // 규칙과 정면으로 부딪친다 — 상태·값 규약부터 정해야 하는 일이라 따로 뗐다.
        return new ObservedValue<>(
                new LatencyPercentiles(millis(p50), millis(p95), millis(p99)),
                freshness.stale() ? SourceStatus.STALE : SourceStatus.VALID,
                freshness.observedAt());
    }

    /**
     * 인스턴스 최댓값을 냅니다(DEC-02).
     *
     * <p><b>{@code instance} 라벨이 없는 표본은 버립니다.</b> 어느 대의 백분위인지 모르는 값을
     * 최댓값 후보로 넣으면 어느 인스턴스의 것도 아닌 지연이 VALID 로 나갑니다 — 값이 정상
     * 범위라 예외도 로그도 남지 않습니다. CY-449 가 자원 사용률에서 내린 것과 같은 판단이고
     * 같은 형태입니다({@link #percent} 의 byInstance 그룹핑).</p>
     *
     * <p>반대 방향 대가 — 원천이 instance 라벨을 떨구면(federation · recording rule) 지연이
     * 값을 못 내고 PENDING 으로 굳습니다. 틀린 값을 권위 있게 내보내는 것보다 낫다고 봅니다.</p>
     */
    private static OptionalDouble quantile(List<PromSample> samples, String quantile) {
        return reduceOrUnknown(MetricAggregation.HTTP_LATENCY_SECONDS,
                samples.stream()
                        .filter(label(TAG_QUANTILE, quantile))
                        .filter(sample -> !sample.label(TAG_INSTANCE).isEmpty())
                        .toList());
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
        return pairedLong(samples, valueMetric, stateMetric, filter, freshness, query,
                MetricAggregation.CONSISTENCY_COUPON_ID);
    }

    /**
     * 값·상태 짝을 읽되 <b>회차 판별에 쓸 미터를 지정</b>합니다.
     *
     * <p>회차 식별자 미터는 수집 경로마다 따로 있습니다({@code consistency.coupon.id} ·
     * {@code observation.coupon.id}) — 주기가 달라 회차가 바뀌는 순간 둘이 잠시 어긋나기
     * 때문입니다. 한쪽 미터로 다른 경로의 값을 판별하면 그 창에서 범위 판정이 틀립니다.
     */
    private ObservedValue<Long> pairedLong(
            QueryResult samples, String valueMetric, String stateMetric,
            Predicate<PromSample> filter, Freshness freshness, MetricsQuery query, String scopeMetric) {
        if (samples.unavailable()) {
            return unavailable();
        }
        try {
            if (outOfScope(samples, query, scopeMetric)) {
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
    private static boolean outOfScope(QueryResult samples, MetricsQuery query, String scopeMetric) {
        if (query.couponId() == null) {
            return false;
        }
        OptionalDouble observedCoupon =
                reduceOrUnknown(scopeMetric, samples.filter(named(scopeMetric)));
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

    // ── 자원 포화 ───────────────────────────────────────────────────────────────

    /**
     * 자원 6행 · in-flight · 큐 3영역입니다.
     *
     * <p><b>행은 원천이 없어도 사라지지 않습니다.</b> 행을 지우면 화면이 "그 자원은 재 봤는데
     * 여유" 로 읽습니다 — 값이 없는 것과 여유로운 것은 다른 사건이라 자리는 두고 상태로 가릅니다.</p>
     *
     * <p>신선도는 HTTP 미터의 것을 씁니다. 자원 미터는 같은 api 인스턴스에서 같은 scrape 로
     * 올라오므로 나이가 같습니다. 큐만 batch 의 재고 수집 경로 시각을 씁니다 — 주기가 달라
     * HTTP 시각으로 판정하면 늘 낡아 보입니다.</p>
     */
    private SaturationPanel saturation(QueryResult samples, Freshness http, Freshness stock,
                                       TrafficMetrics traffic, MetricsQuery query) {
        return new SaturationPanel(
                resources(samples, http),
                inFlight(samples, http),
                queues(samples, stock, traffic, query),
                SaturationPanel.THRESHOLDS);
    }

    private static List<ResourceRow> resources(QueryResult samples, Freshness freshness) {
        return List.of(
                row(ResourceRowSpec.HIKARI, hikariDetail(samples),
                        percent(samples, freshness, MetricAggregation.HIKARI_POOL_UTILIZATION,
                                named(MetricAggregation.HIKARI_ACTIVE).and(issuancePool()),
                                MetricAggregation.SUM,
                                named(MetricAggregation.HIKARI_MAX).and(issuancePool()),
                                MetricAggregation.SUM)),
                row(ResourceRowSpec.TOMCAT, ResourceRowSpec.TOMCAT.detail(),
                        percent(samples, freshness, MetricAggregation.TOMCAT_THREAD_UTILIZATION,
                                named(MetricAggregation.TOMCAT_BUSY), MetricAggregation.SUM,
                                named(MetricAggregation.TOMCAT_MAX), MetricAggregation.SUM)),
                row(ResourceRowSpec.CPU, ResourceRowSpec.CPU.detail(), cpuPercent(samples, freshness)),
                row(ResourceRowSpec.HEAP, ResourceRowSpec.HEAP.detail(),
                        percent(samples, freshness, MetricAggregation.JVM_HEAP_UTILIZATION,
                                named(MetricAggregation.JVM_MEMORY_USED).and(heapArea()),
                                MetricAggregation.SUM,
                                // 영역 상한은 더하지 않는다. G1 은 Eden · Survivor 에 -1 을 싣고
                                // Old Gen 하나만 힙 전체 상한을 낸다(실측).
                                named(MetricAggregation.JVM_MEMORY_MAX).and(heapArea()),
                                MetricAggregation.MAX)),
                // 아래 둘은 사용률의 분모가 없다. 0 으로 채우면 "여유" 를 그린다.
                row(ResourceRowSpec.REDIS, ResourceRowSpec.REDIS.detail(), notApplicable()),
                row(ResourceRowSpec.DISK_NETWORK, ResourceRowSpec.DISK_NETWORK.detail(),
                        notApplicable()));
    }

    private static ResourceRow row(ResourceRowSpec spec, String detail, ObservedValue<Double> utilization) {
        return new ResourceRow(spec.name(), detail, utilization, spec.warnAt());
    }

    /**
     * DB 풀 행의 보조 문구입니다. <b>대기 개수가 진짜 신호</b>라 사용률과 함께 보여야 합니다 —
     * 사용률이 79% 여도 pending 이 쌓이고 있으면 이미 무너지는 중입니다.
     *
     * <p>{@code detail} 에는 상태를 실을 자리가 없어 값이 없으면 기본 문구로 물러납니다.
     * TODO(OBS-9 후속): {@code ResourceRow.pending} 자리를 프론트와 합의하면 문자열이 아니라
     * 상태 있는 값으로 옮긴다.</p>
     */
    private static String hikariDetail(QueryResult samples) {
        if (samples.unavailable()) {
            return ResourceRowSpec.HIKARI.detail();
        }
        OptionalDouble pending = reduceOrUnknown(MetricAggregation.HIKARI_PENDING,
                samples.filter(named(MetricAggregation.HIKARI_PENDING).and(issuancePool())));
        return pending.isEmpty()
                ? ResourceRowSpec.HIKARI.detail()
                : "pending " + Math.round(pending.getAsDouble());
    }

    /** CPU 는 이미 0~1 비율이라 인스턴스 안에서 나눌 것이 없다. 곧바로 인스턴스 최댓값을 쓴다. */
    private static ObservedValue<Double> cpuPercent(QueryResult samples, Freshness freshness) {
        if (samples.unavailable()) {
            return unavailable();
        }
        OptionalDouble ratio = reduceOrUnknown(MetricAggregation.CPU_USAGE,
                samples.filter(named(MetricAggregation.CPU_USAGE)));
        return observedPercent(ratio.isEmpty() ? ratio : OptionalDouble.of(ratio.getAsDouble() * 100d),
                freshness);
    }

    /**
     * 인스턴스 안에서 먼저 나누고, 그 비율들을 규칙표대로 줄입니다.
     *
     * <p><b>순서를 뒤집으면 안 됩니다.</b> 인스턴스를 먼저 합치면 정원이 다른 대가 섞여 한 대의
     * 포화가 다른 대의 여유에 희석됩니다. 여기서 쓰는 SUM·MAX 는 규칙표의 <b>인스턴스 축약</b>이
     * 아니라 인스턴스 <b>안에서</b> 라벨을 접는 단계입니다 — 축약은 마지막 한 번뿐입니다.</p>
     */
    private static ObservedValue<Double> percent(
            QueryResult samples, Freshness freshness, String derivedKey,
            Predicate<PromSample> numerator, MetricAggregation numeratorFold,
            Predicate<PromSample> denominator, MetricAggregation denominatorFold) {
        if (samples.unavailable()) {
            return unavailable();
        }
        Map<String, List<PromSample>> byInstance = new LinkedHashMap<>();
        for (PromSample sample : samples.filter(any())) {
            String instance = sample.label(TAG_INSTANCE);
            if (instance.isEmpty()) {
                // 어느 대의 값인지 모르면 인스턴스 안에서 나눌 수 없다. 그냥 두면 라벨 없는
                // 표본끼리 짝이 맞는 순간 <b>어느 인스턴스의 것도 아닌 사용률</b>이 VALID 로
                // 나간다(실측 80%). 값이 정상 범위라 화면에서는 드러나지 않는다.
                //
                // 반대 방향 대가 — 원천이 instance 라벨을 떨구면(federation·recording rule)
                // 자원 행이 값을 못 내고 PENDING 으로 굳는다. 틀린 값을 권위 있게 내보내는
                // 것보다 낫다고 보고 이쪽을 고른다.
                continue;
            }
            byInstance.computeIfAbsent(instance, ignored -> new ArrayList<>()).add(sample);
        }

        List<PromSample> ratios = new ArrayList<>();
        for (Map.Entry<String, List<PromSample>> entry : byInstance.entrySet()) {
            OptionalDouble used = numeratorFold.reduce(entry.getValue().stream().filter(numerator).toList());
            OptionalDouble size =
                    denominatorFold.reduce(entry.getValue().stream().filter(denominator).toList());
            if (used.isEmpty() || size.isEmpty() || size.getAsDouble() <= 0d) {
                // 정원을 모르면 비율이 아니다. 0 으로 두면 그 인스턴스가 가장 여유로운 대로 보인다.
                continue;
            }
            ratios.add(new PromSample(derivedKey, Map.of(TAG_INSTANCE, entry.getKey()),
                    used.getAsDouble() / size.getAsDouble() * 100d,
                    entry.getValue().get(0).evaluatedAt()));
        }
        return observedPercent(reduceOrUnknown(derivedKey, ratios), freshness);
    }

    private static ObservedValue<Double> observedPercent(OptionalDouble percent, Freshness freshness) {
        if (percent.isEmpty()) {
            return pending();
        }
        if (!freshness.known()) {
            return absent(freshness);
        }
        return new ObservedValue<>(
                Math.round(percent.getAsDouble() * PERCENT_SCALE) / PERCENT_SCALE,
                freshness.stale() ? SourceStatus.STALE : SourceStatus.VALID,
                freshness.observedAt());
    }

    /**
     * 처리 중인 요청 수입니다. <b>스레드가 아니라 요청을 셉니다</b> — 라우트가 없는 요청도 세므로
     * {@code tomcat.threads.busy} 와 같은 값이 아닙니다.
     *
     * <p>{@code mode} · {@code admitThreshold} · {@code releaseThreshold} 는 ADAPTIVE 대기열의
     * 런타임 설정이고 이 API 에 배선돼 있지 않습니다. 화면 계약이 이 셋을 {@code SourceValue} 가
     * 아닌 생값으로 받아 "모름" 을 실을 자리가 없어 자리만 채웁니다.
     * TODO(OBS-19 후속): ConfigStore 가 열리면 실제 설정을 싣는다. 그전까지 이 셋은 판정 근거가
     * 아니다.</p>
     */
    private static InFlightSummary inFlight(QueryResult samples, Freshness freshness) {
        List<PromSample> inFlightSamples =
                samples.unavailable() ? List.of() : samples.filter(named(MetricAggregation.HTTP_IN_FLIGHT));
        ObservedValue<Double> globalSum = samples.unavailable()
                ? unavailable()
                : observed(reduceOrUnknown(MetricAggregation.HTTP_IN_FLIGHT, inFlightSamples), freshness);
        ObservedValue<Double> instanceMax = samples.unavailable()
                ? unavailable()
                : observed(reduceOrUnknown(
                        MetricAggregation.HTTP_IN_FLIGHT_INSTANCE_MAX, inFlightSamples), freshness);
        return new InFlightSummary(
                globalSum,
                instanceMax,
                busiestInstance(inFlightSamples),
                activeInstances(samples),
                QueueGateMode.OFF,
                0,
                0,
                List.of());
    }

    /** 최댓값을 낸 인스턴스. 모르면 빈 문자열이다 — 아무 인스턴스나 고르면 화면이 그 대를 지목한다. */
    private static String busiestInstance(List<PromSample> inFlightSamples) {
        return inFlightSamples.stream()
                .filter(PromSample::hasNumericValue)
                .max(Comparator.comparingDouble(PromSample::value))
                .map(sample -> sample.label(TAG_INSTANCE))
                .orElse("");
    }

    /**
     * {@code up==1} 인 인스턴스 수입니다. 죽은 인스턴스의 마지막 값이 합에 남아 있는 동안 화면이
     * "활성 3/4" 로 그 사실을 드러내는 근거입니다.
     */
    private static int activeInstances(QueryResult samples) {
        if (samples.unavailable()) {
            return 0;
        }
        OptionalDouble up = reduceOrUnknown(MetricAggregation.UP,
                samples.filter(named(MetricAggregation.UP)));
        return up.isEmpty() ? 0 : (int) Math.round(up.getAsDouble());
    }

    private static ObservedValue<Double> observed(OptionalDouble value, Freshness freshness) {
        if (value.isEmpty()) {
            return pending();
        }
        if (!freshness.known()) {
            return absent(freshness);
        }
        return new ObservedValue<>(value.getAsDouble(),
                freshness.stale() ? SourceStatus.STALE : SourceStatus.VALID, freshness.observedAt());
    }

    /**
     * 큐 3영역입니다. <b>합치지 않습니다</b> — 대기 인원은 사람, 저장 대기는 메시지, 관측 지연은
     * 시간이라 한 축에 겹쳐 그리면 어느 것도 해석할 수 없습니다.
     *
     * <p>Persistence 는 Kafka consumer lag 이 원천이라 OBS-15 · OBS-35 뒤입니다. Telemetry 는
     * 정의가 "이벤트 시각 ↔ <b>화면 수신</b> 시각" 이라 서버가 잴 수 없습니다. 둘 다 0 이 아니라
     * PENDING 입니다 — 원천이 없는 것과 큐가 빈 것은 다른 사건입니다.</p>
     */
    private List<QueueZoneSummary> queues(
            QueryResult samples, Freshness stock, TrafficMetrics traffic, MetricsQuery query) {
        ObservedValue<Double> waiting = toDouble(pairedLong(samples,
                MetricAggregation.QUEUE_LENGTH, MetricAggregation.QUEUE_LENGTH_STATE,
                any(), stock, query, MetricAggregation.OBSERVED_COUPON_ID));
        return List.of(
                zone(QueueZone.ADMISSION, List.of(
                        waiting,
                        // 같은 값을 두 번 재지 않는다. 입장 처리율은 트래픽 패널이 이미 낸 값이다.
                        traffic.queueAcceptedRps(),
                        // 추세는 한 시점 질의로 만들 수 없다. range 질의는 OBS-34 가 연다.
                        pending(),
                        pending())),
                zone(QueueZone.PERSISTENCE, List.of(pending(), pending(), pending(), pending())),
                zone(QueueZone.TELEMETRY, List.of(pending())));
    }

    private static QueueZoneSummary zone(QueueZone zone, List<ObservedValue<Double>> values) {
        List<String> labels = QueueMetric.labelsOf(zone);
        List<QueueMetric> metrics = new ArrayList<>();
        for (int i = 0; i < labels.size(); i++) {
            metrics.add(new QueueMetric(labels.get(i), values.get(i), QueueMetric.unitOf(labels.get(i))));
        }
        return new QueueZoneSummary(zone, List.copyOf(metrics), List.of());
    }

    private static ObservedValue<Double> toDouble(ObservedValue<Long> value) {
        return new ObservedValue<>(
                value.value() == null ? null : value.value().doubleValue(),
                value.state(), value.observedAt());
    }

    private static Predicate<PromSample> issuancePool() {
        return sample -> !POOL_OBSERVATION.equals(sample.label(TAG_POOL));
    }

    private static Predicate<PromSample> heapArea() {
        return label(TAG_AREA, AREA_HEAP);
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

    /** 값이 없는 것이 아니라 그 값이 정의되지 않는 상태입니다. */
    private static <T> ObservedValue<T> notApplicable() {
        return new ObservedValue<>(null, SourceStatus.N_A, null);
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
