package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.ReasonCode;

/** Overview 관측이 사용하는 B 소유 scrape 이름·라벨·PromQL을 한곳에 모읍니다. */
public final class OverviewPrometheusContract {

    public static final String FLOW_TOTAL = "app_issuance_flow_total";
    public static final String OUTCOME_TOTAL = MetricAggregation.ISSUANCE_OUTCOME_TOTAL;
    public static final String LAST_SUCCESS_EPOCH = "app_issuance_event_last_success_epoch";
    public static final String HTTP_LATENCY_SECONDS = MetricAggregation.HTTP_LATENCY_SECONDS;
    public static final String ATTEMPT_PUBLISH_FAILURES_TOTAL =
            MetricAggregation.promName(DomainMeterNames.KAFKA_ATTEMPT_PUBLISH_FAILURES) + "_total";
    public static final String COUPON_ID = "coupon_id";
    public static final String STAGE = "stage";
    public static final String OUTCOME = "outcome";
    public static final String URI_GROUP = "uri_group";
    public static final String QUANTILE = "quantile";
    public static final String ATTEMPT = "attempt";
    public static final String SUCCESS = "success";
    /**
     * 실패 원인 Top N 에 실을 사유 코드입니다.
     *
     * <p>{@code app_issuance_outcome_total} 의 {@code outcome} 에는 발급 결과가 전부 들어옵니다 —
     * {@code ISSUED}·{@code QUEUED} 와 정책 거절 사유까지. 정책 거절을 원인 표에 넣으면 재고 소진
     * 구간에서 {@code STOCK_EXHAUSTED} 가 표를 통째로 밀어내 <b>실제 장애 원인이 상위 5행 밖으로
     * 밀립니다</b> — 실패율 분자에서 정책 거절을 빼는 것과 같은 이유입니다.</p>
     *
     * <p><b>{@code EnumSet} 을 감싼 것이지 {@code Set.copyOf} 가 아닙니다.</b> 순회 순서가
     * 셀렉터 문자열이 되므로 순서가 없는 Set 으로 바꾸면 같은 질의가 매 기동마다 다른 문자열이
     * 됩니다 — Prometheus 쪽 질의 캐시와 로그 대조가 그때부터 안 맞습니다.</p>
     *
     * <p>분류는 {@link #isFailure(ReasonCode)} 의 <b>default 없는 switch</b> 가 정합니다. 사유 코드가
     * 늘면 여기서 컴파일이 깨집니다 — 조용히 "실패 아님" 으로 떨어지면 새 장애 사유가 표에서
     * 영영 안 보입니다.</p>
     */
    public static final Set<ReasonCode> FAILURE_REASONS = Collections.unmodifiableSet(
            EnumSet.copyOf(Arrays.stream(ReasonCode.values())
                    .filter(OverviewPrometheusContract::isFailure)
                    .toList()));

    /** 인스턴스화를 막습니다. */
    private OverviewPrometheusContract() { }

    /**
     * 이 사유가 서버 실패인지 판정합니다.
     *
     * @param reasonCode 발급 결과 사유
     * @return 서버 실패이면 true, 정책 거절·클라이언트 요청 오류이면 false
     */
    private static boolean isFailure(ReasonCode reasonCode) {
        return switch (reasonCode) {
            case TEMPORARILY_UNAVAILABLE, INTERNAL_ERROR, UNMAPPED,
                    // v2 의 사고 넷. 정상 운영에서 전부 0 이라 표에 뜨면 그게 원인이다.
                    VALUE_CORRUPT, GATE_NOT_READY, BAD_ARGUMENT, COUNTER_UNREADABLE -> true;
            // 403·409 로 나가는 설계된 거절과 대기열 계약 위반이다. 실패가 아니다.
            case NOT_OPENED, COUPON_ROUND_CLOSED, GRADE_NOT_ELIGIBLE, QUEUE_REQUIRED, NO_ENTRY_TOKEN,
                    ENTRY_TOKEN_EXPIRED, ALREADY_ISSUED, STOCK_EXHAUSTED,
                    INVALID_TRANSITION,
                    // 멱등 재시도다. 장애가 아니라 클라이언트가 다시 누른 것이다.
                    REPLAY_IN_PROGRESS -> false;
        };
    }

    /**
     * 지정 집계 구간의 사유별 <b>초당</b> 실패 건수 질의를 만듭니다.
     *
     * <p>셀렉터와 응답을 접는 쪽이 같은 {@link #FAILURE_REASONS} 를 씁니다. 두 곳에 목록을 따로
     * 적으면 한쪽만 늘었을 때 예외 없이 행 하나가 사라집니다.</p>
     *
     * @param window 비율을 계산할 집계 창
     * @return 사유별 초당 건수 질의
     */
    public static String failureReasonRates(Duration window) {
        String selector = FAILURE_REASONS.stream().map(Enum::name).collect(Collectors.joining("|"));
        return "sum by (" + OUTCOME + ") (rate(" + OUTCOME_TOTAL
                + "{" + OUTCOME + "=~\"" + selector + "\"}[" + promDuration(window) + "]))";
    }

    /** @return 현재 1분 attempt·success 쿠폰 회차별 증가량 질의 */
    public static String currentFlow() {
        return currentFlow(OverviewPrometheusProperties.defaults().currentWindow());
    }

    /** 지정 집계 구간을 사용하는 attempt·success 쿠폰 회차별 증가량 질의를 만듭니다. */
    public static String currentFlow(Duration currentWindow) {
        return "sum by (" + COUPON_ID + ", " + STAGE + ") (increase(" + FLOW_TOTAL
                + "{" + STAGE + "=~\"" + ATTEMPT + "|" + SUCCESS + "\"}["
                + promDuration(currentWindow) + "]))";
    }

    /** @return 직전 1분 success 쿠폰 회차별 증가량 질의 */
    public static String comparisonSuccess() {
        OverviewPrometheusProperties defaults = OverviewPrometheusProperties.defaults();
        return comparisonSuccess(defaults.currentWindow(), defaults.comparisonOffset());
    }

    /** 지정 집계 구간과 offset을 사용하는 직전 success 증가량 질의를 만듭니다. */
    public static String comparisonSuccess(Duration currentWindow, Duration comparisonOffset) {
        return "sum by (" + COUPON_ID + ") (increase(" + FLOW_TOTAL
                + "{" + STAGE + "=\"" + SUCCESS + "\"}[" + promDuration(currentWindow)
                + "] offset " + promDuration(comparisonOffset) + "))";
    }

    /** @return 1분 단위 attempt·success 추세 질의 */
    public static String flowTrend() {
        return currentFlow();
    }

    /** 지정 버킷 구간을 사용하는 attempt·success 추세 질의를 만듭니다. */
    public static String flowTrend(Duration currentWindow) {
        return currentFlow(currentWindow);
    }

    /** @return 쿠폰 회차별 마지막 성공 이벤트 epoch 질의 */
    public static String lastSuccessEpoch() {
        return "max by (" + COUPON_ID + ") (" + LAST_SUCCESS_EPOCH + ")";
    }

    /** @return 쿠폰 회차별 발급 흐름의 가장 오래된 실제 scrape 시각 질의 */
    public static String flowFreshnessEpoch() {
        return "min by (" + COUPON_ID + ") (timestamp(" + FLOW_TOTAL
                + "{" + STAGE + "=~\"" + ATTEMPT + "|" + SUCCESS + "\"}))";
    }

    /** @return 현재 1분 attempt 이벤트 발행 실패 증가량 질의 */
    public static String attemptPublishFailures() {
        return attemptPublishFailures(OverviewPrometheusProperties.defaults().currentWindow());
    }

    /** 지정 집계 구간을 사용하는 attempt 발행 실패 증가량 질의를 만듭니다. */
    public static String attemptPublishFailures(Duration currentWindow) {
        return "sum(increase(" + ATTEMPT_PUBLISH_FAILURES_TOTAL + "["
                + promDuration(currentWindow) + "]))";
    }

    /** @return attempt 발행 실패 시계열의 가장 오래된 실제 scrape 시각 질의 */
    public static String attemptPublishFailureFreshnessEpoch() {
        return "min(timestamp(" + ATTEMPT_PUBLISH_FAILURES_TOTAL + "))";
    }

    /** @return 최근 5분 raw 고객 결과별 reset-aware 추정 발생 건수 질의 */
    public static String outcomes() {
        return outcomes(OverviewPrometheusProperties.defaults().outcomeWindow());
    }

    /** 지정 집계 구간을 사용하는 raw 고객 결과별 추정 발생 건수 질의를 만듭니다. */
    public static String outcomes(Duration outcomeWindow) {
        return "sum by (" + OUTCOME + ") (increase(" + OUTCOME_TOTAL + "["
                + promDuration(outcomeWindow) + "]))";
    }

    /** @return 평가 시각에 존재하는 raw outcome label을 보존하는 모집단 질의 */
    public static String outcomeInventory() {
        return "count by (" + OUTCOME + ") (" + OUTCOME_TOTAL + ")";
    }

    /** @return 고객 결과 시계열의 가장 오래된 실제 scrape 시각 질의 */
    public static String outcomeFreshnessEpoch() {
        return "min(timestamp(" + OUTCOME_TOTAL + "))";
    }

    /** @return issue 성공 요청의 인스턴스별 p99 질의 */
    public static String successfulP99() {
        return HTTP_LATENCY_SECONDS
                + "{" + URI_GROUP + "=\"issue\"," + OUTCOME + "=\"" + SUCCESS + "\","
                + QUANTILE + "=\"0.99\"}";
    }

    /** @return 성공 p99 시계열의 가장 오래된 실제 scrape 시각 질의 */
    public static String latencyFreshnessEpoch() {
        return "min(timestamp(" + successfulP99() + "))";
    }

    /** PromQL range selector와 offset에서 사용하는 정수 분·초 표현으로 변환합니다. */
    private static String promDuration(Duration duration) {
        long seconds = duration.toSeconds();
        return seconds % 60L == 0L ? (seconds / 60L) + "m" : seconds + "s";
    }
}
