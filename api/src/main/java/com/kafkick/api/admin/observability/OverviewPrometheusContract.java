package com.kafkick.api.admin.observability;

import com.kafkick.core.observation.DomainMeterNames;

/** Overview 관측이 사용하는 B 소유 scrape 이름·라벨·PromQL을 한곳에 모읍니다. */
public final class OverviewPrometheusContract {

    public static final String FLOW_TOTAL = "app_issuance_flow_total";
    public static final String OUTCOME_TOTAL = "app_issuance_outcome_total";
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

    /** 인스턴스화를 막습니다. */
    private OverviewPrometheusContract() { }

    /** @return 현재 1분 attempt·success 캠페인별 증가량 질의 */
    public static String currentFlow() {
        return "sum by (coupon_id, stage) (increase(" + FLOW_TOTAL
                + "{stage=~\"attempt|success\"}[1m]))";
    }

    /** @return 직전 1분 success 캠페인별 증가량 질의 */
    public static String comparisonSuccess() {
        return "sum by (coupon_id) (increase(" + FLOW_TOTAL
                + "{stage=\"success\"}[1m] offset 1m))";
    }

    /** @return 1분 단위 attempt·success 추세 질의 */
    public static String flowTrend() {
        return currentFlow();
    }

    /** @return 캠페인별 마지막 성공 이벤트 epoch 질의 */
    public static String lastSuccessEpoch() {
        return "max by (coupon_id) (" + LAST_SUCCESS_EPOCH + ")";
    }

    /** @return 캠페인별 발급 흐름의 가장 오래된 실제 scrape 시각 질의 */
    public static String flowFreshnessEpoch() {
        return "min by (coupon_id) (timestamp(" + FLOW_TOTAL
                + "{stage=~\"attempt|success\"}))";
    }

    /** @return 현재 1분 attempt 이벤트 발행 실패 증가량 질의 */
    public static String attemptPublishFailures() {
        return "sum(increase(" + ATTEMPT_PUBLISH_FAILURES_TOTAL + "[1m]))";
    }

    /** @return attempt 발행 실패 시계열의 가장 오래된 실제 scrape 시각 질의 */
    public static String attemptPublishFailureFreshnessEpoch() {
        return "min(timestamp(" + ATTEMPT_PUBLISH_FAILURES_TOTAL + "))";
    }

    /** @return 최근 5분 raw 고객 결과별 reset-aware 추정 발생 건수 질의 */
    public static String outcomes() {
        return "sum by (outcome) (increase(" + OUTCOME_TOTAL + "[5m]))";
    }

    /** @return 평가 시각에 존재하는 raw outcome label을 보존하는 모집단 질의 */
    public static String outcomeInventory() {
        return "count by (outcome) (" + OUTCOME_TOTAL + ")";
    }

    /** @return 고객 결과 시계열의 가장 오래된 실제 scrape 시각 질의 */
    public static String outcomeFreshnessEpoch() {
        return "min(timestamp(" + OUTCOME_TOTAL + "))";
    }

    /** @return issue 성공 요청의 인스턴스별 p99 질의 */
    public static String successfulP99() {
        return HTTP_LATENCY_SECONDS
                + "{uri_group=\"issue\",outcome=\"success\",quantile=\"0.99\"}";
    }

    /** @return 성공 p99 시계열의 가장 오래된 실제 scrape 시각 질의 */
    public static String latencyFreshnessEpoch() {
        return "min(timestamp(" + successfulP99() + "))";
    }
}
