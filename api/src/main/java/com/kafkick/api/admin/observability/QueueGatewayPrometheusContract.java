package com.kafkick.api.admin.observability;

/** 외부 게이트웨이 v0.4.0 안내서가 보장하는 Prometheus 이름입니다. */
public final class QueueGatewayPrometheusContract {

    public static final String JOB = "queue-gateway";
    public static final String WAITING = "waiting_queue_waiting";
    public static final String SNAPSHOT_AGE = "waiting_snapshot_age";
    public static final String CAPACITY_CREDIT = "waiting_capacity_credit";
    public static final String CAPACITY_NODES = "waiting_capacity_nodes";
    public static final String JUDGEMENT_TOTAL = "waiting_judgement_total";
    public static final String BACKEND_FALLBACK_TOTAL = "waiting_backend_fallback_total";
    public static final String ALLOCATION_OVERSHOOT_TOTAL =
            "waiting_allocation_entered_overshoot_total";

    /** 같은 묶음 질의에서 실제 scrape 나이를 식별하기 위한 파생 시계열 이름입니다. */
    public static final String SCRAPE_AGE = "waiting_gateway_scrape_age_seconds";

    private QueueGatewayPrometheusContract() {
    }
}
