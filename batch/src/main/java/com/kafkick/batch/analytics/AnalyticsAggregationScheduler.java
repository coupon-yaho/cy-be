package com.kafkick.batch.analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.annotation.Scheduled;

/**
 * 1시간 주기 집계. 주기와 {@code admin.analytics.stale-after}(3시간)는 A 와 확정된 값이다 —
 * 한두 번의 지연은 허용하고 3시간 이상 정상 집계가 없을 때 화면이 STALE 이 된다.
 *
 * <p>{@code batch.scheduling.enabled} 를 함께 문다(조건은 {@link AnalyticsConfiguration} 이 건다). 그 스위치는 "전 쓰기 스케줄러를 멈춘다" 는
 * 뜻이라 이 배치도 대상이다 — 다만 이 배치는 {@code dataset_fingerprint} 재료를 건드리지 않으므로,
 * 동결 구간에 멈추는 것은 판정 보호가 아니라 <b>관측 풀을 비워 두기 위한</b> 것이다.
 */
public class AnalyticsAggregationScheduler {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsAggregationScheduler.class);

    private final AnalyticsAggregationRunner runner;

    public AnalyticsAggregationScheduler(AnalyticsAggregationRunner runner) {
        this.runner = runner;
    }

    @Scheduled(cron = "${batch.schedule.analytics-cron}")
    public void aggregate() {
        AnalyticsAggregationResult result = runner.runOnce();
        if (!result.succeeded()) {
            log.warn("analytics aggregation had failed axes: runId={}, failed={}",
                    result.runId(), result.failedAxes());
        }
    }
}
