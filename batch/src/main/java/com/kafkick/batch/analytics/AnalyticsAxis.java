package com.kafkick.batch.analytics;

/**
 * 집계 축 셋. 축마다 상태와 완료 시각을 따로 갖는 이유는 {@code AdminAnalyticsDataset} 이 세 축을
 * 각각 {@code AggregateObservation} 으로 들고 있어서다 — 한 축이 실패해도 나머지는 값을 내야 한다.
 */
public enum AnalyticsAxis {

    MONTHLY_TREND("monthly_trend"),
    HOURLY_HEATMAP("hourly_heatmap"),
    ISSUANCE_STATUS("issuance_status");

    private final String columnPrefix;

    AnalyticsAxis(String columnPrefix) {
        this.columnPrefix = columnPrefix;
    }

    /** {@code analytics_runs} 의 축 상태 컬럼. 값은 {@code AggregateAvailability} 이름 그대로다. */
    public String statusColumn() {
        return columnPrefix + "_status";
    }

    /**
     * {@code analytics_runs} 의 축 완료 시각 컬럼 — 이 축이 마지막으로 정상 커밋된 시각이다.
     *
     * <p>⚠️ 화면 최신성 판정에는 <b>쓰이지 않는다</b>(A 확정 2026-08-26). 따라잡는 중이면 이 값은
     * "지금" 인데 실제 집계는 과거까지라, 이것으로 판정하면 stale-after 가 발동하지 않는다.
     * A 가 읽는 것은 {@link #aggregatedThroughColumn()} 이다. 여기는 운영 진단용이다.
     */
    public String completedAtColumn() {
        return columnPrefix + "_completed_at";
    }

    /**
     * 이 축의 <b>집계 수위선</b> — 이 시각 이전 원천이 모두 반영됐다는 뜻이다.
     *
     * <p>두 곳이 읽는다: 다음 회차가 이어갈 시작점이고, <b>A 의 {@code observedAt}</b> 이다
     * (A 확정 2026-08-26). 정상 커밋될 때만 전진하고 실패하면 이전 값을 유지한다.
     *
     * <p>완료 시각과 다른 값이다 — 밀린 구간을 여러 걸음에 나눠 따라잡는 동안, 회차는 방금 돌았지만
     * (완료 시각은 지금) 아직 과거까지밖에 못 세었을 수 있다(수위선은 과거).
     */
    public String aggregatedThroughColumn() {
        return columnPrefix + "_aggregated_through";
    }
}
