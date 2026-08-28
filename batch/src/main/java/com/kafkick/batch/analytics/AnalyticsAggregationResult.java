package com.kafkick.batch.analytics;

import java.time.Instant;
import java.util.Map;

/**
 * 집계 한 회차의 결과.
 *
 * @param runId        {@code analytics_runs.id}
 * @param asOf         집계 기준 시각. 같은 값으로 다시 돌리면 issued_at 기준 두 축은 같은 값이 나온다
 * @param writtenRows  축별로 쓴 행 수. 안 바뀐 버킷은 여기 안 들어온다.
 *                     ⚠️ 한 회차가 여러 걸음으로 따라잡으면 같은 버킷이 걸음마다 다시 세어지므로
 *                     <b>걸음 중복이 포함된다</b> — 표에 남은 행 수가 아니라 쓴 횟수다. 로그와
 *                     테스트용 숫자이고, 여기서 저장량을 추정하면 안 된다
 * @param failedAxes   실패한 축과 사유. 비어 있으면 회차 전체가 SUCCEEDED 다
 */
public record AnalyticsAggregationResult(
        long runId,
        Instant asOf,
        Map<AnalyticsAxis, Integer> writtenRows,
        Map<AnalyticsAxis, String> failedAxes
) {

    public AnalyticsAggregationResult {
        writtenRows = Map.copyOf(writtenRows);
        failedAxes = Map.copyOf(failedAxes);
    }

    public boolean succeeded() {
        return failedAxes.isEmpty();
    }
}
