package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Prometheus range query를 실행하고 matrix 시계열을 반환하는 경계입니다. */
@FunctionalInterface
public interface PromRangeQuery {

    /**
     * 지정한 폐구간을 일정 간격으로 평가합니다.
     *
     * @param promQl 실행할 PromQL
     * @param start 평가 시작 시각
     * @param end 평가 종료 시각
     * @param step 양수 평가 간격
     * @return matrix 시계열 목록; 일치하는 시계열이 없으면 빈 목록
     * @throws PromQueryException 호출이 실패했거나 결과를 해석할 수 없는 경우
     */
    List<PromRangeSeries> query(String promQl, Instant start, Instant end, Duration step);
}
