package com.kafkick.api.admin.observability;

import java.time.Instant;
import java.util.List;

/** 명시한 평가 시각에서 Prometheus instant-vector를 조회하는 경계입니다. */
@FunctionalInterface
public interface PromTimeQuery {

    /**
     * PromQL을 지정 시각에 평가합니다.
     *
     * @param promQl 실행할 PromQL
     * @param evaluationAt Prometheus가 instant expression을 평가할 시각
     * @return 벡터 결과의 표본 목록
     */
    List<PromSample> query(String promQl, Instant evaluationAt);
}
