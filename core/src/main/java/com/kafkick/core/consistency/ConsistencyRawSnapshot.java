package com.kafkick.core.consistency;

import java.util.Objects;

/**
 * 한 번의 정합성 평가에 사용하는 원천값과 원천별 관측 상태를 묶습니다.
 *
 * <p>값은 {@link ConsistencyRawValues}에 보관하고, 계산 가능 여부와 관측 시각은
 * Redis·DB 각각의 {@link SourceObservation}으로 전달합니다.
 *
 * @param rawValues 같은 수집 주기에서 확보한 정합성 원천값
 * @param redisObservation Redis 원천의 상태와 관측 시각
 * @param databaseObservation DB 원천의 상태와 관측 시각
 */
public record ConsistencyRawSnapshot(
        ConsistencyRawValues rawValues,
        SourceObservation redisObservation,
        SourceObservation databaseObservation
) {

    /** 모든 구성 요소가 존재하는지 검증합니다. */
    public ConsistencyRawSnapshot {
        Objects.requireNonNull(rawValues, "rawValues");
        Objects.requireNonNull(redisObservation, "redisObservation");
        Objects.requireNonNull(databaseObservation, "databaseObservation");
    }
}
