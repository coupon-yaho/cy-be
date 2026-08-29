package com.kafkick.api.admin.observability;

import java.time.Instant;
import java.util.Objects;

/**
 * Prometheus matrix 시계열의 한 시각·값 표본입니다.
 *
 * @param observedAt Prometheus가 반환한 표본 시각
 * @param value 표본 값; NaN 또는 무한대이면 숫자 관측값으로 해석하지 않음
 */
public record PromRangePoint(Instant observedAt, double value) {

    /** 표본 시각을 필수로 검증합니다. */
    public PromRangePoint {
        Objects.requireNonNull(observedAt, "observedAt");
    }

    /**
     * 이 표본이 계산에 사용할 수 있는 유한한 숫자인지 반환합니다.
     *
     * @return 유한한 숫자이면 true
     */
    public boolean hasNumericValue() {
        return Double.isFinite(value);
    }
}
