package com.kafkick.core.admin;

import java.util.Arrays;

/** 관리자 지표 조회가 허용하는 고정 집계 구간입니다. */
public enum MetricsWindow {
    ONE_MINUTE("1m"),
    FIVE_MINUTES("5m"),
    FIFTEEN_MINUTES("15m");

    private final String wireValue;

    MetricsWindow(String wireValue) {
        this.wireValue = wireValue;
    }

    /** @return HTTP query에서 사용하는 짧은 구간 코드; JSON 응답은 다른 enum처럼 상수 이름을 사용합니다. */
    public String wireValue() {
        return wireValue;
    }

    /**
     * HTTP query 값을 공통 enum으로 변환합니다.
     *
     * @param raw {@code 1m}, {@code 5m}, {@code 15m} 중 하나
     * @return 일치하는 집계 구간
     * @throws IllegalArgumentException 지원하지 않는 값인 경우
     */
    public static MetricsWindow fromWireValue(String raw) {
        return Arrays.stream(values())
                .filter(value -> value.wireValue.equals(raw))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("지원하지 않는 metrics window입니다: " + raw));
    }
}
