package com.kafkick.api.admin.support;

import org.springframework.core.convert.converter.Converter;

import com.kafkick.core.admin.MetricsWindow;

/** HTTP의 짧은 집계 구간 코드({@code 1m}, {@code 5m}, {@code 15m})를 확정 enum으로 변환합니다. */
public class MetricsWindowConverter implements Converter<String, MetricsWindow> {

    @Override
    public MetricsWindow convert(String source) {
        if (source == null) {
            throw new IllegalArgumentException("지원하지 않는 metrics window입니다: null");
        }
        return switch (source) {
            case "1m" -> MetricsWindow.ONE_MINUTE;
            case "5m" -> MetricsWindow.FIVE_MINUTES;
            case "15m" -> MetricsWindow.FIFTEEN_MINUTES;
            default -> throw new IllegalArgumentException(
                    "지원하지 않는 metrics window입니다: " + source);
        };
    }
}
