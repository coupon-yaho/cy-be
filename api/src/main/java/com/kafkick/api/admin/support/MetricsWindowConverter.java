package com.kafkick.api.admin.support;

import org.springframework.core.convert.converter.Converter;

import com.kafkick.core.admin.MetricsWindow;

/**
 * 관리자 Metrics query의 집계 구간 값을 확정 enum으로 변환합니다.
 *
 * <p>화면 계약은 {@code 3s}, {@code 1m}, {@code 5m}, {@code 15m} 이고 서버 계약은 enum 이름입니다. 두 표기를
 * 모두 받되 대소문자는 정확히 일치해야 합니다 — 관대하게 받으면 표기가 늘어나 어느 쪽이 정본인지
 * 사라집니다.</p>
 */
public class MetricsWindowConverter implements Converter<String, MetricsWindow> {

    @Override
    public MetricsWindow convert(String source) {
        if (source == null) {
            throw new IllegalArgumentException("지원하지 않는 metrics window입니다: null");
        }
        return switch (source) {
            case "3s", "THREE_SECONDS" -> MetricsWindow.THREE_SECONDS;
            case "1m", "ONE_MINUTE" -> MetricsWindow.ONE_MINUTE;
            case "5m", "FIVE_MINUTES" -> MetricsWindow.FIVE_MINUTES;
            case "15m", "FIFTEEN_MINUTES" -> MetricsWindow.FIFTEEN_MINUTES;
            default -> throw new IllegalArgumentException(
                    "지원하지 않는 metrics window입니다: " + source);
        };
    }
}
