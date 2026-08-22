package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;

/** 관리자 Metrics HTTP query의 짧은 구간 코드를 변환하는 계약을 검증합니다. */
class MetricsWindowConverterTest {

    private final MetricsWindowConverter converter = new MetricsWindowConverter();

    /** 지원하는 세 query 값이 각각의 도메인 구간으로 변환되는지 검증합니다. */
    @Test
    void convertsSupportedHttpValues() {
        assertThat(converter.convert("1m")).isEqualTo(MetricsWindow.ONE_MINUTE);
        assertThat(converter.convert("5m")).isEqualTo(MetricsWindow.FIVE_MINUTES);
        assertThat(converter.convert("15m")).isEqualTo(MetricsWindow.FIFTEEN_MINUTES);
    }

    /** 지원하지 않는 query 값은 묵시적 기본값 없이 거부합니다. */
    @Test
    void rejectsUnsupportedHttpValue() {
        assertThatThrownBy(() -> converter.convert("30m"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원하지 않는 metrics window");
        assertThatThrownBy(() -> converter.convert(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원하지 않는 metrics window");
    }
}
