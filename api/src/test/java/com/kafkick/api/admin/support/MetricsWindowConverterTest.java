package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;

/** 관리자 Metrics HTTP query의 집계 구간 표기를 변환하는 계약을 검증합니다. */
class MetricsWindowConverterTest {

    private final MetricsWindowConverter converter = new MetricsWindowConverter();

    /** 지원하는 세 query 값이 각각의 도메인 구간으로 변환되는지 검증합니다. */
    @Test
    void convertsSupportedHttpValues() {
        assertThat(converter.convert("3s")).isEqualTo(MetricsWindow.THREE_SECONDS);
        assertThat(converter.convert("1m")).isEqualTo(MetricsWindow.ONE_MINUTE);
        assertThat(converter.convert("5m")).isEqualTo(MetricsWindow.FIVE_MINUTES);
        assertThat(converter.convert("15m")).isEqualTo(MetricsWindow.FIFTEEN_MINUTES);
    }

    /** 기존 enum 이름 표기도 같은 구간으로 변환되는지 검증합니다. */
    @Test
    void convertsEnumNames() {
        assertThat(converter.convert("THREE_SECONDS")).isEqualTo(MetricsWindow.THREE_SECONDS);
        assertThat(converter.convert("ONE_MINUTE")).isEqualTo(MetricsWindow.ONE_MINUTE);
        assertThat(converter.convert("FIVE_MINUTES")).isEqualTo(MetricsWindow.FIVE_MINUTES);
        assertThat(converter.convert("FIFTEEN_MINUTES")).isEqualTo(MetricsWindow.FIFTEEN_MINUTES);
    }

    /** 대소문자가 다른 표기는 받지 않습니다. */
    @Test
    void rejectsDifferentCasing() {
        assertThatThrownBy(() -> converter.convert("1M"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> converter.convert("one_minute"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 지원하지 않는 값과 빈 값을 묵시적 기본값 없이 거부합니다. */
    @Test
    void rejectsUnsupportedOrBlankValues() {
        assertThatThrownBy(() -> converter.convert("24h"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원하지 않는 metrics window");
        assertThatThrownBy(() -> converter.convert(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원하지 않는 metrics window");
        // 빈 값은 default 가 잡는다. 나중에 표기를 손질하다 앞쪽에 빈 문자열 분기를 하나
        // 끼워 넣으면 조용히 기본 창으로 떨어질 수 있어 여기에 못을 박는다.
        assertThatThrownBy(() -> converter.convert(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원하지 않는 metrics window");
        assertThatThrownBy(() -> converter.convert(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원하지 않는 metrics window");
    }
}
