package com.kafkick.core.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** 집계 창의 길이와 초 환산이 질의 문자열과 같은 값을 내는지 검증합니다. */
class MetricsWindowTest {

    /** 이름과 길이가 어긋나면 5분 창으로 계산한 비율이 1분 창 라벨로 나갑니다. */
    @Test
    @DisplayName("각 구간의 길이는 이름이 말하는 값과 같다")
    void durationMatchesName() {
        assertThat(MetricsWindow.THREE_SECONDS.duration()).isEqualTo(Duration.ofSeconds(3));
        assertThat(MetricsWindow.ONE_MINUTE.duration()).isEqualTo(Duration.ofMinutes(1));
        assertThat(MetricsWindow.FIVE_MINUTES.duration()).isEqualTo(Duration.ofMinutes(5));
        assertThat(MetricsWindow.FIFTEEN_MINUTES.duration()).isEqualTo(Duration.ofMinutes(15));
    }

    /** 질의의 구간 표기는 초로만 쓰므로 두 표현이 갈라지면 안 됩니다. */
    @ParameterizedTest
    @EnumSource(MetricsWindow.class)
    @DisplayName("seconds()는 duration()과 같은 길이를 초로 돌려준다")
    void secondsMatchesDuration(MetricsWindow window) {
        assertThat(window.seconds()).isEqualTo(window.duration().toSeconds());
        assertThat(window.seconds()).isPositive();
    }
}
