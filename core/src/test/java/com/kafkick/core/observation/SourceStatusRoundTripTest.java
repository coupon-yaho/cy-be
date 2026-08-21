package com.kafkick.core.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * 상태 코드의 두 방향이 어긋나지 않는지 검증합니다.
 *
 * <p>내는 쪽은 batch, 읽는 쪽은 api 라 어긋나도 예외가 나지 않습니다 — 화면이 조용히 다른
 * 상태를 그립니다. 그래서 왕복을 테스트가 지킵니다.</p>
 */
class SourceStatusRoundTripTest {

    @ParameterizedTest
    @EnumSource(SourceStatus.class)
    @DisplayName("모든 상태는 코드로 바꿨다 되돌려도 같은 상태다")
    void roundTrips(SourceStatus status) {
        assertThat(SourceStatusCode.statusOf(SourceStatusCode.of(status))).isEqualTo(status);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 8, -1})
    @DisplayName("정의되지 않은 코드는 임의의 상태로 해석하지 않고 거부한다")
    void rejectsUnknownCode(int code) {
        assertThatThrownBy(() -> SourceStatusCode.statusOf(code))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(Severity.class)
    @DisplayName("모든 심각도는 코드로 바꿨다 되돌려도 같은 심각도다")
    void severityRoundTrips(Severity severity) {
        assertThat(SourceStatusCode.severityOf(SourceStatusCode.of(severity))).isEqualTo(severity);
    }
}
