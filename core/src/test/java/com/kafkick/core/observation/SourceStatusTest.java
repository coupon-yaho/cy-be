package com.kafkick.core.observation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** 관측 상태별 값과 관측 시각 보유 계약을 검증합니다. */
class SourceStatusTest {

    /** 실제 관측 결과를 표현하는 상태는 값과 관측 시각을 함께 가져야 합니다. */
    @ParameterizedTest
    @EnumSource(value = SourceStatus.class, names = {"VALID", "WARMING_UP", "STALE", "NO_TRAFFIC"})
    @DisplayName("관측 결과가 있는 상태는 값과 시각을 보유한다")
    void observedStatesCarryValue(SourceStatus status) {
        assertThat(status.carriesValue()).isTrue();
    }

    /** 값이 확정되지 않았거나 적용되지 않는 상태는 값과 관측 시각을 갖지 않습니다. */
    @ParameterizedTest
    @EnumSource(value = SourceStatus.class, names = {"PENDING", "UNAVAILABLE", "N_A"})
    @DisplayName("관측 결과가 없는 상태는 값과 시각을 보유하지 않는다")
    void unobservedStatesDoNotCarryValue(SourceStatus status) {
        assertThat(status.carriesValue()).isFalse();
    }
}
