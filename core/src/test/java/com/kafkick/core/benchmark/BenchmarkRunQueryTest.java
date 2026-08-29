package com.kafkick.core.benchmark;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** Benchmark 목록 필터와 Keyset 위치의 입력 불변식을 검증한다. */
class BenchmarkRunQueryTest {

    @Test
    @DisplayName("종료 경계가 시작 경계보다 빠르거나 같으면 목록 조건을 거부한다")
    void rejectsNonIncreasingTimeRange() {
        Instant instant = Instant.parse("2026-08-26T00:00:00Z");

        assertThatThrownBy(() -> new BenchmarkRunQuery(
                instant, instant, EngineVersion.V3, null, null, 50))
                .isInstanceOf(BusinessException.class)
                .satisfies(it -> assertThat(((BusinessException) it).getErrorCode())
                        .isEqualTo(CommonErrorCode.INVALID_INPUT));
    }

    @Test
    @DisplayName("페이지 크기는 1에서 200 사이만 허용한다")
    void rejectsOutOfRangeLimit() {
        assertThatThrownBy(() -> new BenchmarkRunQuery(
                null, null, null, null, null, 201))
                .isInstanceOf(BusinessException.class)
                .satisfies(it -> assertThat(((BusinessException) it).getErrorCode())
                        .isEqualTo(CommonErrorCode.INVALID_INPUT));
    }

    @Test
    @DisplayName("Keyset 위치는 시작 시각과 양수 회차 ID를 함께 요구한다")
    void positionRequiresCompletePositiveKey() {
        assertThatThrownBy(() -> new BenchmarkRunPosition(null, 1L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new BenchmarkRunPosition(Instant.parse("2026-08-26T00:00:00Z"), 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("양수");
    }
}
