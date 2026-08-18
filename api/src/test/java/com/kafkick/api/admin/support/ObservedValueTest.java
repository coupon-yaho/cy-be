package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.observation.SourceStatus;

/** 미수집 관측값을 정상값이나 가짜 0으로 바꾸지 않는 공통 DTO 규칙을 검증합니다. */
@AdminJsonTest
class ObservedValueTest {

    @Autowired
    private ObjectMapper objectMapper;

    /** PENDING 상태에서는 null 필드를 생략하고 상태만 직렬화하는지 확인합니다. */
    @Test
    void pendingValueKeepsNullInsteadOfInventingZero() throws Exception {
        ObservedValue<Long> value = new ObservedValue<>(
                null,
                SourceStatus.PENDING,
                null
        );

        assertThat(objectMapper.writeValueAsString(value))
                .isEqualTo("{\"state\":\"PENDING\"}");
    }

    /** 실제 0은 미관측 null과 달리 상태와 실제 관측 시각을 함께 보존하는지 검증합니다. */
    @Test
    void observedZeroKeepsValueAndObservedAt() {
        Instant observedAt = Instant.parse("2026-08-16T00:00:00Z");

        ObservedValue<Long> value = new ObservedValue<>(0L, SourceStatus.NO_TRAFFIC, observedAt);

        assertThat(value.value()).isZero();
        assertThat(value.observedAt()).isEqualTo(observedAt);
    }

    /** SourceObservation과 충돌하는 상태·시각 조합을 HTTP Wrapper가 허용하지 않는지 검증합니다. */
    @Test
    void stateAndObservedAtMustFollowCanonicalObservationInvariant() {
        Instant observedAt = Instant.parse("2026-08-16T00:00:00Z");

        assertThatThrownBy(() -> new ObservedValue<>(null, SourceStatus.PENDING, observedAt))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObservedValue<>(1L, SourceStatus.UNAVAILABLE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObservedValue<>(1L, SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
