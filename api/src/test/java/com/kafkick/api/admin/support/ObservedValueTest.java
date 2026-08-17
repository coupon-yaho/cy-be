package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.admin.SourceStatus;

/** 미수집 관측값을 정상값이나 가짜 0으로 바꾸지 않는 공통 DTO 규칙을 검증합니다. */
class ObservedValueTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** PENDING 상태에서는 value와 observedAt이 null인 구조가 그대로 유지되는지 확인합니다. */
    @Test
    void pendingValueKeepsNullInsteadOfInventingZero() throws Exception {
        ObservedValue<Long> value = new ObservedValue<>(
                null,
                SourceStatus.PENDING,
                Instant.parse("2026-08-16T00:00:00Z")
        );

        assertThat(objectMapper.writeValueAsString(value)).isEqualTo(
                "{\"value\":null,\"state\":\"PENDING\",\"observedAt\":\"2026-08-16T00:00:00Z\"}"
        );
    }
}
