package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

/** 과거 목록과 live polling이 서로 다른 cursor 및 복구 플래그 구조를 사용하는지 검증합니다. */
class AdminCommonDtoJsonSerializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 일반 과거 목록 응답이 nextBeforeCursor와 hasOlder를 제공하는지 검증합니다. */
    @Test
    void olderCursorPageUsesBeforeCursorContract() throws Exception {
        OlderCursorPageResponse<Long> page = new OlderCursorPageResponse<>(List.of(3L, 2L), "older", true);

        assertThat(objectMapper.writeValueAsString(page)).isEqualTo(
                "{\"items\":[3,2],\"nextBeforeCursor\":\"older\",\"hasOlder\":true}"
        );
    }

    /** live event 응답이 nextAfterCursor와 cursor 복구·유실 가능성 플래그를 보존하는지 검증합니다. */
    @Test
    void liveEventPollKeepsRecoveryFlags() throws Exception {
        LiveEventPollResponse response = new LiveEventPollResponse(List.of(), "next", true, true, true);

        assertThat(objectMapper.writeValueAsString(response)).isEqualTo(
                "{\"items\":[],\"nextAfterCursor\":\"next\",\"hasMore\":true,"
                        + "\"cursorReset\":true,\"eventsMayBeMissing\":true}"
        );
    }
}
