package com.kafkick.api.admin.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminEventItem;
import com.kafkick.api.admin.support.LiveEventPollResponse;
import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.attempt.AttemptLiveEntry;
import com.kafkick.core.observation.attempt.AttemptLivePage;
import com.kafkick.core.observation.attempt.AttemptLiveReader;

class LiveEventAssemblerTest {

    /**
     * <b>두 레코드를 잇는 계약 테스트다.</b> 각각을 따로 보는 테스트 두 개로는 "한쪽에 필드가
     * 늘었는데 옮기는 것을 빠뜨렸다" 를 못 잡는다 — 양쪽 다 자기 테스트는 초록이고, 화면의 그
     * 칸만 조용히 빈다.
     *
     * <p>{@link AttemptLiveEntry} 에 {@code ingestedAt} 이 하나 더 있다. 그것은 의도적으로
     * 안 옮기는 필드이고, 그 목록이 {@code LiveEventAssembler.droppedOnPurpose()} 에 있다.
     * 필드가 새로 늘면 이 산수가 안 맞아 여기서 깨진다 — 그때 옮길지 뺄지를 정하고 그 결정을
     * 저 목록에 적으면 된다.
     */
    @Test
    void carriesEveryEntryFieldExceptTheOnesDroppedOnPurpose() {
        List<String> entryFields = componentNames(AttemptLiveEntry.class);
        List<String> itemFields = componentNames(AdminEventItem.class);

        assertThat(entryFields).containsAll(LiveEventAssembler.droppedOnPurpose());
        assertThat(entryFields.size() - LiveEventAssembler.droppedOnPurpose().size())
                .as("한쪽에 필드가 늘면 여기서 깨져야 한다")
                .isEqualTo(itemFields.size());
        // 이름까지 맞춘다. 개수만 보면 필드 하나를 지우고 다른 하나를 더한 변경이 통과한다.
        assertThat(itemFields).containsExactlyInAnyOrderElementsOf(
                entryFields.stream()
                        .filter(name -> !LiveEventAssembler.droppedOnPurpose().contains(name))
                        .toList());
    }

    /** 값이 실제로 제자리에 들어가는지. 위 테스트는 이름만 보고 배선은 안 본다. */
    @Test
    void mapsEveryValueToTheMatchingResponseField() {
        AttemptLiveEntry entry = new AttemptLiveEntry(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                EventType.ENTRY_RESULT, 101L, 201L, 301L, "ABCD1234", Grade.GOLD,
                202, ReasonCode.QUEUE_REQUIRED, 3L, 8L, true,
                Instant.parse("2026-08-25T00:00:00Z"), Instant.parse("2026-08-25T00:00:01Z"));

        LiveEventPollResponse response = assembler(
                new AttemptLivePage(List.of(entry), "1-0", false, false)).assemble(null, 50);

        assertThat(response.items()).containsExactly(new AdminEventItem(
                entry.eventId(), EventType.ENTRY_RESULT, 101L, 201L, 301L, "ABCD1234", Grade.GOLD,
                202, ReasonCode.QUEUE_REQUIRED, 3L, 8L, true,
                Instant.parse("2026-08-25T00:00:00Z")));
    }

    /**
     * 만료는 <b>두</b> 플래그를 함께 세운다. 같은 사실의 두 얼굴이라 하나만 세우면 화면이
     * "복구했지만 유실은 없다" 로 읽는다 — 정확히 반대다.
     */
    @Test
    void reportsBothResetAndPossibleLossWhenTheCursorExpired() {
        LiveEventPollResponse response = assembler(
                new AttemptLivePage(List.of(), "9-0", false, true)).assemble("0-1", 50);

        assertThat(response.cursorReset()).isTrue();
        assertThat(response.eventsMayBeMissing()).isTrue();
    }

    /** 만료가 아니면 둘 다 거짓이다. 위 테스트만 두면 "항상 참" 인 구현도 초록이다. */
    @Test
    void reportsNeitherFlagOnANormalPoll() {
        LiveEventPollResponse response = assembler(
                new AttemptLivePage(List.of(), "9-0", true, false)).assemble("8-0", 50);

        assertThat(response.cursorReset()).isFalse();
        assertThat(response.eventsMayBeMissing()).isFalse();
        assertThat(response.hasMore()).isTrue();
        assertThat(response.nextAfterCursor()).isEqualTo("9-0");
    }

    private static LiveEventAssembler assembler(AttemptLivePage page) {
        AttemptLiveReader reader = (afterCursor, limit) -> page;
        return new LiveEventAssembler(reader);
    }

    private static List<String> componentNames(Class<?> recordType) {
        return Arrays.stream(recordType.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();
    }
}
