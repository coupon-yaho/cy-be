package com.kafkick.api.admin.events;

import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.api.admin.observability.dto.AdminEventItem;
import com.kafkick.api.admin.support.LiveEventPollResponse;
import com.kafkick.core.observation.attempt.AttemptLiveEntry;
import com.kafkick.core.observation.attempt.AttemptLivePage;
import com.kafkick.core.observation.attempt.AttemptLiveReader;

/**
 * live 버퍼 한 페이지를 관제 응답으로 옮긴다. <b>두 모듈에 걸친 계약이 만나는 유일한 자리다.</b>
 *
 * <p>{@link AttemptLiveEntry} 는 core 가 소유하고 {@code infra:redis} 가 쓴다.
 * {@link AdminEventItem} 은 api 가 소유하고 화면이 읽는다. 둘은 필드가 거의 같지만 <b>같은
 * 타입이 아니어야 한다</b> — 하나로 합치면 화면 응답의 모양을 바꾸는 일이 Redis 에 이미 앉아
 * 있는 항목의 역직렬화를 깨뜨린다. 배포 경계에서 두 형식이 공존하는 구간이 반드시 있다.
 *
 * <p>그 대가가 이 매퍼이고, {@code LiveEventAssemblerTest} 가 두 레코드의 <b>컴포넌트 수</b>를
 * 함께 본다. 한쪽에 필드가 늘면 그 테스트가 깨진다 — 따로 검증하는 테스트 두 개로는 "옮기는
 * 것을 빠뜨렸다" 를 못 잡는다.
 */
@Component
public class LiveEventAssembler {

    private final AttemptLiveReader reader;

    public LiveEventAssembler(AttemptLiveReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    public LiveEventPollResponse assemble(String afterCursor, int limit) {
        AttemptLivePage page = reader.readAfter(afterCursor, limit);
        return new LiveEventPollResponse(
                page.entries().stream().map(LiveEventAssembler::toItem).toList(),
                page.nextCursor(),
                page.hasMore(),
                page.cursorExpired(),
                // 만료됐다면 그 사이에 무엇이 몇 건 지나갔는지 알 수 없다. 세는 것이 불가능한
                // 이유는 AttemptLivePage javadoc 에 있다 — 두 플래그가 같은 사실의 두 얼굴이다.
                page.cursorExpired());
    }

    /**
     * 필드를 하나씩 옮긴다. <b>정제는 여기서 하지 않는다</b> — 쿠폰 코드 마스킹은 Redis 에
     * 쓰기 <b>전에</b> 이미 끝났다({@link AttemptLiveEntry#from}). 여기서 다시 자르면 정제 지점이
     * 둘이 되고, 그러면 어느 쪽이 진짜 경계인지 알 수 없어진다.
     */
    private static AdminEventItem toItem(AttemptLiveEntry entry) {
        return new AdminEventItem(
                entry.eventId(),
                entry.eventType(),
                entry.memberId(),
                entry.couponId(),
                entry.issuanceId(),
                entry.issuanceCodeMasked(),
                entry.grade(),
                entry.httpStatus(),
                entry.reasonCode(),
                entry.queuePosition(),
                entry.queueSequence(),
                entry.replayed(),
                entry.occurredAt());
    }

    /** 화면이 쓰지 않는 {@code ingestedAt} 은 옮기지 않는다. 목록은 이미 도착 순서다. */
    static List<String> droppedOnPurpose() {
        return List.of("ingestedAt");
    }
}
