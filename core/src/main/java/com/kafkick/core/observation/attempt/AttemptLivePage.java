package com.kafkick.core.observation.attempt;

import java.util.List;
import java.util.Objects;

/**
 * live 버퍼를 커서 이후로 한 번 읽은 결과.
 *
 * <h2>{@code droppedCount} 가 없는 이유</h2>
 *
 * 버려진 건수를 셀 수 없기 때문이다. 버퍼는 {@code MAXLEN ~ 200} 으로 잘리는데 {@code ~} 는
 * <b>근사</b> trimming 이다 — Redis 는 노드 경계에서만 잘라서 실제 길이가 200 을 넘는다. 커서와
 * 현재 첫 항목 사이에 몇 건이 있었는지는 이미 지워진 뒤라 알 수 없고, 추정치를 정수로 내보내면
 * 화면이 그것을 사실로 읽는다. 그래서 필드를 두지 않는다 — {@code null} 을 내보내는 것보다
 * 없는 편이 낫다. {@link #cursorExpired()} 가 "얼마인지는 모르지만 놓친 것이 있다" 를 말한다.
 *
 * @param entries 커서 이후의 항목. 저장소 수집 순서다
 * @param nextCursor 다음 폴링에 넘길 불투명 커서. 항목이 없으면 요청한 커서가 그대로 돌아온다
 * @param hasMore 같은 조회 시점에 아직 안 준 항목이 남았는지
 * @param cursorExpired 요청한 커서가 트림 구간 밖이라 현재 첫 항목부터 다시 읽었는지
 */
public record AttemptLivePage(
        List<AttemptLiveEntry> entries,
        String nextCursor,
        boolean hasMore,
        boolean cursorExpired
) {

    public AttemptLivePage {
        entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
    }
}
