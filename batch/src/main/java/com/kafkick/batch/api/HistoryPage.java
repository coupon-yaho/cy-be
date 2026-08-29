package com.kafkick.batch.api;

import java.util.List;
import java.util.function.ToLongFunction;

/**
 * 이력 한 페이지.
 *
 * <p>{@code total} 을 함께 준다. 화면이 원형 그래프를 그리려면 한 페이지가 아니라 전체를
 * 봐야 하는데, 그 값이 있어야 "다 받았는지" 를 알 수 있다.
 *
 * <p><b>{@code anchor} 는 페이지 경계를 얼리는 값이다.</b> 첫 요청은 안 보내고, 응답이 준
 * 값을 <b>다음 요청부터 그대로 되돌려주면</b> 그 사이에 새 실행이 생겨도 목록이 안 밀린다.
 * 안 보내면 매 요청이 그 시점 전체를 보므로, {@code OFFSET} 특성상 <b>같은 행이 다시 나오고
 * 뒤쪽 행이 빠진다</b> — 여러 페이지를 이어 붙여 집계하면 수가 틀어진다.
 *
 * <p><b>왜 필요한가.</b> 한때 여기 "전체가 수십 건이라 한 요청에 다 들어온다" 고 적었는데
 * <b>틀린 단정이었다</b>(봇 리뷰가 두 번 짚었다) — {@code verification_runs} 는
 * {@code cleanupJob} 이 <b>의도적으로 안 지우는</b> 이력이고({@code CleanupJdbcAdapter}),
 * 온디맨드 트리거가 하루에도 여러 건을 만든다. 배치 메타 쪽도 보존 창과 크론이 설정값이라
 * 상한이 보장되지 않는다.
 *
 * <p>{@code anchor} 는 <b>그 페이지의 첫 행 id</b> 다 — 목록이 id 내림차순이라 그것이 곧
 * 스냅샷의 최댓값이다. 질의를 하나 더 치지 않는 이유가 그것이다. 목록이 비면 {@code null} 이고,
 * 그때는 되돌려줄 경계도 없다.
 */
public record HistoryPage<T>(List<T> items, int total, int limit, int offset, Long anchor) {

    /** 한 번에 줄 수 있는 최대. 화면이 실수로 큰 값을 보내도 DB 를 오래 잡지 않는다. */
    static final int MAX_LIMIT = 200;
    static final int DEFAULT_LIMIT = 50;

    /** 안 주면 기본값, 범위를 벗어나면 자른다. 400 을 내지 않는 것은 조회라서다. */
    static int pageSize(Integer limit) {
        if (limit == null || limit < 1) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }

    static int pageOffset(Integer offset) {
        return offset == null || offset < 0 ? 0 : offset;
    }

    /**
     * 첫 요청이면 이 페이지의 첫 행 id 를 경계로 삼고, 이미 받은 경계가 있으면 그대로 쓴다.
     *
     * <p><b>요청이 준 값을 이기지 않는다.</b> 뒤 페이지에서 첫 행으로 다시 잡으면 경계가
     * 페이지마다 앞으로 밀려 <b>anchor 를 안 쓴 것과 같아진다.</b>
     */
    static <T> Long anchorOf(Long requested, List<T> items, ToLongFunction<T> id) {
        if (requested != null) {
            return requested;
        }
        return items.isEmpty() ? null : id.applyAsLong(items.getFirst());
    }
}
