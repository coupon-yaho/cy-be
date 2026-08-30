package com.kafkick.batch.api;

import java.util.List;

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
 * <p>{@code anchor} 는 <b>그 조건에서 가장 큰 id</b> 다({@code latestExecutionId} ·
 * {@code latestRunId}). ⚠️ <b>페이지의 첫 행으로 대신하면 안 된다</b> — 한때 그렇게 썼는데,
 * 첫 요청이 {@code offset > 0} 이면 그 행은 전체의 최댓값이 아니라 <b>그 페이지의 첫 행</b>이라
 * 경계가 낮게 잡힌다. 그러면 {@code total} 이 그만큼 줄고, 그 경계로 다음 요청을 하면
 * 같은 {@code offset} 이 좁아진 창에 다시 적용돼 행을 건너뛴다(봇 리뷰가 짚었다).
 * 질의 한 번을 아끼려다 답을 틀리게 하는 거래였다. 대상이 없으면 {@code null} 이다.
 *
 * <p><b>경계는 위쪽만 막는다.</b> 아래쪽(오래된 행)이 정리로 사라지는 것은 안 막지만,
 * 배치 메타 정리는 {@code CREATE_TIME} 오름차순으로 <b>가장 오래된 것부터</b> 지우므로
 * {@code id DESC} 목록의 <b>꼬리</b>가 줄 뿐이고 이미 읽은 앞쪽은 안 밀린다(실측).
 * {@code verification_runs} 는 아예 안 지운다. 그래도 순회 중에 {@code total} 이 줄 수는
 * 있다 — 그것까지 막으려면 {@code offset} 을 버리고 {@code id < :lastSeen} 키셋으로 가야 하고,
 * <b>다시 볼 기준</b>은 그때다.
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

}
