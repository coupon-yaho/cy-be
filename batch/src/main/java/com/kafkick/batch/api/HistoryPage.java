package com.kafkick.batch.api;

import java.util.List;

/**
 * 이력 한 페이지.
 *
 * <p>{@code total} 을 함께 준다. 화면이 원형 그래프를 그리려면 한 페이지가 아니라 전체를
 * 봐야 하는데, 그 값이 있어야 "다 받았는지" 를 알 수 있다.
 */
public record HistoryPage<T>(List<T> items, int total, int limit, int offset) {

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
