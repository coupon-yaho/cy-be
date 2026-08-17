package com.kafkick.api.admin.support;

/**
 * cursor 기반 관리자 조회 DTO가 동일한 기본 페이지 크기를 사용하도록 정규화합니다.
 *
 * <p>이 클래스는 누락된 값을 기본값으로 바꾸는 역할만 합니다. 최소·최대 범위 위반은 Query record의
 * Jakarta Validation이 HTTP 400으로 처리하므로 여기서 임의 보정하거나 예외를 던지지 않습니다.</p>
 */
public final class CursorPageNormalizer {

    /** 발급 문의·이력·이벤트·Benchmark 목록에 공통으로 적용하는 기본 조회 건수입니다. */
    public static final int DEFAULT_LIMIT = 50;

    private CursorPageNormalizer() {
    }

    /**
     * limit이 생략된 경우에만 기본값 50을 적용합니다.
     *
     * @param limit 요청에서 바인딩된 페이지 크기; 생략하면 null
     * @return 명시된 원래 값 또는 기본 페이지 크기 50
     */
    public static Integer normalizeLimit(Integer limit) {
        return limit == null ? DEFAULT_LIMIT : limit;
    }
}
