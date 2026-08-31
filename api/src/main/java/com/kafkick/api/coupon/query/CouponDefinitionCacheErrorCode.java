package com.kafkick.api.coupon.query;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.support.exception.ErrorCode;

/**
 * API 힙 캐시가 정의 DB 조회를 제한 시간 안에 끝내지 못했을 때의 HTTP 계약이다.
 *
 * <p>core의 {@code CouponPersistenceErrorCode}는 저장소 일반 실패(500)용이다. 이 코드는
 * stale-if-error 소진 뒤 조회만 503으로 완화하는 API 경계의 의미를 가지므로 api에 둔다.
 *
 * <p><b>두 개의 503 을 가르는 것은 취향이 아니라 귀속의 문제다.</b> 예산 초과와 로더 거절은
 * 이 프로세스 안에서 벌어진 일이고(GC 스톨·스레드풀 포화), DB 는 멀쩡할 수 있다. 하나로
 * 묶어 MySQL 로 보고하면 MySQL 을 건드리지도 않은 Chaos 구간에서 MySQL 실패 수가 오른다.
 */
enum CouponDefinitionCacheErrorCode implements ErrorCode {
    LOAD_UNAVAILABLE(503, "COUPON-501", "쿠폰 목록을 잠시 불러올 수 없습니다.", Dependency.MYSQL),
    LOAD_TIMEOUT(503, "COUPON-503", "쿠폰 목록을 잠시 불러올 수 없습니다.", Dependency.NONE),
    CONTRACT_BROKEN(500, "COUPON-502", "쿠폰 목록 캐시 계약이 올바르지 않습니다.", Dependency.NONE);

    private final int status;
    private final String code;
    private final String message;
    private final Dependency dependency;

    CouponDefinitionCacheErrorCode(int status, String code, String message, Dependency dependency) {
        this.status = status;
        this.code = code;
        this.message = message;
        this.dependency = dependency;
    }
    @Override public int getStatus() { return status; }
    @Override public String getCode() { return code; }
    @Override public String getMessage() { return message; }
    @Override public Dependency dependency() { return dependency; }

    /** 장애 동안 초당 수천 건이 되는 완화 응답이다. 계약 위반(500)만 스택을 남긴다. */
    @Override public boolean logStackTrace() { return this == CONTRACT_BROKEN; }
}
