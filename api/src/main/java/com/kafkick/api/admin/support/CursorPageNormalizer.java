package com.kafkick.api.admin.support;

import org.springframework.stereotype.Component;

import com.kafkick.api.admin.support.config.AdminPaginationProperties;

/**
 * cursor 기반 관리자 조회 DTO가 동일한 기본 페이지 크기를 사용하도록 정규화합니다.
 *
 * <p>이 클래스는 누락된 값을 기본값으로 바꾸는 역할만 합니다. 최소·최대 범위 위반은 Query record의
 * Jakarta Validation이 HTTP 400으로 처리하므로 여기서 임의 보정하거나 예외를 던지지 않습니다.</p>
 */
@Component
public final class CursorPageNormalizer {

    private final AdminPaginationProperties properties;

    public CursorPageNormalizer(AdminPaginationProperties properties) {
        this.properties = properties;
    }

    /**
     * limit이 생략된 경우에만 설정된 기본값을 적용합니다.
     *
     * @param limit 요청에서 바인딩된 페이지 크기; 생략하면 null
     * @return 명시된 원래 값 또는 설정된 기본 페이지 크기
     */
    public Integer normalizeLimit(Integer limit) {
        return limit == null ? properties.defaultLimit() : limit;
    }
}
