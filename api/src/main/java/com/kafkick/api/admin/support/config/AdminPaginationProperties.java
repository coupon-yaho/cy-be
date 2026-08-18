package com.kafkick.api.admin.support.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** 관리자 cursor 조회의 공통 페이지 기본값입니다. */
@ConfigurationProperties(prefix = "admin.pagination")
public record AdminPaginationProperties(@DefaultValue("50") int defaultLimit) {

    public AdminPaginationProperties {
        if (defaultLimit < 1 || defaultLimit > 200) {
            throw new IllegalArgumentException("admin.pagination.default-limit은 1 이상 200 이하여야 합니다.");
        }
    }
}
