package com.kafkick.api.admin.issuance;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySourceReader;
import com.kafkick.core.support.exception.BusinessException;

/** 관리자 회원 발급 문의의 관측 비활성 경계를 API 실행 환경에 조립합니다. */
@Configuration(proxyBeanMethods = false)
public class AdminIssuanceInquiryConfig {

    /** 관측이 꺼졌을 때 가짜 행 대신 명시적인 503 오류를 반환하는 Reader를 등록합니다. */
    @Bean
    @ConditionalOnProperty(
            name = "observation.datasource.enabled",
            havingValue = "false",
            matchIfMissing = true
    )
    public AdminIssuanceInquirySourceReader unavailableAdminIssuanceInquirySourceReader() {
        return (query, snapshotAt) -> {
            throw new BusinessException(AdminApiErrorCode.OBSERVATION_DISABLED);
        };
    }
}
