package com.kafkick.api.admin.issuance;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryReader;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryService;
import com.kafkick.core.admin.issuancehistory.IssuanceCodeMasker;
import com.kafkick.core.admin.issuancehistory.IssuanceHistoryCalculator;
import com.kafkick.core.support.TimeProvider;

/** API는 기술 중립 Reader 계약으로 관리자 이력 Service를 조립합니다. */
@Configuration(proxyBeanMethods = false)
public class AdminIssuanceHistoryConfig {
    /** 실제 Reader와 Core 계산기를 관리자 이력 Service로 조립합니다. */
    @Bean
    @ConditionalOnMissingBean(AdminIssuanceHistoryService.class)
    public AdminIssuanceHistoryService adminIssuanceHistoryService(
            TimeProvider timeProvider, AdminIssuanceHistoryReader reader) {
        // Storage가 제공한 실제 Reader만 조립하며, 빈 목록으로 대체하는 fallback은 만들지 않습니다.
        return new AdminIssuanceHistoryService(timeProvider, reader,
                new IssuanceHistoryCalculator(new IssuanceCodeMasker()));
    }
}
