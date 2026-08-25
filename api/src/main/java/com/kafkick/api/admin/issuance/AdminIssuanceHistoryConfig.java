package com.kafkick.api.admin.issuance;

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
    public AdminIssuanceHistoryService adminIssuanceHistoryService(
            TimeProvider timeProvider, AdminIssuanceHistoryReader reader) {
        return new AdminIssuanceHistoryService(timeProvider, reader,
                new IssuanceHistoryCalculator(new IssuanceCodeMasker()));
    }
}
