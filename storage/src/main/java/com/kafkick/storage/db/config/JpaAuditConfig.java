package com.kafkick.storage.db.config;

import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

import com.kafkick.core.support.TimeProvider;

@Configuration(proxyBeanMethods = false)
@EnableJpaAuditing(dateTimeProviderRef = "auditingDateTimeProvider")
public class JpaAuditConfig {

    /** 기본 CurrentDateTimeProvider 는 현재 시각을 직접 호출하므로 주입된 UTC Clock 을 사용한다. */
    @Bean
    public DateTimeProvider auditingDateTimeProvider(TimeProvider timeProvider) {
        return () -> Optional.of(timeProvider.instant());
    }
}
