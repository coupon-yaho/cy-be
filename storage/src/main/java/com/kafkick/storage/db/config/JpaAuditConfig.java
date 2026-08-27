package com.kafkick.storage.db.config;

import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

import com.kafkick.core.support.TimeProvider;

/**
 * JPA 를 쓰는 모듈만 켠다.
 *
 * <p><b>{@code @EnableJpaAuditing}</b> 은 엔티티가 0개면 <b>JPA metamodel must not be empty</b> 로
 * <b>기동 자체를 세운다</b>. 그래서 JPA 를 안 쓰는 모듈이 끌 수 있게 스위치를 뒀다.
 *
 * <p><b>지금은 아무도 끄지 않는다.</b> 이 스위치의 유일한 사용처가 batch 였는데, CY-245 계보가
 * 들어오면서 엔티티가 생겼고 batch 의 만료 경로가 storage 의 JPA 어댑터를 탄다. 엔티티가
 * 있으므로 metamodel 이 비지 않고, 그 엔티티들이 {@code @CreatedDate} 와
 * {@code AuditingEntityListener} 를 쓰므로 auditing 을 끄면 <b>기동은 되고 쓰기만 실패한다</b>.
 *
 * <p>스위치를 지우지 않고 남겨 두는 이유는, 엔티티를 하나도 안 쓰는 모듈이 나중에 다시 생길 수
 * 있어서다. 끄려는 사람은 그 모듈의 {@code spring.autoconfigure.exclude} 와 <b>함께</b> 판단할 것
 * — 하나만 걷으면 증상이 서로 다른 자리에서 나온다.
 * {@code DomainGaugeConfigContractTest} 가 batch 쪽에서 그 쌍을 지킨다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(value = JpaAuditConfig.AUDITING_ENABLED_PROPERTY, matchIfMissing = true)
@EnableJpaAuditing(dateTimeProviderRef = "auditingDateTimeProvider")
public class JpaAuditConfig {

    /**
     * 이 키는 storage 밖(batch 의 application.yml)에서 꺼진다. 문자열을 저쪽에 옮겨 적으면
     * 여기서 이름을 바꿔도 CI 는 통과하고 기동만 "JPA metamodel must not be empty" 로 죽는다.
     * 그래서 상수로 두고 양쪽 테스트가 이 상수를 참조한다.
     */
    public static final String AUDITING_ENABLED_PROPERTY = "storage.jpa.auditing.enabled";

    /** 기본 CurrentDateTimeProvider 는 현재 시각을 직접 호출하므로 주입된 UTC Clock 을 사용한다. */
    @Bean
    public DateTimeProvider auditingDateTimeProvider(TimeProvider timeProvider) {
        return () -> Optional.of(timeProvider.instant());
    }
}
