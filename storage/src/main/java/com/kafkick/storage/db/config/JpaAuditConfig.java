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
 * <p>{@code @EnableJpaAuditing} 은 엔티티가 0개면 <b>JPA metamodel must not be empty</b> 로
 * <b>기동 자체를 세운다</b>. batch 는 엔티티를 만들지 않고 JDBC 와 Spring Batch 로만 도는데,
 * storage 를 얹었다는 이유만으로 이 설정을 물려받는다. 그래서 기본값은 켜 두되(api 는 그대로)
 * JPA 를 안 쓰는 모듈이 끌 수 있게 스위치를 둔다.
 *
 * <p>TODO(엔티티 도입 담당): 지금 저장소에는 {@code @Entity} 가 하나도 없다
 * ({@code BaseEntity}·{@code UpdatableEntity} 는 {@code @MappedSuperclass} 라 관리 타입으로
 * 세어지지 않는다). 첫 엔티티가 생기면 <b>api 는 아무 변화 없이</b> 그대로 돌지만, batch 는
 * 그때 다시 판단해야 한다 — 배치가 여전히 JPA 를 안 쓴다면 이 스위치를 그대로 두고, 쓰기
 * 시작한다면 batch 의 {@code storage.jpa.auditing.enabled} 와 JPA 자동설정 제외를 함께 걷는다.
 * 그 시점은 {@code BatchJpaFreeAssumptionTest} 가 알려 준다.
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
