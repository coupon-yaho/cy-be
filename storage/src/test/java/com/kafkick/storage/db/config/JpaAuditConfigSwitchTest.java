package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.core.support.TimeProvider;

/**
 * JPA 를 쓰지 않는 모듈(batch)이 감사 설정을 끌 수 있어야 한다. 못 끄면 엔티티가 0개라
 * {@code JPA metamodel must not be empty} 로 <b>기동 자체가</b> 멈춘다.
 *
 * <p>이 계약은 두 파일에 걸쳐 있다 — 스위치를 읽는 쪽은 여기고, 끄는 쪽은
 * {@code batch/application.yml} 이다. batch 쪽은 {@code DomainGaugeConfigContractTest} 가
 * 같은 상수로 확인한다.
 */
class JpaAuditConfigSwitchTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
        .withUserConfiguration(JpaAuditConfig.class);

    @Test
    @DisplayName("스위치를 끄면 감사 설정이 통째로 빠진다")
    void switchOffRemovesTheConfig() {
        runner.withPropertyValues(JpaAuditConfig.AUDITING_ENABLED_PROPERTY + "=false")
            .run(context -> assertThat(context).doesNotHaveBean(JpaAuditConfig.class));
    }

    /**
     * 켜진 상태를 "빈이 있다" 로 확인하지 않는다 — 켜지면 {@code @EnableJpaAuditing} 이
     * EntityManagerFactory 를 요구해 이 슬라이스에서는 기동이 실패한다. 그 실패가 곧 batch 가
     * 만났던 사고이고, 스위치가 기본으로 켜져 있다는 증거다.
     */
    @Test
    @DisplayName("적지 않으면 켜진 상태다 — 엔티티 없는 컨텍스트는 그래서 기동에 실패한다")
    void missingPropertyKeepsAuditingOn() {
        runner.run(context -> assertThat(context)
            .as("스위치가 기본으로 꺼져 있으면 JPA 를 쓰는 api 가 감사 필드를 조용히 잃는다")
            .hasFailed());
    }
}
