package com.kafkick.storage.db.notification.repository;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * outbox 어댑터가 미터를 갖게 되면서(CY-908) 필요해진 배선.
 *
 * <p>{@code @DataJpaTest} 는 웹·관측 자동설정을 뺀 얇은 컨텍스트라 {@link MeterRegistry}
 * 가 없다. 붙이지 않으면 어댑터가 아예 안 뜬다.
 *
 * <p><b>대역이 아니라 진짜 {@link SimpleMeterRegistry} 다.</b> 미터 이름과 태그가 실제로
 * 등록되는지 읽어서 확인할 수 있어야 하는데, 모의 객체로는 이름이 틀려도 통과한다.
 *
 * <p>{@code @Configuration} 이 아니라 {@code @TestConfiguration} 인 이유는 형제
 * {@code NotificationResendTransactionIntegrationTest.Config} 와 같다 — 중첩·독립
 * {@code @Configuration} 은 테스트의 <b>주 설정</b>으로 잡혀 자동설정 기준 패키지 탐색을
 * 밀어낸다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class OutboxMeterTestConfig {

    @Bean
    MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean
    NotificationOutboxMeter notificationOutboxMeter(MeterRegistry registry) {
        return new NotificationOutboxMeter(registry);
    }
}
