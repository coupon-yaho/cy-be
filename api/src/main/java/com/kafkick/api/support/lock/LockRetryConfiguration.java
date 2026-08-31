package com.kafkick.api.support.lock;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 락 재시도 설정을 <b>제 패키지에서</b> 켠다.
 *
 * <p>전에는 {@code ApiObservationAutoConfiguration} 의
 * {@code @EnableConfigurationProperties} 가 유일한 등록 지점이었다. 발급만 쓰던 동안에는
 * 그럴듯했지만, 지금 이 값은 <b>사용·사용취소·발급취소 세 컨트롤러의 기동 조건</b>이다 —
 * 없으면 {@link LockContentionRetry} → {@code CouponOperationRetryingExecutor} →
 * 컨트롤러 셋이 연쇄로 못 뜬다.
 *
 * <p>관측 자동설정에 클래스 수준 조건이 없어 지금은 항상 적용되므로 실제 고장은 없다.
 * 다만 {@code spring.autoconfigure.exclude} 로 관측을 빼는 순간 <b>쿠폰 사용이 안 뜬다</b> —
 * 그 인과가 아무 데도 안 적혀 있는 것이 문제라 자리를 옮긴다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(LockRetryProperties.class)
public class LockRetryConfiguration {
}
