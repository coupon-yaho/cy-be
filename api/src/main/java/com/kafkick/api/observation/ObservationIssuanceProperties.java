package com.kafkick.api.observation;

import com.kafkick.api.observation.issuance.IssuanceObservationService;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 발급 관측의 운영 임계치를 외부 설정에서 읽습니다.
 *
 * <p>{@code @Value}가 아니라 {@link ConfigurationProperties}를 쓰는 이유 — {@code @Value}는
 * 문자열 {@code "10s"}를 {@link Duration}으로 바꾸지 못해 기동이 실패합니다(실측). 바인딩
 * 경로가 달라서이며, 같은 이유로 {@code ConsistencySeverityProperties}도 이 방식입니다.
 *
 * @param attemptFailureLogInterval 발급 시도 기록 실패를 다시 로그로 남기기까지의 최소 간격;
 *                                  생략하면 {@link IssuanceObservationService#DEFAULT_LOG_INTERVAL}
 */
@ConfigurationProperties(prefix = "observation.issuance")
public record ObservationIssuanceProperties(Duration attemptFailureLogInterval) {

    /**
     * 설정값이 없으면 기본 간격을 돌려줍니다.
     *
     * @return 실제로 사용할 실패 로그 유량 제한 간격
     */
    public Duration resolvedAttemptFailureLogInterval() {
        return attemptFailureLogInterval == null
                ? IssuanceObservationService.DEFAULT_LOG_INTERVAL
                : attemptFailureLogInterval;
    }
}
