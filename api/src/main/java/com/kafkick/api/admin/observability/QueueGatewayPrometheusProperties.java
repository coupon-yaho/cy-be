package com.kafkick.api.admin.observability;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 외부 게이트웨이 지표를 관리자 관제에 연결할지와 신선도 기준을 정합니다.
 * {@code stale-after}는 양수여야 하며 0 이하이면 설정 바인딩 중 예외가 발생해 애플리케이션이
 * 기동하지 않습니다.
 */
@ConfigurationProperties(prefix = "observation.prometheus.queue-gateway")
public record QueueGatewayPrometheusProperties(Boolean enabled, Duration staleAfter) {

    public QueueGatewayPrometheusProperties {
        enabled = enabled == null ? false : enabled;
        staleAfter = staleAfter == null ? Duration.ofSeconds(5) : staleAfter;
        if (staleAfter.isZero() || staleAfter.isNegative()) {
            throw new IllegalArgumentException("queue-gateway stale-after는 양수여야 합니다.");
        }
    }
}
