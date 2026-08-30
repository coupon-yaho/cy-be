package com.kafkick.api.queuegateway;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 외부 대기열 게이트웨이에 상태를 공급하는 주기와 인스턴스별 안전 상한입니다. */
@ConfigurationProperties(prefix = "queue.gateway.publisher")
public record QueueGatewayPublisherProperties(
        Boolean enabled,
        Duration capacityInterval,
        Duration couponRoundInterval,
        Long creditsPerSecond,
        String instanceId
) {

    public QueueGatewayPublisherProperties {
        enabled = enabled == null ? false : enabled;
        capacityInterval = capacityInterval == null ? Duration.ofSeconds(1) : capacityInterval;
        couponRoundInterval = couponRoundInterval == null ? Duration.ofSeconds(5) : couponRoundInterval;
        creditsPerSecond = creditsPerSecond == null ? 0L : creditsPerSecond;
        instanceId = instanceId == null ? "api-local" : instanceId;
        if (capacityInterval.isZero() || capacityInterval.isNegative()
                || couponRoundInterval.isZero() || couponRoundInterval.isNegative()) {
            throw new IllegalArgumentException("게이트웨이 상태 공급 주기는 양수여야 합니다.");
        }
        if (creditsPerSecond < 0L || creditsPerSecond > 10_000L
                || (enabled && creditsPerSecond == 0L)) {
            throw new IllegalArgumentException(
                    "활성화된 게이트웨이 공급기의 credits-per-second는 1 이상 10000 이하여야 합니다.");
        }
        if (instanceId.isBlank() || instanceId.length() > 100) {
            throw new IllegalArgumentException("instance-id는 1자 이상 100자 이하여야 합니다.");
        }
    }
}
