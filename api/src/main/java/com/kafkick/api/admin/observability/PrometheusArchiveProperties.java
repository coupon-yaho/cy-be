package com.kafkick.api.admin.observability;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 완료 회차 range query 전용 타임아웃. 1초 D2 폴링 예산과 독립적으로 조정한다. */
@ConfigurationProperties(prefix = "observation.prometheus.archive")
public record PrometheusArchiveProperties(Duration connectTimeout, Duration readTimeout) {

    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(10);

    public PrometheusArchiveProperties {
        connectTimeout = connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT : connectTimeout;
        readTimeout = readTimeout == null ? DEFAULT_READ_TIMEOUT : readTimeout;
        if (connectTimeout.isZero() || connectTimeout.isNegative()) {
            throw new IllegalArgumentException("archive connect-timeout은 양수여야 합니다.");
        }
        if (readTimeout.isZero() || readTimeout.isNegative()) {
            throw new IllegalArgumentException("archive read-timeout은 양수여야 합니다.");
        }
    }
}
