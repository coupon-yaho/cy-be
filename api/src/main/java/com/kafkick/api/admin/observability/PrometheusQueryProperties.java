package com.kafkick.api.admin.observability;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 관제 API 가 Prometheus 를 읽을 때 쓰는 접속 설정입니다.
 *
 * <p>Prometheus 에는 인증이 없고 호스트에 포트도 열려 있지 않습니다. compose 네트워크 안의
 * 서비스 이름으로만 닿을 수 있고, 이 API 가 화면으로 가는 유일한 통로입니다.</p>
 *
 * <p>타임아웃은 화면의 1초 폴링보다 짧아야 합니다. 길게 두면 Prometheus 가 느려질 때 관제
 * 요청이 쌓여 API 자신이 부하가 됩니다 — 그 구간은 {@code UNAVAILABLE} 로 내려보내는 편이 낫습니다.</p>
 *
 * @param baseUrl Prometheus HTTP API 주소
 * @param connectTimeout 연결 타임아웃
 * @param readTimeout 응답 타임아웃
 * @param staleAfter 마지막 관측 이후 이 시간이 지나면 값을 STALE 로 내려보낸다
 */
@ConfigurationProperties(prefix = "observation.prometheus")
public record PrometheusQueryProperties(
        String baseUrl, Duration connectTimeout, Duration readTimeout, Duration staleAfter) {

    private static final String DEFAULT_BASE_URL = "http://prometheus:9090";
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(200);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofMillis(500);

    /**
     * 수집 경로마다 주기가 달라(재고 1초 · 정합성 30초) 횟수로는 같은 임계를 쓸 수 없다.
     * {@code DomainMeterNames.COLLECT_LAST_SUCCESS_EPOCH} 가 제시한 {@code time() - 값 > 120} 을 따른다.
     */
    private static final Duration DEFAULT_STALE_AFTER = Duration.ofSeconds(120);

    public PrometheusQueryProperties {
        baseUrl = (baseUrl == null || baseUrl.isBlank()) ? DEFAULT_BASE_URL : stripTrailingSlash(baseUrl);
        connectTimeout = connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT : connectTimeout;
        readTimeout = readTimeout == null ? DEFAULT_READ_TIMEOUT : readTimeout;
        if (connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("connect-timeout은 양수여야 합니다.");
        }
        if (readTimeout.isNegative() || readTimeout.isZero()) {
            throw new IllegalArgumentException("read-timeout은 양수여야 합니다.");
        }
        staleAfter = staleAfter == null ? DEFAULT_STALE_AFTER : staleAfter;
        if (staleAfter.isNegative() || staleAfter.isZero()) {
            throw new IllegalArgumentException("stale-after는 양수여야 합니다.");
        }
    }

    private static String stripTrailingSlash(String value) {
        String trimmed = value.trim();
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }
}
