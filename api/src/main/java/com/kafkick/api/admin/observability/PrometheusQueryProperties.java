package com.kafkick.api.admin.observability;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 관제 API 가 Prometheus 를 읽을 때 쓰는 접속 설정입니다.
 *
 * <p>Prometheus 에는 인증이 없고 호스트에 포트도 열려 있지 않습니다. compose 네트워크 안의
 * 서비스 이름으로만 닿을 수 있고, 이 API 가 화면으로 가는 유일한 통로입니다.</p>
 *
 * <p><b>타임아웃은 요청 1건이 아니라 응답 1장 기준으로 봐야 합니다.</b> 한 응답에 질의가 넷이라
 * 질의별 타임아웃만 두면 최악의 경우 {@code 4 × (connect + read)} 가 걸립니다 — 화면은 1초마다
 * 부르므로 그 사이 요청이 쌓여 API 자신이 부하가 됩니다. 그래서 {@code totalBudget} 이 응답 전체
 * 시간을 자르고, 예산을 넘긴 뒤의 질의는 보내지 않고 {@code UNAVAILABLE} 로 내려보냅니다.</p>
 *
 * @param baseUrl Prometheus HTTP API 주소
 * @param connectTimeout 연결 타임아웃
 * @param readTimeout 응답 타임아웃
 * @param staleAfter 마지막 관측 이후 이 시간이 지나면 값을 STALE 로 내려보낸다
 * @param totalBudget 응답 한 장에 쓸 수 있는 전체 시간. 넘기면 남은 질의를 보내지 않는다
 */
@ConfigurationProperties(prefix = "observation.prometheus")
public record PrometheusQueryProperties(
        String baseUrl, Duration connectTimeout, Duration readTimeout, Duration staleAfter,
        Duration totalBudget) {

    private static final String DEFAULT_BASE_URL = "http://prometheus:9090";
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(200);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofMillis(500);

    /**
     * 수집 경로마다 주기가 달라(재고 1초 · 정합성 30초) 횟수로는 같은 임계를 쓸 수 없다.
     * {@code DomainMeterNames.COLLECT_LAST_SUCCESS_EPOCH} 가 제시한 {@code time() - 값 > 120} 을 따른다.
     */
    private static final Duration DEFAULT_STALE_AFTER = Duration.ofSeconds(120);

    /**
     * 화면의 폴링 간격(1초)보다 짧게 둔다. 응답이 폴링 간격을 넘기면 다음 폴링이 앞 요청을 따라잡아
     * 관제가 스스로 부하가 된다 — 그 구간은 값을 비우는 편이 낫다.
     */
    private static final Duration DEFAULT_TOTAL_BUDGET = Duration.ofMillis(900);

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
        totalBudget = totalBudget == null ? DEFAULT_TOTAL_BUDGET : totalBudget;
        if (totalBudget.isNegative() || totalBudget.isZero()) {
            throw new IllegalArgumentException("total-budget은 양수여야 합니다.");
        }
    }

    private static String stripTrailingSlash(String value) {
        String trimmed = value.trim();
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }
}
