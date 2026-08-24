package com.kafkick.api.admin.observability;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.kafkick.core.admin.MetricsWindow;

/**
 * {@code GET /metrics/series} 전용 range 조회 예산입니다.
 *
 * <p><b>1초 폴링 예산과 완전히 분리합니다.</b> {@link PrometheusQueryProperties} 가 확정한
 * total-budget 500ms · connect 100ms · read 300ms 는 화면이 1초마다 부르는 {@code /metrics} 를
 * 위한 값입니다. range 는 평가점 수만큼 Prometheus 가 expression 을 다시 계산하므로 instant 대비
 * 평가 시간이 자릿수로 뜁니다 — 같은 예산에 얹으면 우선순위가 뒤인 정합성과 traffic 이 잘립니다.
 * {@link PrometheusArchiveProperties} 가 완료 회차 아카이빙에 같은 이유로 별도 예산을 둔 선례를
 * 따릅니다.</p>
 *
 * <p><b>최악의 응답 시간 관계는 여기서도 강제합니다.</b> 예산은 "새 질의를 시작하지 않는 시점"
 * 이지 "응답이 끝나는 시점" 이 아니므로 최악은 {@code totalBudget + connectTimeout + readTimeout}
 * 이고, 이 합이 폴링 간격을 넘으면 다음 폴링이 앞 요청을 따라잡습니다. 화면이 5~10초 주기로
 * 부르기로 했으므로 짧은 쪽인 5초를 기준으로 검증합니다.</p>
 *
 * <p><b>{@code step} 은 rate 집계 창을 겸합니다.</b> 평가 간격과 집계 창이 다르면 추세 값이
 * 겹치거나 비므로 {@link OverviewPrometheusProperties} 가 {@code current-window == trend-step} 으로
 * 강제한 것과 같은 규칙입니다. scrape 간격(1초)보다 충분히 커야 {@code rate} 가 표본을 둘 이상
 * 봅니다.</p>
 *
 * @param connectTimeout 연결 타임아웃
 * @param readTimeout 응답 타임아웃
 * @param totalBudget 응답 한 장에 쓸 수 있는 전체 시간. 넘기면 남은 계열을 보내지 않는다
 * @param step 평가 간격이자 rate 집계 창
 * @param maxPoints 계열 하나가 가질 수 있는 최대 평가점 수
 */
@ConfigurationProperties(prefix = "observation.prometheus.series")
public record PrometheusSeriesProperties(
        Duration connectTimeout, Duration readTimeout, Duration totalBudget,
        Duration step, Integer maxPoints) {

    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(500);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofMillis(1_500);
    private static final Duration DEFAULT_TOTAL_BUDGET = Duration.ofMillis(2_500);

    /**
     * scrape 간격이 1초이므로 {@code rate(...[5s])} 는 표본을 다섯 개 봅니다. 1~2초로 줄이면
     * 표본이 하나뿐인 순간이 생겨 rate 가 값을 내지 못합니다.
     */
    private static final Duration DEFAULT_STEP = Duration.ofSeconds(5);

    /** 티켓이 가정한 계열당 300점. 15분 창을 5초 간격으로 봐도 181점이라 여유가 있습니다. */
    private static final int DEFAULT_MAX_POINTS = 300;

    /**
     * 화면이 이 경로를 부르기로 한 주기의 짧은 쪽. 최악의 응답 시간이 이 값을 넘으면 다음 폴링이
     * 앞 요청을 따라잡아 관제가 스스로 부하가 된다. 이 상수가 바뀌면 화면 쪽 주기도 함께 바뀐
     * 것이어야 한다.
     *
     * <p><b>설정 키로 열지 않습니다.</b> 이 값은 서버가 정하는 설정이 아니라 <b>화면의 행동에 대한
     * 사실</b>입니다. 바인딩 가능하게 만들면 운영자가 {@code poll-interval: 30s} 로 올려 기동 검증을
     * 통과시킬 수 있는데, 그 사이 화면은 여전히 5초마다 부릅니다 — 불변식이 조용히 거짓이 됩니다.
     * 상수로 두면 바꾸는 일이 코드 변경이 되어 리뷰에 걸리고, 화면 주기를 함께 고쳤는지 확인할 수
     * 있습니다. {@link PrometheusQueryProperties} 도 같은 이유로 1초를 상수로 둡니다.</p>
     */
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(5);

    /** 기본값을 채우고 예산·폴링·평가점 사이의 관계를 기동 시점에 강제합니다. */
    public PrometheusSeriesProperties {
        connectTimeout = connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT : connectTimeout;
        readTimeout = readTimeout == null ? DEFAULT_READ_TIMEOUT : readTimeout;
        totalBudget = totalBudget == null ? DEFAULT_TOTAL_BUDGET : totalBudget;
        step = step == null ? DEFAULT_STEP : step;
        maxPoints = maxPoints == null ? DEFAULT_MAX_POINTS : maxPoints;

        requirePositive(connectTimeout, "series connect-timeout");
        requirePositive(readTimeout, "series read-timeout");
        requirePositive(totalBudget, "series total-budget");
        requirePositive(step, "series step");
        if (step.getNano() != 0) {
            // PromQL 의 구간 표기에 정수 초로 실어야 하므로 소수 초는 표현할 수 없습니다.
            throw new IllegalArgumentException("series step은 정수 초여야 합니다.");
        }
        if (maxPoints <= 0) {
            throw new IllegalArgumentException("series max-points는 양수여야 합니다.");
        }

        Duration worstCase = totalBudget.plus(connectTimeout).plus(readTimeout);
        if (worstCase.compareTo(POLL_INTERVAL) > 0) {
            throw new IllegalArgumentException(
                    "series total-budget + connect-timeout + read-timeout(" + worstCase.toMillis()
                            + "ms)이 폴링 간격(" + POLL_INTERVAL.toMillis() + "ms)을 넘습니다."
                            + " 예산 종료 직전에 시작한 질의가 자기 타임아웃만큼 더 돕니다.");
        }

        // 계약이 두 곳에 걸린다 — 조회 구간은 MetricsWindow 가 정하고 평가점 상한은 여기가 정한다.
        // 어느 한쪽만 바뀌어도 가장 긴 창에서만 조용히 터지므로 기동에서 둘을 함께 본다.
        for (MetricsWindow window : MetricsWindow.values()) {
            if (window.duration().toSeconds() % step.toSeconds() != 0L) {
                throw new IllegalArgumentException(
                        "series step은 모든 MetricsWindow 를 나누어야 합니다: " + window);
            }
            long points = window.duration().toSeconds() / step.toSeconds() + 1L;
            if (points > maxPoints) {
                throw new IllegalArgumentException(
                        window + " 창의 평가점(" + points + ")이 series max-points(" + maxPoints + ")를 넘습니다.");
            }
        }
    }

    /**
     * 기본 설정을 반환합니다. 테스트와 코드 경로가 같은 기본 계약을 재사용합니다.
     *
     * @return 모든 값이 기본값인 설정
     */
    public static PrometheusSeriesProperties defaults() {
        return new PrometheusSeriesProperties(null, null, null, null, null);
    }

    /**
     * 가장 긴 집계 창까지 담을 수 있는 조회 구간 상한을 반환합니다.
     *
     * @return 허용 창 중 가장 긴 것의 길이
     */
    public Duration maxRange() {
        Duration longest = Duration.ZERO;
        for (MetricsWindow window : MetricsWindow.values()) {
            if (window.duration().compareTo(longest) > 0) {
                longest = window.duration();
            }
        }
        return longest;
    }

    private static void requirePositive(Duration value, String name) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "은 양수여야 합니다.");
        }
    }
}
