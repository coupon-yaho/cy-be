package com.kafkick.core.admin;

import java.time.Duration;

/**
 * 관리자 지표 조회가 허용하는 고정 집계 구간입니다.
 *
 * <p><b>부르는 경로에 따라 두 가지를 뜻합니다.</b> {@code GET /metrics} 에서는 "얼마나 과거까지
 * 되돌아볼지" 가 아니라 <b>비율을 계산할 집계 창</b>이고, {@code rate(...[5m])} 처럼 질의 안에서만
 * 쓰입니다 — 응답이 한 시점 스냅샷이기 때문입니다. {@code GET /metrics/series} 에서는 그에 더해
 * <b>돌려줄 시계열의 조회 구간</b>을 정합니다.</p>
 *
 * <p><b>화면의 누적 버퍼는 그래도 필요합니다.</b> {@code /metrics/series} 가 채우는 것은 진입
 * 시점의 과거 구간뿐이고, 그 뒤의 1초 갱신은 여전히 화면이 {@code /metrics} 폴링으로 잇습니다.
 * 서버가 시계열 전부를 대신 들고 있는 것이 아닙니다.</p>
 *
 * <p><b>백분위에는 걸리지 않습니다.</b> {@code app.http.latency} 의 백분위 관측 창은 Micrometer
 * expiry(api 의 {@code observation.yml}, 현재 10초)가 정하고 PromQL 로는 바꿀 수 없습니다 —
 * 창을 1m 에서 15m 으로 바꿔도 p99 는 같은 값입니다. 이 사실을 모르면 "창을 바꿨는데 왜 지연이
 * 안 변하나" 를 아무도 설명하지 못합니다.</p>
 */
public enum MetricsWindow {

    ONE_MINUTE(Duration.ofMinutes(1)),
    FIVE_MINUTES(Duration.ofMinutes(5)),
    FIFTEEN_MINUTES(Duration.ofMinutes(15));

    private final Duration duration;

    MetricsWindow(Duration duration) {
        this.duration = duration;
    }

    /**
     * 집계 창의 길이를 반환합니다.
     *
     * @return 집계 창 길이
     */
    public Duration duration() {
        return duration;
    }

    /**
     * 집계 창을 초로 환산합니다. 질의 문자열의 구간 표기({@code [60s]})에 그대로 씁니다.
     *
     * @return 집계 창 길이(초)
     */
    public long seconds() {
        return duration.toSeconds();
    }
}
