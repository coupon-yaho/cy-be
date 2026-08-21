package com.kafkick.api.admin.observability;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Prometheus instant query 가 돌려준 시계열 하나의 현재 표본입니다.
 *
 * <p>{@code value} 는 {@code NaN} 일 수 있습니다. batch 는 "값 없음" 의 이유를 담을 자리가 없는
 * Prometheus 샘플에 NaN 을 싣고 이유는 짝이 되는 상태 미터로 냅니다. <b>0 으로 바꾸지 않습니다.</b></p>
 *
 * @param metricName {@code __name__} 라벨 값
 * @param labels 나머지 라벨 전부
 * <p><b>{@code evaluatedAt} 은 원천이 값을 관측한 시각이 아닙니다.</b> instant query 는 시계열마다
 * 원래 스크레이프 시각이 아니라 <b>질의 평가 시각</b>을 돌려줍니다 — 즉 언제나 "지금" 입니다.
 * 이것을 {@code ObservedValue.observedAt} 으로 흘려보내면 죽은 원천의 값이 매 폴링마다 갱신되는
 * 것처럼 보이고 STALE 이 구조적으로 나올 수 없게 됩니다. 실제 관측 시각은 신선도 미터
 * ({@code app.observation.collect.last.success.epoch}) 나 {@code timestamp()} 로 따로 물어야 합니다.</p>
 *
 * @param value 표본 값; 원천이 값을 못 냈으면 NaN
 * @param evaluatedAt Prometheus 가 이 질의를 평가한 시각. <b>관측 시각이 아니다.</b>
 */
public record PromSample(String metricName, Map<String, String> labels, double value, Instant evaluatedAt) {

    public PromSample {
        Objects.requireNonNull(metricName, "metricName");
        labels = Map.copyOf(Objects.requireNonNull(labels, "labels"));
        Objects.requireNonNull(evaluatedAt, "evaluatedAt");
    }

    /**
     * 라벨 값을 읽습니다.
     *
     * @param name 라벨 이름
     * @return 라벨 값; 없으면 빈 문자열
     */
    public String label(String name) {
        return labels.getOrDefault(name, "");
    }

    /**
     * 이 표본이 응답에 실을 수 있는 숫자를 들고 있는지 여부입니다.
     *
     * <p>NaN 뿐 아니라 <b>무한대도 값이 아닙니다.</b> 무한대를 통과시키면 {@code Math.round} 가
     * {@code Long.MAX_VALUE} 를 만들어 "값 없음" 이 천문학적 숫자로 응답에 실리고, JSON 으로는
     * 표준 밖 토큰({@code Infinity})이 나가 화면의 파싱이 던집니다. 값이 아닌 것은 값이 아닌
     * 채로 두고 이유는 상태 미터가 냅니다.</p>
     *
     * @return 값이 유한한 숫자이면 true
     */
    public boolean hasNumericValue() {
        return Double.isFinite(value);
    }
}
