package com.kafkick.api.observation.http;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;

/**
 * URI 그룹마다 결과 Counter 여섯 종류와 성공/실패 Timer 두 종류를 등록한다.
 *
 * <p>정책 거절과 시스템 실패의 비율은 Counter 여섯 종류로 분리하되 Timer는 두 개만 둔다.
 * 결과마다 Timer를 만들면 완료 조건인 성공/실패 두 지연 축과 후속 대시보드 계약이 바뀐다.
 * 세부 실패 원인의 분해는 Timer가 아니라 결과 Counter로 본다.
 */
@Component
public final class HttpMetrics {

    private final Map<UriGroup, Map<ResultClass, Counter>> counters = new EnumMap<>(UriGroup.class);
    private final Map<UriGroup, Timer> successTimers = new EnumMap<>(UriGroup.class);
    private final Map<UriGroup, Timer> failureTimers = new EnumMap<>(UriGroup.class);

    public HttpMetrics(MeterRegistry meterRegistry) {
        for (UriGroup uriGroup : UriGroup.values()) {
            Map<ResultClass, Counter> byResult = new EnumMap<>(ResultClass.class);
            for (ResultClass resultClass : ResultClass.values()) {
                byResult.put(resultClass, Counter.builder(MeterNames.HTTP_RESULT)
                        .description("HTTP request outcomes")
                        .tag("uri_group", uriGroup.tagValue())
                        .tag("result", resultClass.name().toLowerCase(Locale.ROOT))
                        .register(meterRegistry));
            }
            counters.put(uriGroup, byResult);
            successTimers.put(uriGroup, timer(meterRegistry, uriGroup, "success"));
            failureTimers.put(uriGroup, timer(meterRegistry, uriGroup, "failure"));
        }
    }

    public void record(UriGroup uriGroup, ResultClass resultClass, long elapsedNanos) {
        counters.get(uriGroup).get(resultClass).increment();
        Map<UriGroup, Timer> timers = resultClass.isSuccess() ? successTimers : failureTimers;
        timers.get(uriGroup).record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    private static Timer timer(MeterRegistry registry, UriGroup uriGroup, String outcome) {
        return Timer.builder(MeterNames.HTTP_LATENCY)
                .description("HTTP request latency aggregated for dashboard percentiles")
                .tag("uri_group", uriGroup.tagValue())
                .tag("outcome", outcome)
                .register(registry);
    }
}
