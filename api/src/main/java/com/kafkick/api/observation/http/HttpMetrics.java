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
 * URI 그룹마다 결과 Counter 여섯 종류와 지연 Timer 네 종류를 등록한다.
 *
 * <p><b>[OBS-31] Timer 를 둘에서 넷으로 늘렸다.</b> 이전 javadoc 은 "결과마다 Timer 를 만들면
 * 완료 조건인 성공/실패 두 지연 축과 후속 대시보드 계약이 바뀐다" 며 의도적으로 둘로 제한했다.
 * 뒤집는 근거는 셋이다.
 *
 * <ul>
 *   <li><b>화면 계약이 이미 셋을 요구한다.</b> {@code LatencyMetrics} 가
 *       {@code success · policyReject · systemFailure} 세 자리를 두고 있고, Timer 가 둘이라
 *       뒤의 둘이 값을 못 만들어 조립기가 PENDING 을 고정 반환하고 있었다.</li>
 *   <li><b>실패 하나로 묶으면 화면이 정확히 반대로 읽는다.</b> 정책 거절은 재고 소진 판정으로
 *       끝나 1ms 미만이고 시스템 실패는 타임아웃까지 끌린다. 둘을 한 Timer 에 넣으면 거절이
 *       쏟아질수록 '실패 지연' 이 좋아 보인다.</li>
 *   <li><b>읽던 계열이 깨지지 않는다.</b> {@code outcome="failure"} 를 읽는 코드는 조립기·
 *       시계열·프론트 어디에도 없었다(저장소 전수 확인). 남는 비용은 시계열 개수뿐이다.</li>
 * </ul>
 *
 * <p><b>넷인 이유.</b> 실패를 정책 거절과 시스템 실패 둘로만 가르면 {@link
 * ResultClass#CLIENT_INVALID} 가 갈 곳이 없다. 인증 실패·라우팅 실패·rate limit 이 '정책 거절'
 * 에 섞이면 그 축이 다시 못 믿을 값이 되고, 버리면 4xx 지연 관측이 아예 사라진다. 그래서
 * {@code client_invalid} 도 등록한다 — <b>다만 응답 DTO 에는 노출하지 않는다.</b> 필드는 화면이
 * 요구할 때 붙이면 되지만, 세지 않은 지연은 소급해서 만들 수 없다.
 *
 * <p><b>실패로 세는 범위는 이 변경과 무관하다.</b> 실패<i>율</i>의 분자는 여전히
 * {@link ResultClass#systemFailures()} 하나가 정하고, 그 정의는 여기서도 그대로 쓴다. 지연 축을
 * 넷으로 가르는 것과 실패로 세는 범위는 다른 얘기다 — 섞으면 실패율이 조용히 바뀐다.
 *
 * <p>세부 실패 원인의 분해는 여전히 Timer 가 아니라 결과 Counter 여섯 종류로 본다.
 */
@Component
public final class HttpMetrics {

    private final Map<UriGroup, Map<LatencyOutcome, Timer>> timers = new EnumMap<>(UriGroup.class);
    private final Map<UriGroup, Map<ResultClass, Counter>> counters = new EnumMap<>(UriGroup.class);

    /**
     * 지연 Timer 의 {@code outcome} 라벨 값이다.
     *
     * <p>결과 여섯 분류를 지연 관점에서 넷으로 접는다. <b>접는 규칙은 여기 한 곳에만 있다</b> —
     * 조립기가 라벨 문자열을 따로 적어 두므로, 이름을 바꾸면 그쪽 상수도 함께 바뀌어야 한다.
     * 그 두 곳을 잇는 것은 {@code HttpLatencyOutcomeContractTest} 다.
     */
    public enum LatencyOutcome {
        SUCCESS,
        POLICY_REJECT,
        CLIENT_INVALID,
        SYSTEM_FAILURE;

        public String tagValue() {
            return name().toLowerCase(Locale.ROOT);
        }

        /**
         * 결과 분류가 실릴 지연 축입니다.
         *
         * <p>시스템 실패의 경계는 {@link ResultClass#systemFailures()} 를 그대로 읽습니다 —
         * 여기에 옮겨 적으면 분류가 늘어날 때 한쪽만 고쳐지고, 그때 스냅샷의 지연 축과 실패율의
         * 분자가 서로 다른 집합을 가리킵니다.</p>
         */
        public static LatencyOutcome of(ResultClass resultClass) {
            if (resultClass.isSuccess()) {
                return SUCCESS;
            }
            if (ResultClass.systemFailures().contains(resultClass)) {
                return SYSTEM_FAILURE;
            }
            return resultClass == ResultClass.POLICY_REJECT ? POLICY_REJECT : CLIENT_INVALID;
        }
    }

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

            Map<LatencyOutcome, Timer> byOutcome = new EnumMap<>(LatencyOutcome.class);
            for (LatencyOutcome outcome : LatencyOutcome.values()) {
                byOutcome.put(outcome, timer(meterRegistry, uriGroup, outcome));
            }
            timers.put(uriGroup, byOutcome);
        }
    }

    public void record(UriGroup uriGroup, ResultClass resultClass, long elapsedNanos) {
        counters.get(uriGroup).get(resultClass).increment();
        timers.get(uriGroup).get(LatencyOutcome.of(resultClass))
                .record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    private static Timer timer(MeterRegistry registry, UriGroup uriGroup, LatencyOutcome outcome) {
        return Timer.builder(MeterNames.HTTP_LATENCY)
                .description("HTTP request latency aggregated for dashboard percentiles")
                .tag("uri_group", uriGroup.tagValue())
                .tag("outcome", outcome.tagValue())
                .register(registry);
    }
}
