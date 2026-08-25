package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.LatencyMetrics;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.observation.MeterNames;
import com.kafkick.api.observation.http.HttpMetrics;
import com.kafkick.api.observation.http.HttpMetrics.LatencyOutcome;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * 지연 축의 {@code outcome} 라벨을 <b>계측이 붙이는 쪽과 조립기가 찾는 쪽 사이에서</b> 고정합니다.
 *
 * <p><b>각각을 따로 보는 테스트로는 이 계약을 지킬 수 없습니다.</b> {@code HttpMetrics} 는
 * {@link LatencyOutcome} 을, {@code PromMetricsAssembler} 는 자기 파일의 문자열 상수를 씁니다.
 * 한쪽 이름만 바뀌어도 컴파일은 통과하고, 조립기 필터가 아무 표본도 못 골라 값이
 * <b>{@code PENDING} 으로 조용히 굳습니다</b> — 화면에서 그것은 "아직 안 만들었다" 와 똑같이
 * 보이므로 예외도 로그도 남지 않습니다.</p>
 *
 * <p>그래서 라벨을 손으로 적지 않고 <b>실제 레지스트리에 기록해 나온 태그</b>를 그대로 조립기에
 * 먹입니다. 두 파일 사이에 문자열이 하나라도 어긋나면 여기가 red 가 됩니다.</p>
 */
class HttpLatencyOutcomeContractTest {

    private static final Instant NOW = Instant.parse("2026-08-25T00:00:00Z");
    private static final TimeProvider FIXED_TIME =
            new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC));
    private static final Duration STALE_AFTER = Duration.ofSeconds(30);
    private static final Duration BUDGET = Duration.ofSeconds(5);

    /**
     * 계측이 결과 여섯 분류를 지연 축 넷으로 접되, <b>어느 분류도 잃지 않는지</b> 봅니다.
     * 하나라도 빠지면 그 지연은 어느 Timer 에도 안 실려 영영 관측되지 않습니다.
     */
    @Test
    @DisplayName("결과 여섯 분류가 지연 축 넷에 빠짐없이 접힌다")
    void everyResultClassFoldsIntoExactlyOneOutcome() {
        Map<LatencyOutcome, Set<ResultClass>> folded = new LinkedHashMap<>();
        for (ResultClass resultClass : ResultClass.values()) {
            folded.computeIfAbsent(LatencyOutcome.of(resultClass), key -> new LinkedHashSet<>())
                    .add(resultClass);
        }

        assertThat(folded.keySet()).containsExactlyInAnyOrder(LatencyOutcome.values());
        assertThat(folded.get(LatencyOutcome.SUCCESS))
                .containsExactlyInAnyOrder(ResultClass.SUCCESS, ResultClass.QUEUE_ACCEPTED);
        assertThat(folded.get(LatencyOutcome.POLICY_REJECT))
                .containsExactly(ResultClass.POLICY_REJECT);
        assertThat(folded.get(LatencyOutcome.CLIENT_INVALID))
                .containsExactly(ResultClass.CLIENT_INVALID);
        // 시스템 실패의 경계는 실패율 분자와 같은 정의를 읽어야 한다. 여기에 따로 적으면
        // 지연 축과 실패율이 서로 다른 집합을 가리키게 된다.
        assertThat(folded.get(LatencyOutcome.SYSTEM_FAILURE))
                .containsExactlyInAnyOrderElementsOf(ResultClass.systemFailures());
    }

    /**
     * <b>이 테스트가 두 파일을 잇습니다.</b> 라벨을 문자열로 적지 않고 레지스트리에서 뽑아
     * 씁니다. 조립기가 다른 이름을 찾고 있으면 세 값이 전부 PENDING 으로 떨어집니다.
     */
    @Test
    @DisplayName("계측이 붙인 outcome 라벨을 조립기가 그대로 찾아 세 값을 만든다")
    void assemblerReadsTheLabelsInstrumentationActuallyWrites() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HttpMetrics metrics = new HttpMetrics(registry);

        // 값을 자릿수로 벌려 둔다. 축이 섞이면 단언이 아니라 수치가 먼저 틀린다.
        metrics.record(UriGroup.ISSUE, ResultClass.SUCCESS, millis(250));
        metrics.record(UriGroup.ISSUE, ResultClass.POLICY_REJECT, millis(1));
        metrics.record(UriGroup.ISSUE, ResultClass.DEPENDENCY_FAILURE, millis(3000));
        metrics.record(UriGroup.ISSUE, ResultClass.CLIENT_INVALID, millis(7));

        List<PromSample> samples = percentileSamplesFrom(registry);
        assertThat(samples).as("레지스트리에서 지연 Timer 를 못 찾았습니다").isNotEmpty();

        LatencyMetrics latency = assemble(samples).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.VALID);
        assertThat(latency.success().value().p99Millis()).isEqualTo(250d);
        assertThat(latency.policyReject().state()).isEqualTo(SourceStatus.VALID);
        assertThat(latency.policyReject().value().p99Millis()).isEqualTo(1d);
        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.VALID);
        assertThat(latency.systemFailure().value().p99Millis()).isEqualTo(3000d);
    }

    /**
     * 정책 거절과 시스템 실패를 한 Timer 에 넣으면 거절이 쏟아질수록 '실패 지연' 이 좋아 보입니다.
     * 이 티켓이 없애려던 것이 정확히 그 화면입니다.
     */
    @Test
    @DisplayName("정책 거절 지연이 시스템 실패 지연을 끌어내리지 않는다")
    void policyRejectsDoNotDiluteSystemFailureLatency() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HttpMetrics metrics = new HttpMetrics(registry);

        for (int i = 0; i < 1000; i++) {
            metrics.record(UriGroup.ISSUE, ResultClass.POLICY_REJECT, millis(1));
        }
        metrics.record(UriGroup.ISSUE, ResultClass.APPLICATION_FAILURE, millis(3000));

        LatencyMetrics latency = assemble(percentileSamplesFrom(registry)).latency();

        assertThat(latency.systemFailure().value().p99Millis()).isEqualTo(3000d);
        assertThat(latency.policyReject().value().p99Millis()).isEqualTo(1d);
    }

    /**
     * {@code client_invalid} 는 계측하되 응답에 자리가 없습니다. 정책 거절 칸으로 새면 인증
     * 실패·라우팅 실패가 '정책상 거절' 로 읽힙니다.
     */
    @Test
    @DisplayName("client_invalid 는 세되 어느 응답 칸으로도 새지 않는다")
    void clientInvalidIsMeasuredButNeverSurfaces() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HttpMetrics metrics = new HttpMetrics(registry);

        metrics.record(UriGroup.ISSUE, ResultClass.CLIENT_INVALID, millis(42));

        assertThat(registry.find(MeterNames.HTTP_LATENCY)
                .tags("uri_group", "issue", "outcome", "client_invalid")
                .timer().count())
                .as("계측이 4xx 지연을 세지 않으면 소급해서 만들 수 없습니다")
                .isEqualTo(1);

        LatencyMetrics latency = assemble(percentileSamplesFrom(registry)).latency();

        assertThat(latency.success().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.policyReject().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.systemFailure().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(latency.groups()).allSatisfy(stat ->
                assertThat(stat.percentiles().state()).isEqualTo(SourceStatus.PENDING));
    }

    // ── 지원 ────────────────────────────────────────────────────────────────────

    private static long millis(long value) {
        return TimeUnit.MILLISECONDS.toNanos(value);
    }

    /**
     * 레지스트리의 지연 Timer 를 Prometheus 가 낼 {@code quantile} 표본 모양으로 옮깁니다.
     *
     * <p>백분위 자체는 값을 옮기지 않고 <b>기록한 최댓값</b>을 세 분위에 그대로 씁니다 —
     * 이 테스트가 보는 것은 분위 계산이 아니라 <b>라벨</b>이기 때문입니다. 라벨은 손으로 적지
     * 않고 {@link Meter.Id} 에서 꺼냅니다.</p>
     */
    private static List<PromSample> percentileSamplesFrom(SimpleMeterRegistry registry) {
        List<PromSample> samples = new ArrayList<>();
        for (Timer timer : registry.find(MeterNames.HTTP_LATENCY).timers()) {
            if (timer.count() == 0) {
                continue;
            }
            double seconds = timer.max(TimeUnit.NANOSECONDS) / 1_000_000_000d;
            for (String quantile : List.of("0.5", "0.95", "0.99")) {
                Map<String, String> labels = new LinkedHashMap<>();
                timer.getId().getTags().forEach(tag -> labels.put(tag.getKey(), tag.getValue()));
                labels.put("quantile", quantile);
                labels.put("instance", "api-1");
                samples.add(new PromSample(
                        MetricAggregation.HTTP_LATENCY_SECONDS, labels, seconds, NOW));
            }
        }
        return samples;
    }

    private static AdminMetricsResponse assemble(List<PromSample> latencySamples) {
        PromQuery client = new FakePromQuery(promQl -> {
            if (promQl.contains("quantile!=")) {
                return latencySamples;
            }
            if (promQl.contains("timestamp(")) {
                return List.of(new PromSample("", Map.of(), 3L, NOW));
            }
            return List.of();
        });
        return new PromMetricsAssembler(client, FIXED_TIME, STALE_AFTER, BUDGET)
                .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, null, null));
    }
}
