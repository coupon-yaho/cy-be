package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.core.observation.DomainMeterNames;

/** 지표별 집계 규칙표와 축약 의미를 검증합니다. 이 표가 계약입니다. */
class MetricAggregationTest {

    /**
     * 규칙이 패널마다 달라지면 화면의 숫자가 무슨 의미인지 아무도 설명하지 못합니다.
     * 특히 사용률을 sum 하면 200% 가 나옵니다.
     */
    @Test
    @DisplayName("집계 규칙표가 확정된 근거대로 고정돼 있다")
    void ruleTableIsFixed() {
        assertThat(MetricAggregation.of(MetricAggregation.HTTP_LATENCY_SECONDS))
                .isEqualTo(MetricAggregation.MAX);
        assertThat(MetricAggregation.of(MetricAggregation.HTTP_RESULT_TOTAL))
                .isEqualTo(MetricAggregation.SUM);
        assertThat(MetricAggregation.of(MetricAggregation.HTTP_IN_FLIGHT))
                .isEqualTo(MetricAggregation.SUM);
        assertThat(MetricAggregation.of(MetricAggregation.HIKARI_ACTIVE))
                .isEqualTo(MetricAggregation.SUM);
        assertThat(MetricAggregation.of(MetricAggregation.HIKARI_PENDING))
                .isEqualTo(MetricAggregation.SUM);
        assertThat(MetricAggregation.of(MetricAggregation.HIKARI_POOL_UTILIZATION))
                .isEqualTo(MetricAggregation.MAX);
        assertThat(MetricAggregation.of(MetricAggregation.CPU_USAGE))
                .isEqualTo(MetricAggregation.MAX);
        assertThat(MetricAggregation.of(MetricAggregation.JVM_MEMORY_USED))
                .isEqualTo(MetricAggregation.MAX);
        assertThat(MetricAggregation.of(MetricAggregation.CONSISTENCY_GAP))
                .isEqualTo(MetricAggregation.SINGLE);
        assertThat(MetricAggregation.of(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH))
                .isEqualTo(MetricAggregation.SINGLE);
        // 인스턴스 하나가 스크레이프에서 빠졌을 때 드러나야 하므로 가장 오래된 나이를 쓴다.
        assertThat(MetricAggregation.of(MetricAggregation.HTTP_FRESHNESS_AGE_SECONDS))
                .isEqualTo(MetricAggregation.MAX);
        // 정원은 절대 개수라 더한다. 사용률은 더하면 200% 가 나오므로 최댓값이다.
        assertThat(MetricAggregation.of(MetricAggregation.HIKARI_MAX))
                .isEqualTo(MetricAggregation.SUM);
        assertThat(MetricAggregation.of(MetricAggregation.TOMCAT_BUSY))
                .isEqualTo(MetricAggregation.SUM);
        assertThat(MetricAggregation.of(MetricAggregation.TOMCAT_MAX))
                .isEqualTo(MetricAggregation.SUM);
        assertThat(MetricAggregation.of(MetricAggregation.TOMCAT_THREAD_UTILIZATION))
                .isEqualTo(MetricAggregation.MAX);
        assertThat(MetricAggregation.of(MetricAggregation.JVM_MEMORY_MAX))
                .isEqualTo(MetricAggregation.MAX);
        assertThat(MetricAggregation.of(MetricAggregation.JVM_HEAP_UTILIZATION))
                .isEqualTo(MetricAggregation.MAX);
        // 전역 합과 인스턴스 최댓값은 같은 미터의 다른 축약이라 규칙이 갈린다.
        assertThat(MetricAggregation.of(MetricAggregation.HTTP_IN_FLIGHT_INSTANCE_MAX))
                .isEqualTo(MetricAggregation.MAX);
        // up 을 더하면 살아 있는 인스턴스 수가 된다.
        assertThat(MetricAggregation.of(MetricAggregation.UP)).isEqualTo(MetricAggregation.SUM);
        // 대기열은 batch 한 곳에서만 나온다. 표본이 둘이면 배선이 잘못된 것이다.
        assertThat(MetricAggregation.of(MetricAggregation.QUEUE_LENGTH))
                .isEqualTo(MetricAggregation.SINGLE);
        assertThat(MetricAggregation.of(MetricAggregation.QUEUE_LENGTH_STATE))
                .isEqualTo(MetricAggregation.SINGLE);
        assertThat(MetricAggregation.of(MetricAggregation.OBSERVED_COUPON_ID))
                .isEqualTo(MetricAggregation.SINGLE);
    }

    /**
     * 접미사를 빼면 표본이 0 개인데 예외도 로그도 나지 않습니다. 이름의 근거는
     * {@code TomcatMeterSwitchContractTest} 가 실제 레지스트리로 잽니다.
     */
    @Test
    @DisplayName("tomcat 이름은 미터 이름에 base unit 접미사를 붙인 것이다")
    void tomcatNamesCarryBaseUnitSuffix() {
        assertThat(MetricAggregation.TOMCAT_BUSY)
                .isEqualTo(MeterNames.TOMCAT_BUSY.replace('.', '_') + "_threads");
        assertThat(MetricAggregation.TOMCAT_MAX)
                .isEqualTo(MeterNames.TOMCAT_MAX.replace('.', '_') + "_threads");
        assertThat(MetricAggregation.HIKARI_MAX)
                .isEqualTo(MeterNames.HIKARI_MAX.replace('.', '_'));
        assertThat(MetricAggregation.JVM_MEMORY_MAX)
                .isEqualTo(MeterNames.JVM_MEMORY_MAX.replace('.', '_') + "_bytes");
        assertThat(MetricAggregation.QUEUE_LENGTH)
                .isEqualTo(DomainMeterNames.QUEUE_LENGTH.replace('.', '_'));
    }

    /** 이름을 옮겨 적으면 한쪽만 바뀌어도 예외 없이 "값 없음" 만 옵니다. */
    @Test
    @DisplayName("규칙표의 이름은 미터 이름 상수에서 나온다")
    void namesComeFromMeterNameConstants() {
        assertThat(MetricAggregation.HTTP_LATENCY_SECONDS)
                .isEqualTo(MeterNames.HTTP_LATENCY.replace('.', '_') + "_seconds");
        assertThat(MetricAggregation.HTTP_RESULT_TOTAL)
                .isEqualTo(MeterNames.HTTP_RESULT.replace('.', '_') + "_total");
        assertThat(MetricAggregation.CONSISTENCY_GAP_STATE)
                .isEqualTo(DomainMeterNames.CONSISTENCY_GAP_STATE.replace('.', '_'));
    }

    /** 표에 없는 지표를 패널이 임의로 집계하지 못하게 막습니다. */
    @Test
    @DisplayName("규칙이 없는 지표는 기본값을 고르지 않고 거부한다")
    void unknownMetricIsRejected() {
        assertThatThrownBy(() -> MetricAggregation.of("app_something_new"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("SUM 은 인스턴스 값을 더하고 MAX 는 가장 위험한 인스턴스를 고른다")
    void reducesByRule() {
        List<PromSample> samples = List.of(sample(3d), sample(7d));
        assertThat(MetricAggregation.SUM.reduce(samples)).hasValue(10d);
        assertThat(MetricAggregation.MAX.reduce(samples)).hasValue(7d);
    }

    /** NaN 은 값이 아니라 "이유는 상태 미터가 낸다" 는 표시입니다. */
    @Test
    @DisplayName("NaN 표본은 셈에서 빠지고 0으로 취급되지 않는다")
    void nanIsNotZero() {
        assertThat(MetricAggregation.SUM.reduce(List.of(sample(4d), sample(Double.NaN)))).hasValue(4d);
        assertThat(MetricAggregation.SUM.reduce(List.of(sample(Double.NaN)))).isEmpty();
    }

    /** 숫자 표본이 없으면 0 이 아니라 빈 값입니다 — 0 은 화면이 그리는 거짓말이 됩니다. */
    @Test
    @DisplayName("표본이 없으면 0이 아니라 빈 값이다")
    void emptyIsNotZero() {
        assertThat(MetricAggregation.SUM.reduce(List.of())).isEmpty();
        assertThat(MetricAggregation.MAX.reduce(List.of())).isEmpty();
    }

    /** 정합성 gap 은 batch 한 곳에서만 나옵니다. 둘이 오면 배선이 잘못된 것입니다. */
    @Test
    @DisplayName("SINGLE 규칙에 표본이 둘 이상이면 하나를 고르지 않고 실패한다")
    void singleRejectsMultipleSources() {
        assertThatThrownBy(() -> MetricAggregation.SINGLE.reduce(List.of(sample(1d), sample(2d))))
                .isInstanceOf(IllegalStateException.class);
    }

    private static PromSample sample(double value) {
        return new PromSample("m", Map.of(), value, Instant.parse("2026-08-21T00:00:00Z"));
    }
}
