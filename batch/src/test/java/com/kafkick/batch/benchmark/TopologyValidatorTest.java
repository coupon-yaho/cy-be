package com.kafkick.batch.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.scheduling.annotation.Scheduled;

import com.kafkick.batch.benchmark.TopologyValidator.Topology;

class TopologyValidatorTest {

    private final TopologyValidator validator = new TopologyValidator(30_000L);

    @Test
    @DisplayName("batch 소유 조건이 맞으면 통과한다")
    void protocolTopologyPasses() {
        assertThat(validator.validate(new Topology(false, true, 30_000, 10L, 10L)).valid()).isTrue();
    }

    @Test
    @DisplayName("batch 소유 위반을 모두 모아 실제값과 함께 반환한다")
    void localViolationsAreAggregated() {
        assertThat(validator.validate(new Topology(true, false, 5_000, 10L, 10L)).violations())
            .extracting("key", "expected", "actual")
            .containsExactly(
                org.assertj.core.groups.Tuple.tuple("batch.scheduling.enabled", "false", "true"),
                org.assertj.core.groups.Tuple.tuple(
                    "observation.domain-gauge.enabled", "true", "false"),
                org.assertj.core.groups.Tuple.tuple(
                    "observation.domain-gauge.aggregate-interval-ms", "30000", "5000")
            );
    }

    @Test
    void differentGaugeCouponIsRejectedByTheValidatorBoundary() {
        assertThat(validator.validate(new Topology(false, true, 30_000, 11L, 10L)).violations())
            .extracting("key", "expected", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple(
                "observation.domain-gauge.coupon-id", "10", "11"));
    }

    @Test
    @DisplayName("batch 검증 경계에는 예약 실행 지점이 없다")
    void benchmarkBoundaryIsNotScheduled() {
        assertThat(java.util.stream.Stream.of(TopologyValidator.class, TopologyPreflightController.class)
                .flatMap(type -> Arrays.stream(type.getDeclaredMethods()))
                .map(Method::getAnnotations)
                .flatMap(Arrays::stream)
                .noneMatch(annotation -> annotation.annotationType() == Scheduled.class))
            .isTrue();
    }
}
