package com.kafkick.batch.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** batch 프로세스가 소유한 L2 회차 조건만 검사한다. */
public final class TopologyValidator {

    private final long expectedGapIntervalMs;

    public TopologyValidator(long expectedGapIntervalMs) {
        this.expectedGapIntervalMs = expectedGapIntervalMs;
    }

    public ValidationResult validate(Topology topology) {
        Objects.requireNonNull(topology, "topology");
        List<Violation> violations = new ArrayList<>();
        if (topology.verificationSchedulingEnabled()) {
            violations.add(new Violation(
                "batch.scheduling.enabled", "false", "true",
                "회차 중 검증 배치가 MySQL을 변경해 측정을 오염시킨다"));
        }
        if (!topology.domainGaugeEnabled()) {
            violations.add(new Violation(
                "observation.domain-gauge.enabled", "true", "false",
                "회차 중 Gauge가 멈추면 측정 시계열을 수집할 수 없다"));
        }
        if (topology.gapCollectionIntervalMs() != expectedGapIntervalMs) {
            violations.add(new Violation(
                "observation.domain-gauge.aggregate-interval-ms",
                Long.toString(expectedGapIntervalMs),
                Long.toString(topology.gapCollectionIntervalMs()),
                "MySQL을 조회하는 유일한 Gauge 주기가 AB-G3와 다르다"));
        }
        if (!Objects.equals(topology.expectedCouponId(), topology.gaugeCouponId())) {
            violations.add(new Violation(
                "observation.domain-gauge.coupon-id",
                Long.toString(topology.expectedCouponId()), String.valueOf(topology.gaugeCouponId()),
                "Gauge가 시작 회차와 다른 쿠폰을 관측해 측정을 오염시킨다"));
        }
        return new ValidationResult(violations);
    }

    public record Topology(
        boolean verificationSchedulingEnabled,
        boolean domainGaugeEnabled,
        long gapCollectionIntervalMs,
        Long gaugeCouponId,
        long expectedCouponId
    ) {
    }

    public record Violation(String key, String expected, String actual, String reason) {
        public Violation {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(expected, "expected");
            Objects.requireNonNull(actual, "actual");
            Objects.requireNonNull(reason, "reason");
        }
    }

    public record ValidationResult(List<Violation> violations) {
        public ValidationResult {
            violations = List.copyOf(violations);
        }

        public boolean valid() {
            return violations.isEmpty();
        }
    }
}
