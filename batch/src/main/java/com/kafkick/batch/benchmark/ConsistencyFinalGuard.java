package com.kafkick.batch.benchmark;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.kafkick.batch.benchmark.TopologyValidator.Violation;
import com.kafkick.core.observation.EngineVersion;

/** FINAL 계산 요청이 batch의 현재 관측 대상과 같은 회차인지 판정합니다. */
public final class ConsistencyFinalGuard {

    private ConsistencyFinalGuard() {
    }

    /** 원시값을 읽기 전에 판정할 수 있는 엔진 버전 계약입니다. */
    public static List<Violation> checkEngineVersion(
            EngineVersion gaugeEngineVersion, EngineVersion requestedEngineVersion) {
        if (gaugeEngineVersion == requestedEngineVersion) {
            return List.of();
        }
        return List.of(new Violation("observation.domain-gauge.engine-version",
                String.valueOf(requestedEngineVersion), String.valueOf(gaugeEngineVersion),
                "batch 관측 엔진과 FINAL 회차 engineVersion이 다릅니다"));
    }

    /**
     * 회차 확정 이후 허용 지연 안에서 읽는지 판정합니다. 원시값을 읽기 전에 판정할 수
     * 있으므로 관측 풀 쿼리보다 앞에 두어야 합니다. 이 창을 넘긴 재실행은 회차와 무관한
     * 시점의 라이브 상태를 그 회차의 verdict 로 굳혀 버립니다.
     */
    public static List<Violation> checkFinalizeWindow(
            Instant readAt, Instant runFinalizedAt, Duration maxLag) {
        if (runFinalizedAt != null && !readAt.isAfter(runFinalizedAt.plus(maxLag))) {
            return List.of();
        }
        return List.of(new Violation("benchmark.consistency.max-observation-lag",
                maxLag.toString(), String.valueOf(runFinalizedAt),
                "회차 확정으로부터 허용 지연을 넘겨 읽었습니다. 이 시점의 값은 그 회차의 값이 아닙니다"));
    }

    /** 읽어 온 원시값의 관측 대상이 요청 회차와 같은지 판정합니다. */
    public static List<Violation> checkCouponId(Long observedCouponId, long requestedCouponId) {
        if (observedCouponId != null && observedCouponId == requestedCouponId) {
            return List.of();
        }
        return List.of(new Violation("observation.domain-gauge.coupon-id",
                String.valueOf(requestedCouponId), String.valueOf(observedCouponId),
                "batch 관측 대상과 FINAL 회차 couponId가 다릅니다"));
    }
}
