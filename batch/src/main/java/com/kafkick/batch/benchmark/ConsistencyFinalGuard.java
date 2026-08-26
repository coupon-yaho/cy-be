package com.kafkick.batch.benchmark;

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
