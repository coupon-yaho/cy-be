package com.kafkick.core.benchmark;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.support.exception.BusinessException;

/**
 * {@link ClientLoadSummary} · {@link ServerLoadSummary} 가 공유하는 값 검증.
 *
 * <p>두 요약을 <b>공통 타입으로 묶지 않고</b> 검증만 공유한다. 타입을 하나로 만들면 인자 순서
 * 하나로 server 값이 client 자리에 들어갈 수 있는데, 그렇게 섞인 회차는 사후에 분리할 방법이 없다.
 * 규칙은 같지만 값은 섞이면 안 된다 — 그 둘을 동시에 만족시키는 자리가 여기다.
 */
final class SummaryValues {

    private SummaryValues() {
    }

    static void validate(String side, long requestCount, long failureCount,
                         double tps, double p95Millis, double p99Millis, Instant measuredAt) {
        Objects.requireNonNull(measuredAt, side + " measuredAt");
        requireNotNegative(side, "requestCount", requestCount);
        requireNotNegative(side, "failureCount", failureCount);
        requireFinite(side, "tps", tps);
        requireFinite(side, "p95Millis", p95Millis);
        requireFinite(side, "p99Millis", p99Millis);
        if (failureCount > requestCount) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,
                    side + " failureCount 가 requestCount 보다 크다: " + failureCount + " > " + requestCount);
        }
        // p99 < p95 는 산식을 잘못 적용했거나 두 값을 다른 표본에서 뽑았다는 뜻이다.
        // 통과시키면 그 회차만 꼬리가 뒤집힌 채 비교표에 들어가고, 눈으로는 안 보인다.
        if (p99Millis < p95Millis) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,
                    side + " p99 가 p95 보다 작다: " + p99Millis + " < " + p95Millis);
        }
    }

    private static void requireNotNegative(String side, String name, long value) {
        if (value < 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,side + " " + name + " 는 0 이상이어야 한다: " + value);
        }
    }

    /**
     * NaN 을 여기서 막는다. 관측 미터에서는 NaN 이 "값 없음" 의 정식 표현이지만, 공식 요약에서는
     * 아니다 — 요약은 통째로 있거나 통째로 없어야 하고, "요약은 올라왔는데 p99 만 NaN" 은
     * 비교표에서 0 과 구분되지 않는 채 흘러간다.
     */
    private static void requireFinite(String side, String name, double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,side + " " + name + " 가 유한한 값이 아니다: " + value);
        }
        if (value < 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,side + " " + name + " 는 0 이상이어야 한다: " + value);
        }
    }
}
