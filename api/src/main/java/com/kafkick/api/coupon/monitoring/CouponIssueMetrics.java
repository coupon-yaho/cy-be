package com.kafkick.api.coupon.monitoring;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.support.exception.ErrorCode;

// 쿠폰 발급 결과와 응답 지연을 제한된 태그로 기록하고 TRACE 과정 로그를 제공합니다.

@Component
public class CouponIssueMetrics {

    static final String REQUEST_METRIC = "coupon.issue.operation.requests";
    static final String DURATION_METRIC = "coupon.issue.operation.duration";

    private static final String OUTCOME_SUCCESS = "success";
    private static final String OUTCOME_SOLD_OUT = "sold_out";
    private static final String OUTCOME_ALREADY_ISSUED = "already_issued";
    private static final String OUTCOME_REJECTED = "rejected";
    private static final String OUTCOME_ERROR = "error";
    private static final Logger log = LoggerFactory.getLogger(
            CouponIssueMetrics.class
    );

    private final MeterRegistry meterRegistry;
    private final Map<String, Counter> requestCounters;
    private final Map<String, Timer> durationTimers;

    public CouponIssueMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.requestCounters = Map.of(
                OUTCOME_SUCCESS, registerCounter(OUTCOME_SUCCESS),
                OUTCOME_SOLD_OUT, registerCounter(OUTCOME_SOLD_OUT),
                OUTCOME_ALREADY_ISSUED,
                registerCounter(OUTCOME_ALREADY_ISSUED),
                OUTCOME_REJECTED, registerCounter(OUTCOME_REJECTED),
                OUTCOME_ERROR, registerCounter(OUTCOME_ERROR)
        );
        this.durationTimers = Map.of(
                OUTCOME_SUCCESS, registerTimer(OUTCOME_SUCCESS),
                OUTCOME_SOLD_OUT, registerTimer(OUTCOME_SOLD_OUT),
                OUTCOME_ALREADY_ISSUED,
                registerTimer(OUTCOME_ALREADY_ISSUED),
                OUTCOME_REJECTED, registerTimer(OUTCOME_REJECTED),
                OUTCOME_ERROR, registerTimer(OUTCOME_ERROR)
        );
    }

    public void recordStarted(Long couponRoundId, Long memberId) {
        log.trace(
                "쿠폰 발급 시작 couponRoundId={}, memberId={}",
                couponRoundId,
                memberId
        );
    }

    public void recordSuccess(
            Long couponRoundId,
            Long memberId,
            long durationNanos
    ) {
        record(OUTCOME_SUCCESS, durationNanos);
        log.trace(
                "쿠폰 발급 종료 couponRoundId={}, memberId={}, "
                        + "outcome={}, durationMs={}",
                couponRoundId,
                memberId,
                OUTCOME_SUCCESS,
                elapsedMillis(durationNanos)
        );
    }

    public void recordBusinessFailure(
            Long couponRoundId,
            Long memberId,
            ErrorCode errorCode,
            long durationNanos
    ) {
        String outcome = businessOutcome(errorCode);
        record(outcome, durationNanos);
        log.trace(
                "쿠폰 발급 종료 couponRoundId={}, memberId={}, "
                        + "outcome={}, errorCode={}, durationMs={}",
                couponRoundId,
                memberId,
                outcome,
                errorCode.getCode(),
                elapsedMillis(durationNanos)
        );
    }

    public void recordUnexpectedFailure(
            Long couponRoundId,
            Long memberId,
            long durationNanos
    ) {
        record(OUTCOME_ERROR, durationNanos);
        log.trace(
                "쿠폰 발급 종료 couponRoundId={}, memberId={}, "
                        + "outcome={}, durationMs={}",
                couponRoundId,
                memberId,
                OUTCOME_ERROR,
                elapsedMillis(durationNanos)
        );
    }

    private void record(String outcome, long durationNanos) {
        requestCounters.get(outcome).increment();
        durationTimers.get(outcome).record(
                durationNanos,
                TimeUnit.NANOSECONDS
        );
    }

    private Counter registerCounter(String outcome) {
        return Counter.builder(REQUEST_METRIC)
                .description("쿠폰 발급 유즈케이스 실행 결과 수")
                .tag("outcome", outcome)
                .register(meterRegistry);
    }

    private Timer registerTimer(String outcome) {
        return Timer.builder(DURATION_METRIC)
                .description("쿠폰 발급 유즈케이스 처리 시간")
                .tag("outcome", outcome)
                .register(meterRegistry);
    }

    private static String businessOutcome(ErrorCode errorCode) {
        if (errorCode == CouponIssueErrorCode.SOLD_OUT) {
            return OUTCOME_SOLD_OUT;
        }
        if (errorCode == CouponIssueErrorCode.ALREADY_ISSUED) {
            return OUTCOME_ALREADY_ISSUED;
        }
        return OUTCOME_REJECTED;
    }

    private static long elapsedMillis(long durationNanos) {
        return TimeUnit.NANOSECONDS.toMillis(durationNanos);
    }
}
