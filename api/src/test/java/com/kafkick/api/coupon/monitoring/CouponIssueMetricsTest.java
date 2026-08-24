package com.kafkick.api.coupon.monitoring;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;

import static org.assertj.core.api.Assertions.assertThat;

// 쿠폰 발급 결과별 카운터와 지연 시간이 낮은 카디널리티 태그로 기록되는지 검증합니다.

class CouponIssueMetricsTest {

    private final SimpleMeterRegistry meterRegistry =
            new SimpleMeterRegistry();
    private final CouponIssueMetrics couponIssueMetrics =
            new CouponIssueMetrics(meterRegistry);

    @Test
    @DisplayName("발급 성공 건수와 처리 시간을 기록한다")
    void recordSuccess() {
        couponIssueMetrics.recordSuccess(
                10L,
                20L,
                Duration.ofMillis(250).toNanos()
        );

        assertThat(counter("success").count()).isEqualTo(1);
        assertThat(timer("success").count()).isEqualTo(1);
        assertThat(timer("success").totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(250);
    }

    @Test
    @DisplayName("품절과 중복 발급을 서로 다른 결과로 기록한다")
    void recordKnownBusinessFailures() {
        couponIssueMetrics.recordBusinessFailure(
                10L,
                20L,
                CouponIssueErrorCode.SOLD_OUT,
                Duration.ofMillis(300).toNanos()
        );
        couponIssueMetrics.recordBusinessFailure(
                10L,
                21L,
                CouponIssueErrorCode.ALREADY_ISSUED,
                Duration.ofMillis(400).toNanos()
        );

        assertThat(counter("sold_out").count()).isEqualTo(1);
        assertThat(counter("already_issued").count()).isEqualTo(1);
        assertThat(timer("sold_out").max(
                TimeUnit.MILLISECONDS
        )).isEqualTo(300);
        assertThat(timer("already_issued").max(
                TimeUnit.MILLISECONDS
        )).isEqualTo(400);
    }

    @Test
    @DisplayName("그 밖의 비즈니스 거절과 예상하지 못한 실패를 구분한다")
    void recordRejectedAndUnexpectedFailures() {
        couponIssueMetrics.recordBusinessFailure(
                10L,
                20L,
                CouponIssueErrorCode.GRADE_NOT_ELIGIBLE,
                Duration.ofMillis(10).toNanos()
        );
        couponIssueMetrics.recordUnexpectedFailure(
                10L,
                20L,
                Duration.ofMillis(20).toNanos()
        );

        assertThat(counter("rejected").count()).isEqualTo(1);
        assertThat(counter("error").count()).isEqualTo(1);
    }

    private Counter counter(String outcome) {
        return meterRegistry.get(CouponIssueMetrics.REQUEST_METRIC)
                .tag("outcome", outcome)
                .counter();
    }

    private Timer timer(String outcome) {
        return meterRegistry.get(CouponIssueMetrics.DURATION_METRIC)
                .tag("outcome", outcome)
                .timer();
    }
}
