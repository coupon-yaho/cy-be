package com.kafkick.api.coupon.monitoring;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.api.observation.MeterNames;
import com.kafkick.api.observation.issuance.CouponRoundMeterProperties;
import com.kafkick.api.observation.issuance.CouponRoundMeterRegistry;
import com.kafkick.api.observation.issuance.MeterEventRecorder;
import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;

import static org.assertj.core.api.Assertions.assertThat;

// 발급 결과 수는 EventRecorder가, 서버 처리 시간은 이 컴포넌트가 기록하는지 검증합니다.

class CouponIssueMetricsTest {

    private final SimpleMeterRegistry meterRegistry =
            new SimpleMeterRegistry();
    private final CouponIssueMetrics couponIssueMetrics =
            new CouponIssueMetrics(meterRegistry);

    @Test
    @DisplayName("한 번의 발급 성공은 EventRecorder 카운터와 서버 지연만 한 번 기록한다")
    void recordSuccessOnlyThroughEventRecorderAndKeepDuration() {
        CouponRoundMeterRegistry couponRoundMeters = new CouponRoundMeterRegistry(
                meterRegistry,
                new CouponRoundMeterProperties(null, null, null, null),
                Duration.ofSeconds(10)
        );
        MeterEventRecorder eventRecorder = new MeterEventRecorder(
                couponRoundMeters,
                Duration.ofSeconds(10)
        );
        IssuanceFlowEventFactory eventFactory =
                new IssuanceFlowEventFactory(java.util.UUID::randomUUID);
        IssuanceFlowEvent.Ctx context = context();
        eventRecorder.record(eventFactory.issueAttempt(context));
        eventRecorder.record(eventFactory.issued(
                context,
                100L,
                "ABCDEFGHJKLM2345"
        ));
        couponIssueMetrics.recordSuccess(
                10L,
                20L,
                Duration.ofMillis(250).toNanos()
        );

        assertThat(meterRegistry.find("coupon.issue.operation.requests")
                .counters()).isEmpty();
        assertThat(meterRegistry.find(MeterNames.ISSUANCE_FLOW)
                .tags("coupon_id", "10", "stage", "attempt")
                .counter().count()).isEqualTo(1);
        assertThat(meterRegistry.find(MeterNames.ISSUANCE_FLOW)
                .tags("coupon_id", "10", "stage", "success")
                .counter().count()).isEqualTo(1);
        assertThat(meterRegistry.find(MeterNames.ISSUANCE_OUTCOME)
                .tag("outcome", "ISSUED")
                .counter().count()).isEqualTo(1);
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

        assertThat(meterRegistry.find("coupon.issue.operation.requests")
                .counters()).isEmpty();
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

        assertThat(meterRegistry.find("coupon.issue.operation.requests")
                .counters()).isEmpty();
        assertThat(timer("rejected").count()).isEqualTo(1);
        assertThat(timer("error").count()).isEqualTo(1);
    }

    private Timer timer(String outcome) {
        return meterRegistry.get(CouponIssueMetrics.DURATION_METRIC)
                .tag("outcome", outcome)
                .timer();
    }

    private IssuanceFlowEvent.Ctx context() {
        return new IssuanceFlowEvent.Ctx(
                "request-1",
                20L,
                10L,
                Grade.GOLD,
                false,
                Instant.parse("2026-08-24T05:00:00Z"),
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                901L,
                "api-1"
        );
    }
}
