package com.kafkick.api.admin.observability;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.kafkick.core.admin.overview.OverviewCalculationPolicy;

/**
 * 관리자 운영현황 판정 임계치 설정입니다.
 *
 * <p>현재 기본값 50%, 2분, 10분, 2분, 10분은 운영 합의 전의 재검토 가능한 정책값입니다.
 * 값의 검증은 Core 정책 생성자가 담당합니다.</p>
 */
@ConfigurationProperties(prefix = "admin.overview.policy")
public class AdminOverviewPolicyProperties {

    private double issuanceDecreaseRatio = 0.50;
    private Duration issuanceStoppedAfter = Duration.ofMinutes(2);
    private Duration queueGuidanceThreshold = Duration.ofMinutes(10);
    private Duration queueAdmissionStoppedAfter = Duration.ofMinutes(2);
    private Duration stockDepletionThreshold = Duration.ofMinutes(10);

    public double getIssuanceDecreaseRatio() {
        return issuanceDecreaseRatio;
    }

    public void setIssuanceDecreaseRatio(double issuanceDecreaseRatio) {
        this.issuanceDecreaseRatio = issuanceDecreaseRatio;
    }

    public Duration getIssuanceStoppedAfter() {
        return issuanceStoppedAfter;
    }

    public void setIssuanceStoppedAfter(Duration issuanceStoppedAfter) {
        this.issuanceStoppedAfter = issuanceStoppedAfter;
    }

    public Duration getQueueGuidanceThreshold() {
        return queueGuidanceThreshold;
    }

    public void setQueueGuidanceThreshold(Duration queueGuidanceThreshold) {
        this.queueGuidanceThreshold = queueGuidanceThreshold;
    }

    public Duration getQueueAdmissionStoppedAfter() {
        return queueAdmissionStoppedAfter;
    }

    public void setQueueAdmissionStoppedAfter(Duration queueAdmissionStoppedAfter) {
        this.queueAdmissionStoppedAfter = queueAdmissionStoppedAfter;
    }

    public Duration getStockDepletionThreshold() {
        return stockDepletionThreshold;
    }

    public void setStockDepletionThreshold(Duration stockDepletionThreshold) {
        this.stockDepletionThreshold = stockDepletionThreshold;
    }

    /** 설정값을 기술 중립 Core 정책으로 변환합니다. */
    public OverviewCalculationPolicy toCorePolicy() {
        return new OverviewCalculationPolicy(
                issuanceDecreaseRatio,
                issuanceStoppedAfter,
                queueGuidanceThreshold,
                queueAdmissionStoppedAfter,
                stockDepletionThreshold);
    }
}
