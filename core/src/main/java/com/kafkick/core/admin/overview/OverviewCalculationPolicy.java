package com.kafkick.core.admin.overview;

import java.time.Duration;
import java.util.Objects;

/**
 * 관리자 운영현황 O1·O2·O4의 판정 임계치를 명시적으로 전달하는 정책 값입니다.
 *
 * <p>비율은 0 초과 1 이하의 무차원 값이고, 모든 시간은 양수 {@link Duration}입니다. 이 타입은
 * 운영 기본값을 가지지 않으므로 호출자가 원천·환경에 맞는 정책을 반드시 공급합니다.</p>
 *
 * @param issuanceDecreaseRatio 이전 비교 구간 대비 감소로 판정하는 비율; 예를 들어 0.5는 50% 감소
 * @param issuanceStoppedAfter 수요·재고가 있는 발급 중단이 유지되어야 하는 시간
 * @param queueGuidanceThreshold 예상 대기시간이 이를 초과하면 안내 기준 초과로 판정하는 시간
 * @param queueAdmissionStoppedAfter 대기자가 있는 입장 중단이 유지되어야 하는 시간
 * @param stockDepletionThreshold 예상 소진시간이 이내이면 소진 임박으로 집계하는 시간
 */
public record OverviewCalculationPolicy(
        double issuanceDecreaseRatio,
        Duration issuanceStoppedAfter,
        Duration queueGuidanceThreshold,
        Duration queueAdmissionStoppedAfter,
        Duration stockDepletionThreshold) {

    /** 정책 경계가 수치·시간의 유효 범위를 벗어나지 않게 검증합니다. */
    public OverviewCalculationPolicy {
        if (!Double.isFinite(issuanceDecreaseRatio)
                || issuanceDecreaseRatio <= 0.0 || issuanceDecreaseRatio > 1.0) {
            throw new IllegalArgumentException("issuanceDecreaseRatio는 0 초과 1 이하여야 합니다.");
        }
        requirePositive(issuanceStoppedAfter, "issuanceStoppedAfter");
        requirePositive(queueGuidanceThreshold, "queueGuidanceThreshold");
        requirePositive(queueAdmissionStoppedAfter, "queueAdmissionStoppedAfter");
        requirePositive(stockDepletionThreshold, "stockDepletionThreshold");
    }

    /** null과 0 이하 기간을 운영 임계치로 해석하지 않도록 거부합니다. */
    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
    }
}
