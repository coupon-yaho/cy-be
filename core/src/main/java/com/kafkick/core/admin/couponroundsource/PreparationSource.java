package com.kafkick.core.admin.couponroundsource;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.SourceStatus;

/** DB에서 독립적으로 판정한 쿠폰 회차 설정·재고와 Redis 비교용 설정값 준비 원천입니다. */
public record PreparationSource(
        Boolean couponRoundConfigurationReady,
        Boolean databaseStockReady,
        CouponPolicyType policyType,
        Integer eligibleGradesMask,
        SourceStatus status,
        Instant observedAt
) {

    /**
     * 값 보유 상태와 두 DB 준비 판정·관측 시각의 조합을 검증합니다.
     *
     * @throws NullPointerException {@code status}가 {@code null}인 경우
     * @throws IllegalArgumentException 상태와 값 조합이 맞지 않거나 설정 완료 등급 마스크가 유효하지 않은 경우
     */
    public PreparationSource {
        Objects.requireNonNull(status, "status");
        if (status.carriesValue()) {
            if (couponRoundConfigurationReady == null || databaseStockReady == null || observedAt == null
                    || (couponRoundConfigurationReady
                    && (policyType == null || eligibleGradesMask == null))) {
                throw new IllegalArgumentException(
                        status + " 준비 원천에는 두 판정과 유효 설정의 policyType, 등급 마스크,"
                                + " observedAt이 필요합니다.");
            }
            if (couponRoundConfigurationReady) {
                // 설정 완료는 실제 발급 도메인이 복원할 수 있는 비어 있지 않은 비트 조합이어야 합니다.
                MembershipGrade.fromMask(eligibleGradesMask);
            }
        } else if (couponRoundConfigurationReady != null || databaseStockReady != null
                || policyType != null || eligibleGradesMask != null || observedAt != null) {
            throw new IllegalArgumentException(
                    status + " 준비 원천의 판정과 설정값, observedAt은 null이어야 합니다.");
        }
    }
}
