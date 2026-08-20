package com.kafkick.core.coupon.service.result;

public record CouponRoundGenerationResult(
        int creationTargets,
        int createdCount,
        int duplicateCount
) {

    public CouponRoundGenerationResult {
        if (creationTargets < 0
                || createdCount < 0
                || duplicateCount < 0) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 생성 결과 건수는 음수일 수 없습니다."
            );
        }
        if (creationTargets != createdCount + duplicateCount) {
            throw new IllegalArgumentException(
                    "생성 대상 건수와 처리 결과 건수가 일치해야 합니다."
            );
        }
    }
}
