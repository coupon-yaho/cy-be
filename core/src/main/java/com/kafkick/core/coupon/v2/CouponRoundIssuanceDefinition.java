package com.kafkick.core.coupon.v2;

import com.kafkick.core.observation.EngineVersion;

/**
 * 오픈 중 바뀌지 않는 회차별 발급 라우팅과 발급 생성 정보.
 *
 * <p>{@code engineVersion} 이 {@code null} 이면 하위 호환으로 {@link EngineVersion#V1} 이다.
 *
 * @throws IllegalArgumentException {@code couponRoundId} 나 {@code validDays} 가 0 이하일 때,
 *     또는 엔진이 {@link EngineVersion#V3} 일 때 — Phase 0 발급 회차는 V1 또는 V2 다
 */
public record CouponRoundIssuanceDefinition(
        long couponRoundId,
        int validDays,
        EngineVersion engineVersion
) {

    public CouponRoundIssuanceDefinition {
        if (couponRoundId <= 0) {
            throw new IllegalArgumentException("couponRoundId는 0보다 커야 합니다.");
        }
        if (validDays <= 0) {
            throw new IllegalArgumentException("validDays는 0보다 커야 합니다.");
        }
        engineVersion = engineVersion == null ? EngineVersion.V1 : engineVersion;
        if (engineVersion == EngineVersion.V3) {
            throw new IllegalArgumentException("Phase 0 발급 회차는 V1 또는 V2여야 합니다.");
        }
    }
}
