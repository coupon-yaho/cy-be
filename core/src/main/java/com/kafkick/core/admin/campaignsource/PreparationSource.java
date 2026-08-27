package com.kafkick.core.admin.campaignsource;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.observation.SourceStatus;

/** DB에서 독립적으로 판정한 캠페인 설정·재고와 정책 종류 준비 원천입니다. */
public record PreparationSource(
        Boolean campaignConfigurationReady,
        Boolean databaseStockReady,
        CouponPolicyType policyType,
        SourceStatus status,
        Instant observedAt
) {

    /** 값 보유 상태와 두 DB 준비 판정·관측 시각의 조합을 검증합니다. */
    public PreparationSource {
        Objects.requireNonNull(status, "status");
        if (status.carriesValue()) {
            if (campaignConfigurationReady == null || databaseStockReady == null || observedAt == null
                    || (campaignConfigurationReady && policyType == null)) {
                throw new IllegalArgumentException(
                        status + " 준비 원천에는 두 판정과 유효 설정의 policyType, observedAt이 필요합니다.");
            }
        } else if (campaignConfigurationReady != null || databaseStockReady != null
                || policyType != null || observedAt != null) {
            throw new IllegalArgumentException(
                    status + " 준비 원천의 판정과 policyType, observedAt은 null이어야 합니다.");
        }
    }
}
