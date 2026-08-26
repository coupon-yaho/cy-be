package com.kafkick.core.observation;

import java.time.Instant;
import java.util.Objects;

public record CampaignClosedEvent(
        long campaignCouponId,
        Instant closedAt
) {

    public CampaignClosedEvent {
        if (campaignCouponId <= 0) {
            throw new IllegalArgumentException(
                    "캠페인 회차 ID는 양수여야 합니다."
            );
        }
        Objects.requireNonNull(closedAt, "closedAt");
    }
}
