package com.kafkick.core.observation;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CampaignClosedEventTest {

    private static final Instant CLOSED_AT =
            Instant.parse("2026-08-26T05:04:00Z");

    @Test
    @DisplayName("양수 캠페인 회차 ID와 종료 시각을 보존한다")
    void preserveValidClosedCampaignValues() {
        CampaignClosedEvent event = new CampaignClosedEvent(201L, CLOSED_AT);

        assertThat(event.campaignCouponId()).isEqualTo(201L);
        assertThat(event.closedAt()).isEqualTo(CLOSED_AT);
    }

    @Test
    @DisplayName("양수가 아닌 캠페인 회차 ID를 거부한다")
    void rejectNonPositiveCampaignCouponId() {
        assertThatThrownBy(() -> new CampaignClosedEvent(0L, CLOSED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("종료 시각이 없으면 거부한다")
    void rejectMissingClosedAt() {
        assertThatThrownBy(() -> new CampaignClosedEvent(201L, null))
                .isInstanceOf(NullPointerException.class);
    }
}
