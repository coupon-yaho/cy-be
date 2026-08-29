package com.kafkick.api.observation.issuance;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.CampaignLifecycleRecorder;

/** Connects domain campaign-close notifications to the bounded meter registry. */
public final class MeterCampaignLifecycleRecorder implements CampaignLifecycleRecorder {

    private final CampaignMeterRegistry campaignMeters;

    public MeterCampaignLifecycleRecorder(CampaignMeterRegistry campaignMeters) {
        this.campaignMeters = Objects.requireNonNull(campaignMeters, "campaignMeters");
    }

    @Override
    public void retireCampaign(long campaignCouponId, Instant closedAt) {
        campaignMeters.retireCampaign(campaignCouponId, closedAt);
    }
}
