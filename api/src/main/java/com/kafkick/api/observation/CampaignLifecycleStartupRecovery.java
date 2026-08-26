package com.kafkick.api.observation;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import com.kafkick.core.observation.CampaignLifecycleRecorder;
import com.kafkick.core.observation.ClosedCampaign;
import com.kafkick.core.observation.ClosedCampaignRecoverySource;
import com.kafkick.core.support.TimeProvider;

public final class CampaignLifecycleStartupRecovery
        implements ApplicationRunner {

    static final Duration LOOKBACK = Duration.ofDays(1);
    static final int LIMIT = 1_000;

    private static final Logger log = LoggerFactory.getLogger(
            CampaignLifecycleStartupRecovery.class
    );

    private static final Comparator<ClosedCampaign> OLDEST_FIRST =
            Comparator.comparing(ClosedCampaign::closedAt)
                    .thenComparingLong(ClosedCampaign::campaignCouponId);

    private final ClosedCampaignRecoverySource source;
    private final CampaignLifecycleRecorder recorder;
    private final TimeProvider timeProvider;

    public CampaignLifecycleStartupRecovery(
            ClosedCampaignRecoverySource source,
            CampaignLifecycleRecorder recorder,
            TimeProvider timeProvider
    ) {
        this.source = Objects.requireNonNull(source);
        this.recorder = Objects.requireNonNull(recorder);
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    @Override
    public void run(ApplicationArguments arguments) {
        Instant now = timeProvider.instant();
        try {
            List<ClosedCampaign> campaigns = source.findRecentlyClosed(
                    now.minus(LOOKBACK),
                    now,
                    LIMIT
            );
            campaigns.stream()
                    .sorted(OLDEST_FIRST)
                    .forEach(campaign -> recorder.retireCampaign(
                            campaign.campaignCouponId(),
                            campaign.closedAt()
                    ));
        } catch (Exception exception) {
            log.warn(
                    "캠페인 종료 기동 보정에 실패했습니다. API 기동은 계속합니다. cause={}",
                    exception.toString()
            );
        }
    }
}
