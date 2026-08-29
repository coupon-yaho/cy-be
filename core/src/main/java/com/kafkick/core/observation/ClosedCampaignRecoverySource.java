package com.kafkick.core.observation;

import java.time.Instant;
import java.util.List;

public interface ClosedCampaignRecoverySource {

    List<ClosedCampaign> findRecentlyClosed(
            Instant fromInclusive,
            Instant toInclusive,
            int limit
    );
}
