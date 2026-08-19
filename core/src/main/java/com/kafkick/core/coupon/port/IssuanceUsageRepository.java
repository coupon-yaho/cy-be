package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.coupon.domain.IssuanceUsage;

public interface IssuanceUsageRepository {

    IssuanceUsage save(IssuanceUsage issuanceUsage);

    Optional<IssuanceUsage> findActiveByIssuanceId(Long issuanceId);

    boolean cancelIfActive(Long usageId, Instant canceledAt);
}
