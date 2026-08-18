package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.IssuanceUsage;

public interface IssuanceUsageRepository {

    IssuanceUsage save(IssuanceUsage issuanceUsage);
}
