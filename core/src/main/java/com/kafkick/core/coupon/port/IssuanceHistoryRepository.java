package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.IssuanceHistory;

public interface IssuanceHistoryRepository {

    void save(IssuanceHistory issuanceHistory);
}
