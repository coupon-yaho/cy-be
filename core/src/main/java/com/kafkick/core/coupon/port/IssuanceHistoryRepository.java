package com.kafkick.core.coupon.port;

import java.util.List;

import com.kafkick.core.coupon.domain.IssuanceHistory;

public interface IssuanceHistoryRepository {

    void save(IssuanceHistory issuanceHistory);

    void saveAll(List<IssuanceHistory> issuanceHistories);
}
