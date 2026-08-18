package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.Issuance;

public interface IssuanceRepository {

    Issuance save(Issuance issuance);
}
