package com.kafkick.core.coupon.v2.port;

import java.util.Optional;

import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.observation.EngineVersion;

public interface CouponRoundIssuanceDefinitionRepository {

    Optional<CouponRoundIssuanceDefinition> lockAndFindById(long couponRoundId);

    /** 오픈 전 또는 마감 후 회차만 변경한다. 오픈 중이면 {@code false}. */
    boolean updateEngineVersionWhenNotOpen(
            long couponRoundId,
            EngineVersion engineVersion
    );
}
