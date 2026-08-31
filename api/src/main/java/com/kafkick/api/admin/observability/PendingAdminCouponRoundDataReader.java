package com.kafkick.api.admin.observability;

import java.time.Instant;
import java.util.List;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDetailData;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

/** 관측 DB 비활성 시 Overview에는 PENDING 모집단을, 상세에는 ADMIN-003을 제공하는 fallback Reader입니다. */
public final class PendingAdminCouponRoundDataReader implements AdminCouponRoundDataReader {

    @Override
    public AdminCouponRoundCatalog loadCatalog(Instant snapshotAt) {
        return new AdminCouponRoundCatalog(SourceStatus.PENDING, null, List.of());
    }

    @Override
    public AdminCouponRoundDetailData findDetail(
            long couponId,
            Instant fromInclusive,
            Instant toExclusive,
            Instant snapshotAt
    ) {
        throw disabled();
    }

    /** 관측 비활성을 DB 장애나 쿠폰 회차 미존재와 구분합니다. */
    private static BusinessException disabled() {
        return new BusinessException(AdminApiErrorCode.OBSERVATION_DISABLED);
    }
}
