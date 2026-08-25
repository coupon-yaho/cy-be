package com.kafkick.api.admin.observability;

import java.time.Instant;
import java.util.List;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

/** 관측 DB 비활성 시 Overview에는 PENDING 모집단을, 상세에는 ADMIN-003을 제공하는 fallback Reader입니다. */
public final class PendingAdminCampaignDataReader implements AdminCampaignDataReader {

    @Override
    public AdminCampaignCatalog loadCatalog(Instant snapshotAt) {
        return new AdminCampaignCatalog(SourceStatus.PENDING, null, List.of());
    }

    @Override
    public AdminCampaignDetailData findDetail(
            long couponId,
            Instant fromInclusive,
            Instant toExclusive,
            Instant snapshotAt
    ) {
        throw disabled();
    }

    /** 관측 비활성을 DB 장애나 캠페인 미존재와 구분합니다. */
    private static BusinessException disabled() {
        return new BusinessException(AdminApiErrorCode.OBSERVATION_DISABLED);
    }
}
