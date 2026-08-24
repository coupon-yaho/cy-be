package com.kafkick.storage.db.coupon.mapper;

import com.kafkick.core.coupon.domain.IssuanceUsage;
import com.kafkick.storage.db.coupon.entity.IssuanceUsageEntity;

public final class IssuanceUsageEntityMapper {

    private IssuanceUsageEntityMapper() {
    }

    public static IssuanceUsageEntity toEntity(IssuanceUsage usage) {
        return new IssuanceUsageEntity(
                usage.id(),
                usage.issuanceId(),
                usage.orderId(),
                usage.discountAmount(),
                usage.usedAt(),
                usage.canceledAt()
        );
    }

    public static IssuanceUsage toDomain(IssuanceUsageEntity entity) {
        return IssuanceUsage.restore(
                entity.getId(),
                entity.getIssuanceId(),
                entity.getOrderId(),
                entity.getDiscountAmount(),
                entity.getUsedAt(),
                entity.getCanceledAt()
        );
    }
}
