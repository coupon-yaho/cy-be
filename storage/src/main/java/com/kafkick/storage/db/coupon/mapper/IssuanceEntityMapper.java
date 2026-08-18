// 발급 도메인과 issuances 엔티티의 변환을 담당합니다.
package com.kafkick.storage.db.coupon.mapper;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.storage.db.coupon.entity.IssuanceEntity;

public final class IssuanceEntityMapper {

    private IssuanceEntityMapper() {
    }

    public static IssuanceEntity toEntity(Issuance issuance) {
        return new IssuanceEntity(
                issuance.id(),
                issuance.couponRoundId(),
                issuance.memberId(),
                issuance.code(),
                issuance.issuedGrade(),
                issuance.status(),
                issuance.issuedAt(),
                issuance.expiresAt(),
                issuance.updatedAt()
        );
    }

    public static Issuance toDomain(IssuanceEntity entity) {
        return Issuance.restore(
                entity.getId(),
                entity.getCouponId(),
                entity.getMemberId(),
                entity.getCode(),
                entity.getIssuedGrade(),
                entity.getStatus(),
                entity.getIssuedAt(),
                entity.getExpiresAt(),
                entity.getUpdatedAt()
        );
    }
}
