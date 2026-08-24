package com.kafkick.storage.db.coupon.repository;

import java.util.Optional;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.CouponRoundDetailQueryPort;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.membership.domain.MembershipGrade;

@Repository
public class CouponRoundDetailQueryAdapter
        implements CouponRoundDetailQueryPort {

    private final CouponRoundJpaRepository couponRoundJpaRepository;

    public CouponRoundDetailQueryAdapter(
            CouponRoundJpaRepository couponRoundJpaRepository
    ) {
        this.couponRoundJpaRepository = couponRoundJpaRepository;
    }

    @Override
    public Optional<CouponRoundDetail> findById(Long couponRoundId) {
        try {
            return couponRoundJpaRepository
                    .findCouponRoundDetailById(couponRoundId)
                    .map(CouponRoundDetailQueryAdapter::toDetail);
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 회차 상세 조회에 실패했습니다.",
                    exception
            );
        }
    }

    private static CouponRoundDetail toDetail(
            CouponRoundDetailProjection projection
    ) {
        return new CouponRoundDetail(
                projection.getCouponRoundId(),
                projection.getTemplateId(),
                projection.getBrandId(),
                projection.getName(),
                CouponPolicyType.valueOf(projection.getPolicyType()),
                projection.getDiscountRate(),
                projection.getMaxDiscountAmount(),
                projection.getDiscountAmount(),
                projection.getValidDays(),
                MembershipGrade.fromMask(projection.getEligibleGradesMask()),
                projection.getOpenAt(),
                projection.getCloseAt(),
                CouponRoundStatus.valueOf(projection.getStatus()),
                projection.getTotalQuantity(),
                projection.getRemainingQuantity()
        );
    }
}
