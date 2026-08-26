package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.BrandDayCalendarQueryPort;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;

@Repository
public class BrandDayCalendarQueryAdapter
        implements BrandDayCalendarQueryPort {

    private final CouponRoundJpaRepository couponRoundJpaRepository;

    public BrandDayCalendarQueryAdapter(
            CouponRoundJpaRepository couponRoundJpaRepository
    ) {
        this.couponRoundJpaRepository = couponRoundJpaRepository;
    }

    @Override
    public List<CouponRoundDetail> findBetween(
            Instant fromInclusive,
            Instant toExclusive
    ) {
        try {
            return couponRoundJpaRepository.findCalendarRounds(
                            fromInclusive,
                            toExclusive
                    ).stream()
                    .map(BrandDayCalendarQueryAdapter::toDetail)
                    .toList();
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "브랜드 데이 달력 조회에 실패했습니다.",
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
