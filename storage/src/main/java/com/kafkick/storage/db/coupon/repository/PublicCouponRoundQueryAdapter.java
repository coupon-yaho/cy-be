package com.kafkick.storage.db.coupon.repository;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.PublicCouponRoundQueryPort;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;
import com.kafkick.core.membership.domain.MembershipGrade;

@Repository
public class PublicCouponRoundQueryAdapter
        implements PublicCouponRoundQueryPort {

    private final CouponRoundJpaRepository couponRoundJpaRepository;

    public PublicCouponRoundQueryAdapter(
            CouponRoundJpaRepository couponRoundJpaRepository
    ) {
        this.couponRoundJpaRepository = couponRoundJpaRepository;
    }

    @Override
    public PublicCouponRoundPage findPage(
            CouponRoundStatus status,
            int page,
            int size
    ) {
        try {
            Page<CouponRoundDetailProjection> result =
                    couponRoundJpaRepository.findPublicCouponRounds(
                            status == null ? null : status.name(),
                            PageRequest.of(page, size)
                    );

            return new PublicCouponRoundPage(
                    result.getContent().stream()
                            .map(PublicCouponRoundQueryAdapter::toDetail)
                            .toList(),
                    result.getNumber(),
                    result.getSize(),
                    result.getTotalElements(),
                    result.getTotalPages()
            );
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "공개 쿠폰 회차 목록 조회에 실패했습니다.",
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
