package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.IssuableCouponRoundQueryPort;
import com.kafkick.core.coupon.query.IssuableCouponRoundPage;
import com.kafkick.core.coupon.query.IssuableCouponRoundSummary;

@Repository
public class IssuableCouponRoundQueryAdapter
        implements IssuableCouponRoundQueryPort {

    private final CouponRoundJpaRepository couponRoundJpaRepository;

    public IssuableCouponRoundQueryAdapter(
            CouponRoundJpaRepository couponRoundJpaRepository
    ) {
        this.couponRoundJpaRepository = couponRoundJpaRepository;
    }

    @Override
    public IssuableCouponRoundPage findPage(
            Long memberId,
            int membershipGradeBit,
            Instant asOf,
            int page,
            int size
    ) {
        try {
            Page<IssuableCouponRoundProjection> result =
                    couponRoundJpaRepository.findIssuableCouponRounds(
                            memberId,
                            membershipGradeBit,
                            asOf,
                            PageRequest.of(page, size)
                    );

            return new IssuableCouponRoundPage(
                    result.getContent().stream()
                            .map(IssuableCouponRoundQueryAdapter::toSummary)
                            .toList(),
                    result.getNumber(),
                    result.getSize(),
                    result.getTotalElements(),
                    result.getTotalPages()
            );
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "발급 가능한 쿠폰 회차 목록 조회에 실패했습니다.",
                    exception
            );
        }
    }

    private static IssuableCouponRoundSummary toSummary(
            IssuableCouponRoundProjection projection
    ) {
        return new IssuableCouponRoundSummary(
                projection.getCouponRoundId(),
                projection.getBrandId(),
                projection.getName(),
                CouponPolicyType.valueOf(projection.getPolicyType()),
                projection.getDiscountRate(),
                projection.getMaxDiscountAmount(),
                projection.getDiscountAmount(),
                projection.getValidDays(),
                projection.getOpenAt(),
                projection.getCloseAt(),
                projection.getRemainingQuantity()
        );
    }
}
