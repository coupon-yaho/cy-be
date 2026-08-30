package com.kafkick.storage.db.coupon.repository;

import java.util.Optional;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.MemberCouponQueryPort;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.core.coupon.query.MemberCouponSummary;

@Repository
public class MemberCouponQueryAdapter implements MemberCouponQueryPort {

    private final IssuanceJpaRepository issuanceJpaRepository;

    public MemberCouponQueryAdapter(
            IssuanceJpaRepository issuanceJpaRepository
    ) {
        this.issuanceJpaRepository = issuanceJpaRepository;
    }

    @Override
    public MemberCouponPage findPageByMemberId(
            Long memberId,
            IssuanceStatus status,
            int page,
            int size
    ) {
        try {
            Page<MemberCouponProjection> result = issuanceJpaRepository
                    .findMemberCoupons(
                            memberId,
                            status,
                            PageRequest.of(page, size)
                    );

            return new MemberCouponPage(
                    result.getContent().stream()
                            .map(MemberCouponQueryAdapter::toSummary)
                            .toList(),
                    result.getNumber(),
                    result.getSize(),
                    result.getTotalElements(),
                    result.getTotalPages()
            );
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "사용자 보유 쿠폰 목록 조회에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public Optional<MemberCouponSummary> findByMemberIdAndIssuanceId(
            Long memberId,
            Long issuanceId
    ) {
        try {
            return issuanceJpaRepository.findMemberCoupon(
                            memberId,
                            issuanceId
                    )
                    .map(MemberCouponQueryAdapter::toSummary);
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "사용자 보유 쿠폰 단건 조회에 실패했습니다.",
                    exception
            );
        }
    }

    private static MemberCouponSummary toSummary(
            MemberCouponProjection projection
    ) {
        return new MemberCouponSummary(
                projection.getIssuanceId(),
                projection.getCouponRoundId(),
                projection.getCode(),
                projection.getStatus(),
                projection.getName(),
                projection.getPolicyType(),
                projection.getDiscountRate(),
                projection.getMaxDiscountAmount(),
                projection.getDiscountAmount(),
                projection.getIssuedAt(),
                projection.getExpiresAt(),
                projection.getUsedAt(),
                projection.getUsedDiscountAmount(),
                projection.getOrderId()
        );
    }
}
