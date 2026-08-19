// 회원 소유권과 상태 조건으로 발급건·회차 스냅샷을 한 번에 페이지 조회합니다.
package com.kafkick.storage.db.coupon.repository;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponQueryPersistenceException;
import com.kafkick.core.coupon.port.MemberCouponPage;
import com.kafkick.core.coupon.port.MemberCouponQueryRepository;
import com.kafkick.core.coupon.port.MemberCouponSummary;

@Repository
public class MemberCouponQueryRepositoryImpl
        implements MemberCouponQueryRepository {

    private final IssuanceJpaRepository issuanceJpaRepository;

    public MemberCouponQueryRepositoryImpl(
            IssuanceJpaRepository issuanceJpaRepository
    ) {
        this.issuanceJpaRepository = issuanceJpaRepository;
    }

    @Override
    @Transactional(readOnly = true)
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
                            .map(MemberCouponQueryRepositoryImpl::toSummary)
                            .toList(),
                    result.getNumber(),
                    result.getSize(),
                    result.getTotalElements(),
                    result.getTotalPages()
            );
        } catch (DataAccessException exception) {
            throw new CouponQueryPersistenceException(
                    "사용자 보유 쿠폰 목록 조회에 실패했습니다.",
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
                projection.getExpiresAt()
        );
    }
}
