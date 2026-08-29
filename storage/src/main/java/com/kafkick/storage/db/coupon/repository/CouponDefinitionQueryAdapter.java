package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.v2.query.CouponDefinition;
import com.kafkick.core.coupon.v2.query.CouponDefinitionQueryPort;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;

/** 재고·회원 발급 이력을 절대 조인하지 않는 V2 정의 어댑터다. */
@Repository
public class CouponDefinitionQueryAdapter implements CouponDefinitionQueryPort {

    private final CouponRoundJpaRepository repository;

    public CouponDefinitionQueryAdapter(CouponRoundJpaRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<CouponDefinition> findCandidates(Instant asOf) {
        try {
            return repository.findV2CouponDefinitions(asOf).stream()
                    .map(CouponDefinitionQueryAdapter::toDefinition)
                    .toList();
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException("V2 쿠폰 정의 목록 조회에 실패했습니다.", exception);
        }
    }

    private static CouponDefinition toDefinition(CouponDefinitionProjection projection) {
        return new CouponDefinition(
                projection.getCouponRoundId(), projection.getBrandId(), projection.getName(),
                CouponPolicyType.valueOf(projection.getPolicyType()), projection.getDiscountRate(),
                projection.getMaxDiscountAmount(), projection.getDiscountAmount(), projection.getValidDays(),
                projection.getOpenAt(), projection.getCloseAt(),
                CouponRoundStatus.valueOf(projection.getStatus()));
    }
}
