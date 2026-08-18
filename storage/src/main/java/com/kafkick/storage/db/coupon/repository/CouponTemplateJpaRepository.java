// 쿠폰 템플릿의 저장과 조회를 담당합니다.
package com.kafkick.storage.db.coupon.repository;

import java.util.List;
import java.util.Set;

import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.storage.db.coupon.entity.CouponTemplateEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CouponTemplateJpaRepository
        extends JpaRepository<CouponTemplateEntity, Long> {

    List<CouponTemplateEntity>
            findAllByActiveTrueAndPolicyTypeInOrderByIdAsc(
                    Set<CouponPolicyType> policyTypes
            );
}
