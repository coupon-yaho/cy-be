package com.kafkick.storage.db.coupontemplate.repository;

import java.util.List;
import java.util.Set;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.storage.db.coupontemplate.entity.CouponTemplateEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CouponTemplateJpaRepository
        extends JpaRepository<CouponTemplateEntity, Long> {

    List<CouponTemplateEntity>
            findAllByActiveTrueAndPolicyTypeInOrderByIdAsc(
                    Set<CouponPolicyType> policyTypes
            );
}
