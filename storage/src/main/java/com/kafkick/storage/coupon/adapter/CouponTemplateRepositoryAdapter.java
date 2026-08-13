// CouponTemplateRepository 계약을 Spring Data JPA로 구현합니다.
package com.kafkick.storage.coupon.adapter;

import com.kafkick.core.coupon.CouponTemplate;
import com.kafkick.core.coupon.CouponTemplateRepository;
import com.kafkick.storage.coupon.entity.CouponTemplateEntity;
import com.kafkick.storage.coupon.repository.CouponTemplateJpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public class CouponTemplateRepositoryAdapter implements CouponTemplateRepository {

    private final CouponTemplateJpaRepository jpaRepository;

    public CouponTemplateRepositoryAdapter(
            CouponTemplateJpaRepository jpaRepository
    ) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public CouponTemplate save(CouponTemplate couponTemplate) {
        CouponTemplateEntity entity = CouponTemplateEntity.from(couponTemplate);
        CouponTemplateEntity savedEntity = jpaRepository.save(entity);

        return savedEntity.toDomain();
    }
}
