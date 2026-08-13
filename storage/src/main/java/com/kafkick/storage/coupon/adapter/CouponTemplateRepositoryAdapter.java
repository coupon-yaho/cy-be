// CouponTemplateRepository 계약을 Spring Data JPA로 구현합니다.
package com.kafkick.storage.coupon.adapter;

import com.kafkick.core.coupon.CouponTemplate;
import com.kafkick.core.coupon.CouponTemplateRepository;
import com.kafkick.core.coupon.exception.CouponErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.coupon.entity.CouponTemplateEntity;
import com.kafkick.storage.coupon.repository.CouponTemplateJpaRepository;
import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Repository;
import org.springframework.dao.DataIntegrityViolationException;

@Repository
public class CouponTemplateRepositoryAdapter implements CouponTemplateRepository {

    private final CouponTemplateJpaRepository jpaRepository;
    private final EntityManager entityManager;

    public CouponTemplateRepositoryAdapter(
            CouponTemplateJpaRepository jpaRepository,
            EntityManager entityManager
    ) {
        this.jpaRepository = jpaRepository;
        this.entityManager = entityManager;
    }

    @Override
    public CouponTemplate save(CouponTemplate couponTemplate) {
        try {
            CouponTemplateEntity entity =
                    CouponTemplateEntity.from(couponTemplate);

            CouponTemplateEntity savedEntity =
                    jpaRepository.saveAndFlush(entity);

            entityManager.refresh(savedEntity);

            return savedEntity.toDomain();
        } catch (DataIntegrityViolationException exception) {
            throw new BusinessException(
                    CouponErrorCode.INVALID_COUPON_TEMPLATE,
                    "쿠폰 템플릿 저장 중 DB 제약 위반: brandId="
                            + couponTemplate.brandId(),
                    exception
            );
        }
    }
}
