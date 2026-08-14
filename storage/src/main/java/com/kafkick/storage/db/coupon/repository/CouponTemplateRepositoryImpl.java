// CouponTemplateRepository 포트를 Spring Data JPA로 구현합니다.
package com.kafkick.storage.db.coupon.repository;

import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.coupon.entity.CouponTemplateEntity;
import com.kafkick.storage.db.coupon.mapper.CouponTemplateEntityMapper;
import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Repository;
import org.springframework.dao.DataIntegrityViolationException;

@Repository
public class CouponTemplateRepositoryImpl implements CouponTemplateRepository {

    private final CouponTemplateJpaRepository jpaRepository;
    private final EntityManager entityManager;

    public CouponTemplateRepositoryImpl(
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
                    CouponTemplateEntityMapper.toEntity(couponTemplate);

            CouponTemplateEntity savedEntity =
                    jpaRepository.saveAndFlush(entity);

            entityManager.refresh(savedEntity);

            return CouponTemplateEntityMapper.toDomain(savedEntity);
        } catch (DataIntegrityViolationException exception) {
            throw new BusinessException(
                    CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE,
                    "쿠폰 템플릿 저장 중 DB 제약 위반: brandId="
                            + couponTemplate.brandId(),
                    exception
            );
        }
    }
}
