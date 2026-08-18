// 쿠폰 사용 실적을 상태 변경 트랜잭션 안에서 저장합니다.
package com.kafkick.storage.db.coupon.repository;

import jakarta.persistence.EntityManager;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IssuanceUsage;
import com.kafkick.core.coupon.exception.CouponUsePersistenceException;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.storage.db.coupon.entity.IssuanceUsageEntity;
import com.kafkick.storage.db.coupon.mapper.IssuanceUsageEntityMapper;

@Repository
public class IssuanceUsageRepositoryImpl
        implements IssuanceUsageRepository {

    private final IssuanceUsageJpaRepository usageJpaRepository;
    private final EntityManager entityManager;

    public IssuanceUsageRepositoryImpl(
            IssuanceUsageJpaRepository usageJpaRepository,
            EntityManager entityManager
    ) {
        this.usageJpaRepository = usageJpaRepository;
        this.entityManager = entityManager;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public IssuanceUsage save(IssuanceUsage usage) {
        try {
            IssuanceUsageEntity saved = usageJpaRepository.saveAndFlush(
                    IssuanceUsageEntityMapper.toEntity(usage)
            );
            entityManager.refresh(saved);
            return IssuanceUsageEntityMapper.toDomain(saved);
        } catch (DataIntegrityViolationException exception) {
            throw new CouponUsePersistenceException(
                    "쿠폰 사용 실적 저장에 실패했습니다.",
                    exception
            );
        }
    }
}
