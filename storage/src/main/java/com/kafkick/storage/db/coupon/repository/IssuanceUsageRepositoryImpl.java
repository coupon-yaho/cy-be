// 쿠폰 사용 실적을 상태 변경 트랜잭션 안에서 저장합니다.
package com.kafkick.storage.db.coupon.repository;

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
    public IssuanceUsageRepositoryImpl(
            IssuanceUsageJpaRepository usageJpaRepository
    ) {
        this.usageJpaRepository = usageJpaRepository;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public IssuanceUsage save(IssuanceUsage usage) {
        try {
            IssuanceUsageEntity saved = usageJpaRepository.saveAndFlush(
                    IssuanceUsageEntityMapper.toEntity(usage)
            );
            return IssuanceUsageEntityMapper.toDomain(saved);
        } catch (DataIntegrityViolationException exception) {
            throw new CouponUsePersistenceException(
                    "쿠폰 사용 실적 저장에 실패했습니다.",
                    exception
            );
        }
    }
}
