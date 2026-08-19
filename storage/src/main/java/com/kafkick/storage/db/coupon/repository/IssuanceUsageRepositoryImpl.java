// 쿠폰 사용 실적을 상태 변경 트랜잭션 안에서 저장합니다.
package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.Optional;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IssuanceUsage;
import com.kafkick.core.coupon.exception.CouponCancelUsePersistenceException;
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

    @Override
    public Optional<IssuanceUsage> findActiveByIssuanceId(Long issuanceId) {
        try {
            return usageJpaRepository
                    .findByIssuanceIdAndCanceledAtIsNull(issuanceId)
                    .map(IssuanceUsageEntityMapper::toDomain);
        } catch (DataAccessException exception) {
            throw new CouponCancelUsePersistenceException(
                    "활성 쿠폰 사용 실적 조회에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean cancelIfActive(Long usageId, Instant canceledAt) {
        try {
            return usageJpaRepository.cancelIfActive(
                    usageId,
                    canceledAt
            ) == 1;
        } catch (DataAccessException exception) {
            throw new CouponCancelUsePersistenceException(
                    "쿠폰 사용 실적 취소에 실패했습니다.",
                    exception
            );
        }
    }
}
