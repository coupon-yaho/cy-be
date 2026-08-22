package com.kafkick.storage.db.coupon.repository;

import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.storage.db.coupon.entity.IssuanceHistoryEntity;

@Repository
public class IssuanceHistoryRepositoryImpl
        implements IssuanceHistoryRepository {

    private final IssuanceHistoryJpaRepository historyJpaRepository;

    public IssuanceHistoryRepositoryImpl(
            IssuanceHistoryJpaRepository historyJpaRepository
    ) {
        this.historyJpaRepository = historyJpaRepository;
    }

    @Override
    public void save(IssuanceHistory history) {
        try {
            historyJpaRepository.saveAndFlush(toEntity(history));
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 이력 저장에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public void saveAllExpirations(List<IssuanceHistory> histories) {
        List<IssuanceHistoryEntity> entities = histories.stream()
                .map(IssuanceHistoryRepositoryImpl::toEntity)
                .toList();
        try {
            historyJpaRepository.saveAllAndFlush(entities);
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 이력 일괄 저장에 실패했습니다.",
                    exception
            );
        }
    }

    private static IssuanceHistoryEntity toEntity(
            IssuanceHistory history
    ) {
        return new IssuanceHistoryEntity(
                history.id(),
                history.issuanceId(),
                history.eventType(),
                history.fromStatus(),
                history.toStatus(),
                history.reason(),
                history.requestId(),
                history.createdAt()
        );
    }
}
