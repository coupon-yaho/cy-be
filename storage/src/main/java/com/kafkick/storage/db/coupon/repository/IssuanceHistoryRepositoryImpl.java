// 발급건 상태 전이 이력을 해당 트랜잭션 안에서 함께 저장합니다.
package com.kafkick.storage.db.coupon.repository;

import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.exception.CouponCancelUsePersistenceException;
import com.kafkick.core.coupon.exception.CouponCancelPersistenceException;
import com.kafkick.core.coupon.exception.CouponIssuePersistenceException;
import com.kafkick.core.coupon.exception.CouponUsePersistenceException;
import com.kafkick.core.coupon.exception.CouponExpirationPersistenceException;
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
    @Transactional(propagation = Propagation.MANDATORY)
    public void save(IssuanceHistory history) {
        try {
            historyJpaRepository.saveAndFlush(toEntity(history));
        } catch (DataAccessException exception) {
            if (history.eventType() == IssuanceEventType.CANCEL) {
                throw new CouponCancelPersistenceException(
                        "쿠폰 발급 취소 이력 저장에 실패했습니다.",
                        exception
                );
            }
            if (history.eventType() == IssuanceEventType.CANCEL_USE) {
                throw new CouponCancelUsePersistenceException(
                        "쿠폰 사용 취소 이력 저장에 실패했습니다.",
                        exception
                );
            }
            if (history.eventType() == IssuanceEventType.USE) {
                throw new CouponUsePersistenceException(
                        "쿠폰 사용 이력 저장에 실패했습니다.",
                        exception
                );
            }
            throw new CouponIssuePersistenceException(
                    "쿠폰 발급 이력 저장에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void saveAllExpirations(List<IssuanceHistory> histories) {
        List<IssuanceHistoryEntity> entities = histories.stream()
                .map(IssuanceHistoryRepositoryImpl::toEntity)
                .toList();
        try {
            historyJpaRepository.saveAllAndFlush(entities);
        } catch (DataAccessException exception) {
            throw new CouponExpirationPersistenceException(
                    "쿠폰 만료 이력 일괄 저장에 실패했습니다.",
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
