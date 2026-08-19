// ISSUE 이력을 발급 트랜잭션 안에서 함께 저장합니다.
package com.kafkick.storage.db.coupon.repository;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.exception.CouponCancelUsePersistenceException;
import com.kafkick.core.coupon.exception.CouponIssuePersistenceException;
import com.kafkick.core.coupon.exception.CouponUsePersistenceException;
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
        IssuanceHistoryEntity entity = new IssuanceHistoryEntity(
                history.id(),
                history.issuanceId(),
                history.eventType(),
                history.fromStatus(),
                history.toStatus(),
                history.reason(),
                history.requestId(),
                history.createdAt()
        );
        try {
            historyJpaRepository.saveAndFlush(entity);
        } catch (DataAccessException exception) {
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
}
