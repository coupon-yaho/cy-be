// 쿠폰 회차와 최초 재고를 별도 새 트랜잭션에서 함께 저장합니다.
package com.kafkick.storage.db.coupon.repository;

import java.sql.SQLException;

import jakarta.persistence.EntityManager;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.exception.CouponRoundPersistenceException;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.storage.db.coupon.entity.CouponRoundEntity;
import com.kafkick.storage.db.coupon.entity.CouponStockEntity;
import com.kafkick.storage.db.coupon.mapper.CouponRoundEntityMapper;

@Repository
public class CouponRoundRepositoryImpl implements CouponRoundRepository {

    private static final int MYSQL_DUPLICATE_KEY_ERROR = 1062;

    private final CouponRoundJpaRepository couponRoundJpaRepository;
    private final CouponStockJpaRepository couponStockJpaRepository;
    private final EntityManager entityManager;

    public CouponRoundRepositoryImpl(
            CouponRoundJpaRepository couponRoundJpaRepository,
            CouponStockJpaRepository couponStockJpaRepository,
            EntityManager entityManager
    ) {
        this.couponRoundJpaRepository = couponRoundJpaRepository;
        this.couponStockJpaRepository = couponStockJpaRepository;
        this.entityManager = entityManager;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CouponRound saveWithInitialStock(
            CouponRound couponRound,
            CouponStock initialStock
    ) {
        try {
            CouponRoundEntity savedRound = couponRoundJpaRepository
                    .saveAndFlush(
                            CouponRoundEntityMapper.toEntity(couponRound)
                    );
            CouponStockEntity stockEntity = new CouponStockEntity(
                    savedRound.getId(),
                    initialStock.totalQuantity(),
                    initialStock.activeCount(),
                    initialStock.updatedAt()
            );
            couponStockJpaRepository.saveAndFlush(stockEntity);
            entityManager.refresh(savedRound);

            return CouponRoundEntityMapper.toDomain(savedRound);
        } catch (DataIntegrityViolationException exception) {
            if (isDuplicateKey(exception)) {
                throw new CouponRoundAlreadyExistsException(
                        "동일 템플릿과 오픈 시각의 쿠폰 회차가 이미 존재합니다.",
                        exception
                );
            }
            throw new CouponRoundPersistenceException(
                    "쿠폰 회차와 최초 재고 저장에 실패했습니다.",
                    exception
            );
        }
    }

    private static boolean isDuplicateKey(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            if (cause instanceof SQLException sqlException
                    && sqlException.getErrorCode()
                    == MYSQL_DUPLICATE_KEY_ERROR) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
