package com.kafkick.storage.db.coupon.repository;

import java.util.Optional;
import java.time.Instant;
import java.util.EnumSet;

import jakarta.persistence.EntityManager;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.storage.db.coupon.entity.CouponRoundEntity;
import com.kafkick.storage.db.coupon.entity.CouponStockEntity;
import com.kafkick.storage.db.coupon.mapper.CouponRoundEntityMapper;
import com.kafkick.storage.db.support.SqlErrorInspector;

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
    @Transactional(propagation = Propagation.MANDATORY)
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
            throw new CouponPersistenceException(
                    "쿠폰 회차와 최초 재고 저장에 실패했습니다.",
                    exception
            );
        }
    }

    private static boolean isDuplicateKey(Throwable throwable) {
        return SqlErrorInspector.hasErrorCode(
                throwable,
                MYSQL_DUPLICATE_KEY_ERROR
        );
    }

    @Override
    public Optional<CouponRound> findById(Long couponRoundId) {
        return couponRoundJpaRepository.findById(couponRoundId)
                .map(CouponRoundEntityMapper::toDomain);
    }

    @Override
    public boolean existsByTemplateIdAndOpenAt(
            Long templateId,
            Instant openAt
    ) {
        try {
            return couponRoundJpaRepository.existsByTemplateIdAndOpenAt(
                    templateId,
                    openAt
            );
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "동일 쿠폰 회차 조회에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public boolean existsOverlappingSchedule(
            Instant openAt,
            Instant closeAt
    ) {
        try {
            return couponRoundJpaRepository.countOverlappingSchedule(
                    openAt,
                    closeAt,
                    EnumSet.of(
                            CouponRoundStatus.SCHEDULED,
                            CouponRoundStatus.OPEN
                    )
            ) > 0;
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 회차 예약 시간 충돌 조회에 실패했습니다.",
                    exception
            );
        }
    }
}
