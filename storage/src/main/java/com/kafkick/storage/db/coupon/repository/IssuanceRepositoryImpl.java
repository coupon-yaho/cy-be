// 발급건을 저장하고 1인 1매 DB 제약 위반을 비즈니스 오류로 변환합니다.
package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.Optional;

import jakarta.persistence.EntityManager;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponAlreadyIssuedException;
import com.kafkick.core.coupon.exception.CouponIssuePersistenceException;
import com.kafkick.core.coupon.exception.CouponIssueMemberNotFoundException;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.storage.db.coupon.entity.IssuanceEntity;
import com.kafkick.storage.db.coupon.mapper.IssuanceEntityMapper;
import com.kafkick.storage.db.support.SqlErrorInspector;

@Repository
public class IssuanceRepositoryImpl implements IssuanceRepository {

    private static final int MYSQL_DUPLICATE_KEY_ERROR = 1062;
    private static final int MYSQL_FOREIGN_KEY_ERROR = 1452;
    private static final String MEMBER_UNIQUE_KEY = "uk_coupon_member";
    private static final String MEMBER_FOREIGN_KEY = "member_id";

    private final IssuanceJpaRepository issuanceJpaRepository;
    private final EntityManager entityManager;

    public IssuanceRepositoryImpl(
            IssuanceJpaRepository issuanceJpaRepository,
            EntityManager entityManager
    ) {
        this.issuanceJpaRepository = issuanceJpaRepository;
        this.entityManager = entityManager;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public Issuance save(Issuance issuance) {
        try {
            IssuanceEntity saved = issuanceJpaRepository.saveAndFlush(
                    IssuanceEntityMapper.toEntity(issuance)
            );
            entityManager.refresh(saved);
            return IssuanceEntityMapper.toDomain(saved);
        } catch (DataIntegrityViolationException exception) {
            if (isMemberDuplicate(exception)) {
                throw new CouponAlreadyIssuedException(
                        "couponRoundId=" + issuance.couponRoundId()
                                + ", memberId=" + issuance.memberId(),
                        exception
                );
            }
            if (isMissingMember(exception)) {
                throw new CouponIssueMemberNotFoundException(
                        "memberId=" + issuance.memberId(),
                        exception
                );
            }
            throw new CouponIssuePersistenceException(
                    "쿠폰 발급건 저장에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public Optional<Issuance> findById(Long issuanceId) {
        return issuanceJpaRepository.findById(issuanceId)
                .map(IssuanceEntityMapper::toDomain);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean updateStatusIfCurrent(
            Long issuanceId,
            Long memberId,
            IssuanceStatus currentStatus,
            IssuanceStatus nextStatus,
            Instant updatedAt
    ) {
        return issuanceJpaRepository.updateStatusIfCurrent(
                issuanceId,
                memberId,
                currentStatus,
                nextStatus,
                updatedAt
        ) == 1;
    }

    private static boolean isMemberDuplicate(Throwable throwable) {
        return SqlErrorInspector.hasErrorCode(
                throwable,
                MYSQL_DUPLICATE_KEY_ERROR,
                MEMBER_UNIQUE_KEY
        );
    }

    private static boolean isMissingMember(Throwable throwable) {
        return SqlErrorInspector.hasErrorCode(
                throwable,
                MYSQL_FOREIGN_KEY_ERROR,
                MEMBER_FOREIGN_KEY
        );
    }
}
