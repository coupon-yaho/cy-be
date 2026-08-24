package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import jakarta.persistence.EntityManager;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponAlreadyIssuedException;
import com.kafkick.core.coupon.exception.CouponIssueMemberNotFoundException;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.CouponExpirationCandidateQueryPort;
import com.kafkick.storage.db.coupon.entity.IssuanceEntity;
import com.kafkick.storage.db.coupon.mapper.IssuanceEntityMapper;
import com.kafkick.storage.db.support.SqlErrorInspector;

@Repository
public class IssuanceRepositoryImpl
        implements IssuanceRepository, CouponExpirationCandidateQueryPort {

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

    /**
     * 사전검증용 존재 조회를 실행하고 저장소 장애는 쿠폰 영속성 오류로 변환합니다.
     *
     * @param couponId 쿠폰 회차 식별자
     * @param memberId 회원 식별자
     * @return 기존 발급건이 있으면 {@code true}
     */
    @Override
    public boolean existsByCouponIdAndMemberId(
            Long couponId,
            Long memberId
    ) {
        try {
            return issuanceJpaRepository.existsByCouponIdAndMemberId(
                    couponId,
                    memberId
            );
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "기존 쿠폰 발급 여부 조회에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
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
            throw new CouponPersistenceException(
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
    public List<Issuance> findExpiredIssuedAfterId(
            Instant asOf,
            Long afterId,
            int limit
    ) {
        try {
            return issuanceJpaRepository.findExpiredIssuedAfterId(
                            IssuanceStatus.ISSUED,
                            asOf,
                            afterId,
                            PageRequest.of(0, limit)
                    ).stream()
                    .map(IssuanceEntityMapper::toDomain)
                    .toList();
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 만료 대상 조회에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public boolean updateStatusIfCurrent(
            Long issuanceId,
            Long memberId,
            IssuanceStatus currentStatus,
            IssuanceStatus nextStatus,
            Instant updatedAt
    ) {
        try {
            return issuanceJpaRepository.updateStatusIfCurrent(
                    issuanceId,
                    memberId,
                    currentStatus,
                    nextStatus,
                    updatedAt
            ) == 1;
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 상태 저장에 실패했습니다.",
                    exception
            );
        }
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
