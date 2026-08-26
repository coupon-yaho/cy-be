package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.coupon.entity.IssuanceEntity;

public interface IssuanceJpaRepository
        extends JpaRepository<IssuanceEntity, Long> {

    /** 회차와 회원이 같은 기존 발급건의 존재 여부를 조회합니다. */
    boolean existsByCouponIdAndMemberId(Long couponId, Long memberId);

    @Query(
            value = """
                    SELECT issuance.id AS issuanceId,
                           issuance.couponId AS couponRoundId,
                           issuance.code AS code,
                           issuance.status AS status,
                           couponRound.name AS name,
                           couponRound.policyType AS policyType,
                           couponRound.discountRate AS discountRate,
                           couponRound.maxDiscountAmount AS maxDiscountAmount,
                           couponRound.discountAmount AS discountAmount,
                           issuance.issuedAt AS issuedAt,
                           issuance.expiresAt AS expiresAt,
                           activeUsage.usedAt AS usedAt,
                           activeUsage.discountAmount AS usedDiscountAmount,
                           activeUsage.orderId AS orderId
                    FROM IssuanceEntity issuance
                    JOIN CouponRoundEntity couponRound
                      ON couponRound.id = issuance.couponId
                    LEFT JOIN IssuanceUsageEntity activeUsage
                      ON activeUsage.issuanceId = issuance.id
                     AND activeUsage.canceledAt IS NULL
                    WHERE issuance.memberId = :memberId
                      AND (:status IS NULL OR issuance.status = :status)
                    ORDER BY issuance.issuedAt DESC, issuance.id DESC
                    """,
            countQuery = """
                    SELECT COUNT(issuance.id)
                    FROM IssuanceEntity issuance
                    WHERE issuance.memberId = :memberId
                      AND (:status IS NULL OR issuance.status = :status)
                    """
    )
    Page<MemberCouponProjection> findMemberCoupons(
            @Param("memberId") Long memberId,
            @Param("status") IssuanceStatus status,
            Pageable pageable
    );

    @Query("""
            SELECT issuance
            FROM IssuanceEntity issuance
            WHERE issuance.status = :status
              AND issuance.expiresAt < :asOf
              AND issuance.id > :afterId
            ORDER BY issuance.id ASC
            """)
    List<IssuanceEntity> findExpiredIssuedAfterId(
            @Param("status") IssuanceStatus status,
            @Param("asOf") Instant asOf,
            @Param("afterId") Long afterId,
            Pageable pageable
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE IssuanceEntity issuance
            SET issuance.status = :nextStatus,
                issuance.updatedAt = :updatedAt
            WHERE issuance.id = :issuanceId
              AND issuance.memberId = :memberId
              AND issuance.status = :currentStatus
            """)
    int updateStatusIfCurrent(
            @Param("issuanceId") Long issuanceId,
            @Param("memberId") Long memberId,
            @Param("currentStatus") IssuanceStatus currentStatus,
            @Param("nextStatus") IssuanceStatus nextStatus,
            @Param("updatedAt") Instant updatedAt
    );
}
