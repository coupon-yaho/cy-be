package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.storage.db.coupon.entity.CouponRoundEntity;

public interface CouponRoundJpaRepository
        extends JpaRepository<CouponRoundEntity, Long> {

    boolean existsByTemplateIdAndOpenAt(Long templateId, Instant openAt);

    @Query(value = """
            SELECT coupon.id AS couponRoundId,
                   coupon.template_id AS templateId,
                   coupon.brand_id AS brandId,
                   coupon.name AS name,
                   coupon.policy_type AS policyType,
                   coupon.discount_rate AS discountRate,
                   coupon.max_discount_amount AS maxDiscountAmount,
                   coupon.discount_amount AS discountAmount,
                   coupon.valid_days AS validDays,
                   coupon.eligible_grades_mask AS eligibleGradesMask,
                   coupon.open_at AS openAt,
                   coupon.close_at AS closeAt,
                   coupon.status AS status,
                   stock.total_quantity AS totalQuantity,
                   stock.total_quantity - stock.active_count
                       AS remainingQuantity
              FROM coupons coupon
              JOIN coupon_stocks stock
                ON stock.coupon_id = coupon.id
             WHERE coupon.id = :couponRoundId
            """, nativeQuery = true)
    Optional<CouponRoundDetailProjection> findCouponRoundDetailById(
            @Param("couponRoundId") Long couponRoundId
    );

    @Query(
            value = """
                    SELECT coupon.id AS couponRoundId,
                           coupon.template_id AS templateId,
                           coupon.brand_id AS brandId,
                           coupon.name AS name,
                           coupon.policy_type AS policyType,
                           coupon.discount_rate AS discountRate,
                           coupon.max_discount_amount AS maxDiscountAmount,
                           coupon.discount_amount AS discountAmount,
                           coupon.valid_days AS validDays,
                           coupon.eligible_grades_mask AS eligibleGradesMask,
                           coupon.open_at AS openAt,
                           coupon.close_at AS closeAt,
                           coupon.status AS status,
                           stock.total_quantity AS totalQuantity,
                           stock.total_quantity - stock.active_count
                               AS remainingQuantity
                      FROM coupons coupon
                     JOIN coupon_stocks stock
                        ON stock.coupon_id = coupon.id
                     WHERE (:status IS NULL OR coupon.status = :status)
                       AND (:eligibleGradeBit IS NULL
                            OR (coupon.eligible_grades_mask
                                & :eligibleGradeBit) <> 0)
                     ORDER BY coupon.open_at DESC, coupon.id DESC
                    """,
            countQuery = """
                    SELECT COUNT(*)
                      FROM coupons coupon
                      JOIN coupon_stocks stock
                        ON stock.coupon_id = coupon.id
                     WHERE (:status IS NULL OR coupon.status = :status)
                       AND (:eligibleGradeBit IS NULL
                            OR (coupon.eligible_grades_mask
                                & :eligibleGradeBit) <> 0)
                    """,
            nativeQuery = true
    )
    Page<CouponRoundDetailProjection> findPublicCouponRounds(
            @Param("status") String status,
            @Param("eligibleGradeBit") Integer eligibleGradeBit,
            Pageable pageable
    );

    @Query(value = """
            SELECT coupon.id AS couponRoundId,
                   coupon.template_id AS templateId,
                   coupon.brand_id AS brandId,
                   coupon.name AS name,
                   coupon.policy_type AS policyType,
                   coupon.discount_rate AS discountRate,
                   coupon.max_discount_amount AS maxDiscountAmount,
                   coupon.discount_amount AS discountAmount,
                   coupon.valid_days AS validDays,
                   coupon.eligible_grades_mask AS eligibleGradesMask,
                   coupon.open_at AS openAt,
                   coupon.close_at AS closeAt,
                   coupon.status AS status,
                   stock.total_quantity AS totalQuantity,
                   stock.total_quantity - stock.active_count
                       AS remainingQuantity
              FROM coupons coupon
              JOIN coupon_stocks stock
                ON stock.coupon_id = coupon.id
             WHERE coupon.open_at >= :fromInclusive
               AND coupon.open_at < :toExclusive
             ORDER BY coupon.open_at ASC, coupon.id ASC
            """, nativeQuery = true)
    List<CouponRoundDetailProjection> findCalendarRounds(
            @Param("fromInclusive") Instant fromInclusive,
            @Param("toExclusive") Instant toExclusive
    );

    @Query(
            value = """
                    SELECT coupon.id AS couponRoundId,
                           coupon.brand_id AS brandId,
                           coupon.name AS name,
                           coupon.policy_type AS policyType,
                           coupon.discount_rate AS discountRate,
                           coupon.max_discount_amount AS maxDiscountAmount,
                           coupon.discount_amount AS discountAmount,
                           coupon.valid_days AS validDays,
                           coupon.open_at AS openAt,
                           coupon.close_at AS closeAt,
                           stock.total_quantity - stock.active_count
                               AS remainingQuantity
                    FROM coupons coupon
                    JOIN coupon_stocks stock
                      ON stock.coupon_id = coupon.id
                    WHERE coupon.status = 'OPEN'
                      AND coupon.open_at <= :asOf
                      AND coupon.close_at > :asOf
                      AND (coupon.eligible_grades_mask
                           & :membershipGradeBit) <> 0
                      AND stock.active_count < stock.total_quantity
                      AND NOT EXISTS (
                            SELECT 1
                            FROM issuances issuance
                            WHERE issuance.coupon_id = coupon.id
                              AND issuance.member_id = :memberId
                      )
                    ORDER BY coupon.close_at ASC, coupon.id ASC
                    """,
            countQuery = """
                    SELECT COUNT(*)
                    FROM coupons coupon
                    JOIN coupon_stocks stock
                      ON stock.coupon_id = coupon.id
                    WHERE coupon.status = 'OPEN'
                      AND coupon.open_at <= :asOf
                      AND coupon.close_at > :asOf
                      AND (coupon.eligible_grades_mask
                           & :membershipGradeBit) <> 0
                      AND stock.active_count < stock.total_quantity
                      AND NOT EXISTS (
                            SELECT 1
                            FROM issuances issuance
                            WHERE issuance.coupon_id = coupon.id
                              AND issuance.member_id = :memberId
                      )
                    """,
            nativeQuery = true
    )
    Page<IssuableCouponRoundProjection> findIssuableCouponRounds(
            @Param("memberId") Long memberId,
            @Param("membershipGradeBit") int membershipGradeBit,
            @Param("asOf") Instant asOf,
            Pageable pageable
    );

    @Query("""
            select count(roundEntity)
            from CouponRoundEntity roundEntity
            where roundEntity.openAt < :closeAt
              and roundEntity.closeAt > :openAt
              and roundEntity.status in :statuses
            """)
    long countOverlappingSchedule(
            @Param("openAt") Instant openAt,
            @Param("closeAt") Instant closeAt,
            @Param("statuses") Set<CouponRoundStatus> statuses
    );

    @Query("""
            select roundEntity.id
              from CouponRoundEntity roundEntity
             where roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.OPEN
               and roundEntity.closeAt <= :asOf
             order by roundEntity.id asc
            """)
    List<Long> findClosableOpenRoundIds(@Param("asOf") Instant asOf);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update CouponRoundEntity roundEntity
               set roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.CLOSED
             where roundEntity.id in :roundIds
               and roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.OPEN
               and roundEntity.closeAt <= :asOf
            """)
    int closeOpenRoundsByIds(
            @Param("roundIds") List<Long> roundIds,
            @Param("asOf") Instant asOf
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update CouponRoundEntity roundEntity
               set roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.CLOSED
             where roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.SCHEDULED
               and roundEntity.closeAt <= :asOf
            """)
    int closeMissedScheduledRounds(@Param("asOf") Instant asOf);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE coupons
               SET status = 'OPEN'
             WHERE id = (
                   SELECT candidate.id
                     FROM (
                           SELECT scheduledRound.id
                             FROM coupons scheduledRound
                            WHERE scheduledRound.status = 'SCHEDULED'
                              AND scheduledRound.open_at <= :asOf
                              AND scheduledRound.close_at > :asOf
                              AND NOT EXISTS (
                                    SELECT 1
                                      FROM coupons openRound
                                     WHERE openRound.status = 'OPEN'
                                       AND openRound.open_at < scheduledRound.close_at
                                       AND openRound.close_at > scheduledRound.open_at
                              )
                            ORDER BY scheduledRound.open_at ASC,
                                     scheduledRound.id ASC
                            LIMIT 1
                     ) candidate
             )
            """, nativeQuery = true)
    int openNextScheduledRound(@Param("asOf") Instant asOf);
}
