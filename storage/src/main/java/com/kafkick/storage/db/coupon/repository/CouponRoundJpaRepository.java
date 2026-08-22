package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.Set;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.storage.db.coupon.entity.CouponRoundEntity;

public interface CouponRoundJpaRepository
        extends JpaRepository<CouponRoundEntity, Long> {

    boolean existsByTemplateIdAndOpenAt(Long templateId, Instant openAt);

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

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update CouponRoundEntity roundEntity
               set roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.CLOSED
             where roundEntity.status = com.kafkick.core.coupon.domain.CouponRoundStatus.OPEN
               and roundEntity.closeAt <= :asOf
            """)
    int closeOpenRounds(@Param("asOf") Instant asOf);

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
