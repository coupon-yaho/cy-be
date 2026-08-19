package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.coupon.entity.IssuanceEntity;

public interface IssuanceJpaRepository
        extends JpaRepository<IssuanceEntity, Long> {

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
