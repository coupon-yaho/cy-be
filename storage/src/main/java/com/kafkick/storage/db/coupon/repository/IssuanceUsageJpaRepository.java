package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.storage.db.coupon.entity.IssuanceUsageEntity;

public interface IssuanceUsageJpaRepository
        extends JpaRepository<IssuanceUsageEntity, Long> {

    Optional<IssuanceUsageEntity> findByIssuanceIdAndCanceledAtIsNull(
            Long issuanceId
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE IssuanceUsageEntity usage
            SET usage.canceledAt = :canceledAt
            WHERE usage.id = :usageId
              AND usage.canceledAt IS NULL
            """)
    int cancelIfActive(
            @Param("usageId") Long usageId,
            @Param("canceledAt") Instant canceledAt
    );
}
