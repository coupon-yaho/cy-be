package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.storage.db.admin.issuancehistory.AdminIssuanceHistoryRowProjection;
import com.kafkick.storage.db.admin.issuancehistory.AdminIssuanceHistorySummaryProjection;

import com.kafkick.storage.db.coupon.entity.IssuanceHistoryEntity;

public interface IssuanceHistoryJpaRepository
        extends JpaRepository<IssuanceHistoryEntity, Long> {
    @Query("select h.id as historyId, h.issuanceId as issuanceId, i.code as issuanceCode, i.couponId as couponId, h.fromStatus as fromStatus, h.toStatus as toStatus, h.eventType as eventType, h.reason as reason, h.requestId as requestId, h.createdAt as occurredAt from IssuanceHistoryEntity h join IssuanceEntity i on i.id=h.issuanceId where h.createdAt<=:snapshotAt and (:couponId is null or i.couponId=:couponId) and (:fromInclusive is null or h.createdAt>=:fromInclusive) and (:toExclusive is null or h.createdAt<:toExclusive) and (:eventType is null or h.eventType=:eventType) and (:beforeAt is null or h.createdAt<:beforeAt or (h.createdAt=:beforeAt and h.id<:beforeId)) order by h.createdAt desc,h.id desc") List<AdminIssuanceHistoryRowProjection> findAdminHistoryRows(@Param("couponId") Long couponId,@Param("fromInclusive") Instant fromInclusive,@Param("toExclusive") Instant toExclusive,@Param("eventType") IssuanceEventType eventType,@Param("snapshotAt") Instant snapshotAt,@Param("beforeAt") Instant beforeAt,@Param("beforeId") Long beforeId,Pageable pageable);
    @Query("select sum(case when h.eventType='ISSUE' then 1 else 0 end) as issueCount,sum(case when h.eventType='USE' then 1 else 0 end) as useCount,sum(case when h.eventType='CANCEL_USE' then 1 else 0 end) as cancelUseCount,sum(case when h.eventType='CANCEL' then 1 else 0 end) as cancelCount,sum(case when h.eventType='EXPIRE' then 1 else 0 end) as expireCount from IssuanceHistoryEntity h join IssuanceEntity i on i.id=h.issuanceId where h.createdAt<=:snapshotAt and (:couponId is null or i.couponId=:couponId) and (:fromInclusive is null or h.createdAt>=:fromInclusive) and (:toExclusive is null or h.createdAt<:toExclusive) and (:eventType is null or h.eventType=:eventType)") AdminIssuanceHistorySummaryProjection summarizeAdminHistoryRows(@Param("couponId") Long couponId,@Param("fromInclusive") Instant fromInclusive,@Param("toExclusive") Instant toExclusive,@Param("eventType") IssuanceEventType eventType,@Param("snapshotAt") Instant snapshotAt);
}
