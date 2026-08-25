package com.kafkick.storage.db.admin.issuancehistory;
import java.time.Instant;
import java.util.List;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import com.kafkick.core.admin.issuancehistory.*;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource.RawHistory;
import com.kafkick.core.support.exception.*;
import com.kafkick.storage.db.coupon.repository.IssuanceHistoryJpaRepository;
/** DB가 필터, Keyset, limit+1 및 요약을 수행하는 관리자 이력 Reader입니다. */
public class JpaAdminIssuanceHistoryReader implements AdminIssuanceHistoryReader {
 private final IssuanceHistoryJpaRepository repository;
 /** Repository를 주입받습니다. */ public JpaAdminIssuanceHistoryReader(IssuanceHistoryJpaRepository repository){this.repository=repository;}
 /** DB 제한 후보와 Cursor 전 전체 요약을 반환합니다. */ @Override public AdminIssuanceHistoryReadResult read(AdminIssuanceHistoryQuery q,Instant snapshotAt){try{Instant beforeAt=q.before()==null?null:q.before().occurredAt();Long beforeId=q.before()==null?null:q.before().historyId();List<RawHistory> candidates=repository.findAdminHistoryRows(q.couponId(),q.fromInclusive(),q.toExclusive(),q.eventType(),snapshotAt,beforeAt,beforeId,PageRequest.of(0,q.limit()+1)).stream().map(r->new RawHistory(r.getHistoryId(),r.getIssuanceId(),r.getIssuanceCode(),r.getCouponId(),r.getFromStatus(),r.getToStatus(),r.getEventType(),r.getReason(),r.getRequestId(),r.getOccurredAt())).toList();AdminIssuanceHistorySummaryProjection s=repository.summarizeAdminHistoryRows(q.couponId(),q.fromInclusive(),q.toExclusive(),q.eventType(),snapshotAt);long i=z(s.getIssueCount()),u=z(s.getUseCount()),cu=z(s.getCancelUseCount()),c=z(s.getCancelCount()),e=z(s.getExpireCount());return new AdminIssuanceHistoryReadResult(candidates,new HistorySummary(i+u+cu+c+e,i,u,cu,c,e));}catch(DataAccessException e){throw new BusinessException(CommonErrorCode.INTERNAL_ERROR,"발급 이력 원천 조회에 실패했습니다.",e);}}
 private static long z(Long value){return value==null?0L:value;}
}
