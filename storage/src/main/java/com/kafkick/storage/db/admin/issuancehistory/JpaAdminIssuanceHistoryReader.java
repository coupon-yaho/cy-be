package com.kafkick.storage.db.admin.issuancehistory;

import java.time.Instant;
import java.util.List;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryReadResult;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryReader;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource.RawHistory;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;
import com.kafkick.storage.db.coupon.repository.IssuanceHistoryJpaRepository;
/** DB가 필터, Keyset, limit+1 및 요약을 수행하는 관리자 이력 Reader입니다. */
@Component
public class JpaAdminIssuanceHistoryReader implements AdminIssuanceHistoryReader {
    private final IssuanceHistoryJpaRepository repository;

    /** Repository를 주입받습니다. */
    public JpaAdminIssuanceHistoryReader(IssuanceHistoryJpaRepository repository) {
        this.repository = repository;
    }

    /** DB 제한 후보와 Cursor 전 전체 요약을 반환합니다. */
    @Override
    public AdminIssuanceHistoryReadResult read(AdminIssuanceHistoryQuery query, Instant snapshotAt) {
        try {
            List<RawHistory> candidates = readCandidates(query, snapshotAt);
            HistorySummary summary = readSummary(query, snapshotAt);
            return new AdminIssuanceHistoryReadResult(candidates, summary);
        } catch (DataAccessException exception) {
            throw new BusinessException(CommonErrorCode.INTERNAL_ERROR,
                    "발급 이력 원천 조회에 실패했습니다.", exception);
        }
    }

    /** Keyset 조건과 limit + 1을 적용한 후보를 원시 행으로 변환합니다. */
    private List<RawHistory> readCandidates(AdminIssuanceHistoryQuery query, Instant snapshotAt) {
        Instant beforeAt = query.before() == null ? null : query.before().occurredAt();
        Long beforeId = query.before() == null ? null : query.before().historyId();
        return repository.findAdminHistoryRows(query.couponId(), query.fromInclusive(),
                query.toExclusive(), query.eventType(), snapshotAt, beforeAt, beforeId,
                PageRequest.of(0, query.limit() + 1)).stream().map(this::toRawHistory).toList();
    }

    /** Cursor와 limit 이전의 동일 업무 모집단 Summary를 조립합니다. */
    private HistorySummary readSummary(AdminIssuanceHistoryQuery query, Instant snapshotAt) {
        AdminIssuanceHistorySummaryProjection projection = repository.summarizeAdminHistoryRows(
                query.couponId(), query.fromInclusive(), query.toExclusive(), query.eventType(), snapshotAt);
        long issueCount = zero(projection.getIssueCount()); long useCount = zero(projection.getUseCount());
        long cancelUseCount = zero(projection.getCancelUseCount()); long cancelCount = zero(projection.getCancelCount());
        long expireCount = zero(projection.getExpireCount());
        return new HistorySummary(issueCount + useCount + cancelUseCount + cancelCount + expireCount,
                issueCount, useCount, cancelUseCount, cancelCount, expireCount);
    }

    /** Projection을 Core 원시 이력으로 바꿉니다. */
    private RawHistory toRawHistory(AdminIssuanceHistoryRowProjection row) {
        return new RawHistory(row.getHistoryId(), row.getIssuanceId(), row.getIssuanceCode(), row.getCouponId(),
                row.getFromStatus(), row.getToStatus(), row.getEventType(), row.getReason(), row.getRequestId(), row.getOccurredAt());
    }

    /** 조건부 집계의 null을 0건으로 정규화합니다. */
    private static long zero(Long value) { return value == null ? 0L : value; }
}
