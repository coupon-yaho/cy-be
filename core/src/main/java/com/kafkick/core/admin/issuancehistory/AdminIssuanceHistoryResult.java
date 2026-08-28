package com.kafkick.core.admin.issuancehistory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

/** 관리자 발급 상태 변경 이력의 페이지와 필터 모집단 요약입니다. */
public record AdminIssuanceHistoryResult(
        List<HistoryItem> items,
        HistoryPosition nextBefore,
        boolean hasOlder,
        HistorySummary summary
) {

    /** 반환 목록, 다음 Cursor와 요약 간의 일관성을 검증합니다. */
    public AdminIssuanceHistoryResult {
        Objects.requireNonNull(items, "items");
        items = List.copyOf(items);
        Objects.requireNonNull(summary, "summary");
        if (hasOlder && (items.isEmpty() || nextBefore == null)) {
            throw new IllegalArgumentException("이전 이력이 있으면 마지막 항목 Cursor가 필요합니다.");
        }
        if (!hasOlder && nextBefore != null) {
            throw new IllegalArgumentException("이전 이력이 없으면 Cursor가 없어야 합니다.");
        }
    }

    /** HTTP 항목에 이력 ID를 노출하지 않는 발급 상태 변경 이력입니다. */
    public record HistoryItem(
            long issuanceId,
            String issuanceCodeMasked,
            long couponId,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus,
            IssuanceEventType eventType,
            Instant occurredAt
    ) {

        /** 공개할 발급 이력 항목의 필수 식별자와 상태 값을 검증합니다. */
        public HistoryItem {
            if (issuanceId <= 0L || couponId <= 0L) {
                throw new IllegalArgumentException("발급과 쿠폰 ID는 양수여야 합니다.");
            }
            Objects.requireNonNull(issuanceCodeMasked, "issuanceCodeMasked");
            Objects.requireNonNull(toStatus, "toStatus");
            Objects.requireNonNull(eventType, "eventType");
            Objects.requireNonNull(occurredAt, "occurredAt");
        }
    }

    /** 업무 필터가 확정한 전체 모집단의 이벤트 유형별 건수입니다. */
    public record HistorySummary(
            long totalCount,
            long issueCount,
            long useCount,
            long cancelUseCount,
            long cancelCount,
            long expireCount
    ) {

        /** 음수와 합계 불일치를 막고 덧셈 overflow를 명시적으로 감지합니다. */
        public HistorySummary {
            if (totalCount < 0L || issueCount < 0L || useCount < 0L || cancelUseCount < 0L
                    || cancelCount < 0L || expireCount < 0L) {
                throw new IllegalArgumentException("이력 요약 건수는 음수일 수 없습니다.");
            }
            long categorizedCount = Math.addExact(issueCount, useCount);
            categorizedCount = Math.addExact(categorizedCount, cancelUseCount);
            categorizedCount = Math.addExact(categorizedCount, cancelCount);
            categorizedCount = Math.addExact(categorizedCount, expireCount);
            if (totalCount != categorizedCount) {
                throw new IllegalArgumentException("totalCount는 이벤트 유형별 건수의 합계여야 합니다.");
            }
        }
    }
}
