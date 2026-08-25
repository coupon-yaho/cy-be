package com.kafkick.core.admin.issuancehistory;

import java.util.List;
import java.util.Objects;

/** Reader가 반환한 제한 후보와 Cursor 이전 전체 모집단 요약입니다. */
public record AdminIssuanceHistoryReadResult(
        List<AdminIssuanceHistorySource.RawHistory> candidates,
        AdminIssuanceHistoryResult.HistorySummary summary
) {
    /** 후보 목록과 전체 모집단 요약의 null을 막고 불변 복사합니다. */
    public AdminIssuanceHistoryReadResult {
        Objects.requireNonNull(candidates, "candidates");
        candidates = List.copyOf(candidates);
        Objects.requireNonNull(summary, "summary");
    }
}
