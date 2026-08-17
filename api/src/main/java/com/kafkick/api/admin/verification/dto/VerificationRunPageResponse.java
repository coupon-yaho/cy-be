package com.kafkick.api.admin.verification.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;

/** 검증 실행 목록의 과거 방향 cursor 응답입니다. */
public record VerificationRunPageResponse(List<VerificationRunSummary> items, String nextBeforeCursor,
                                          boolean hasOlder) {
    /** 한 검증 실행의 목록 표시 필드입니다. */
    public record VerificationRunSummary(Long runId, Instant asOf, ScopeType scope, DatasetType dataset,
                                         VerdictType verdict, StatsStatus statsStatus, long findingCount,
                                         Instant startedAt, Instant finishedAt) { }
}
