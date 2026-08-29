package com.kafkick.batch.api;

import java.time.LocalDateTime;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;

/**
 * 검증 실행 이력 한 줄.
 *
 * <p>core 레코드를 그대로 내보내지 않는다. 그러면 HTTP 계약이 도메인 모델에 묶여, 거기에
 * 필드를 더하는 다음 티켓이 응답 스키마를 의도 없이 바꾼다 — 그 티켓의 diff 에는 이
 * 컨트롤러가 없어 리뷰에서도 안 잡힌다. verification_runs 에는 이미 레코드가 아직 안 받은
 * origin 컬럼이 있다.
 *
 * <p>필드는 VerifyRunView 와 같은 축으로 고른다. fromTs 와 seedRunId 는 안 싣는다 —
 * 화면이 안 쓰고, 내보내겠다는 결정을 한 적이 없다.
 */
public record VerifyHistoryView(
        Long runId,
        DatasetType dataset,
        ScopeType scope,
        int attempt,
        LocalDateTime asOf,
        VerdictType verdict,
        StatsStatus statsStatus,
        int findingCount,
        String findingsChecksum,
        String datasetFingerprint,
        LocalDateTime startedAt,
        LocalDateTime finishedAt) {

    public static VerifyHistoryView of(VerificationRun run) {
        return new VerifyHistoryView(
                run.id(),
                run.dataset(),
                run.scope(),
                run.attempt(),
                run.asOf(),
                run.verdict(),
                run.statsStatus(),
                run.findingCount(),
                run.findingsChecksum(),
                run.datasetFingerprint(),
                run.startedAt(),
                run.finishedAt());
    }
}
