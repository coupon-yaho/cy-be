package com.kafkick.core.verification;

import java.time.LocalDateTime;

/**
 * 검증 실행 한 건. 같은 asOf 로 다시 돌리면 같은 결과가 나와야 한다.
 *
 * <p>attempt 가 없으면 재실행이 아예 안 된다 — Spring Batch 가 동일 JobParameters 재실행을
 * 차단하고, DB 도 uk_run_params (as_of, dataset, scope, attempt) 로 같이 막는다.
 *
 * <p>verdict 와 statsStatus 를 나눈 이유는 "검증은 됐는데 통계는 안 만들었다" 를 표현하기
 * 위해서다. CORRUPT 실행이 정확히 그 상태다.
 */
public record VerificationRun(
        Long id,
        LocalDateTime asOf,
        LocalDateTime fromTs,
        ScopeType scope,
        DatasetType dataset,
        int attempt,
        VerdictType verdict,
        StatsStatus statsStatus,
        int findingCount,
        String findingsChecksum,
        String datasetFingerprint,
        LocalDateTime startedAt,
        LocalDateTime finishedAt
) {

    public VerificationRun {
        validateAsOf(asOf);
        validateScope(scope, fromTs);
        validateDataset(dataset);
        validateAttempt(attempt);
        validateStartedAt(startedAt);
        validateFindingCount(findingCount);
    }

    public static VerificationRun start(
            LocalDateTime asOf,
            LocalDateTime fromTs,
            ScopeType scope,
            DatasetType dataset,
            int attempt,
            LocalDateTime startedAt
    ) {
        return new VerificationRun(
                null, asOf, fromTs, scope, dataset, attempt,
                null, null, 0, null, null, startedAt, null
        );
    }

    public static VerificationRun restore(
            Long id,
            LocalDateTime asOf,
            LocalDateTime fromTs,
            ScopeType scope,
            DatasetType dataset,
            int attempt,
            VerdictType verdict,
            StatsStatus statsStatus,
            int findingCount,
            String findingsChecksum,
            String datasetFingerprint,
            LocalDateTime startedAt,
            LocalDateTime finishedAt
    ) {
        if (id == null) {
            throw new IllegalArgumentException("복원하려면 검증 실행 ID가 필요합니다.");
        }

        return new VerificationRun(
                id, asOf, fromTs, scope, dataset, attempt,
                verdict, statsStatus, findingCount,
                findingsChecksum, datasetFingerprint, startedAt, finishedAt
        );
    }

    /** 판정을 확정한다. 통계 상태는 Step 7 이 따로 채운다. */
    public VerificationRun finish(
            VerdictType decided,
            int detectedCount,
            String checksum,
            String fingerprint,
            LocalDateTime finishedTime
    ) {
        if (decided == null) {
            throw new IllegalArgumentException("검증 판정이 필요합니다.");
        }
        if (finishedTime == null) {
            throw new IllegalArgumentException("검증 종료 시각이 필요합니다.");
        }

        return new VerificationRun(
                id, asOf, fromTs, scope, dataset, attempt,
                decided, statsStatus, detectedCount,
                checksum, fingerprint, startedAt, finishedTime
        );
    }

    public VerificationRun withStatsStatus(StatsStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("통계 상태가 필요합니다.");
        }

        return new VerificationRun(
                id, asOf, fromTs, scope, dataset, attempt,
                verdict, status, findingCount,
                findingsChecksum, datasetFingerprint, startedAt, finishedAt
        );
    }

    /** 합격 판정을 지는 것은 전수 실행뿐이다. 증분은 관측용이다. */
    public boolean decidesVerdict() {
        return scope == ScopeType.FULL;
    }

    private static void validateAsOf(LocalDateTime asOf) {
        if (asOf == null) {
            throw new IllegalArgumentException("검증 기준 시각이 필요합니다.");
        }
    }

    private static void validateScope(ScopeType scope, LocalDateTime fromTs) {
        if (scope == null) {
            throw new IllegalArgumentException("검증 범위가 필요합니다.");
        }
        if (scope == ScopeType.INCREMENTAL && fromTs == null) {
            throw new IllegalArgumentException("증분 검증에는 시작 시각이 필요합니다.");
        }
        if (scope == ScopeType.FULL && fromTs != null) {
            throw new IllegalArgumentException("전수 검증에는 시작 시각을 지정할 수 없습니다.");
        }
    }

    private static void validateDataset(DatasetType dataset) {
        if (dataset == null) {
            throw new IllegalArgumentException("검증 대상 데이터셋이 필요합니다.");
        }
    }

    private static void validateAttempt(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("재실행 횟수는 1 이상이어야 합니다.");
        }
    }

    private static void validateStartedAt(LocalDateTime startedAt) {
        if (startedAt == null) {
            throw new IllegalArgumentException("검증 시작 시각이 필요합니다.");
        }
    }

    private static void validateFindingCount(int findingCount) {
        if (findingCount < 0) {
            throw new IllegalArgumentException("검출 건수는 0 이상이어야 합니다.");
        }
    }
}
