package com.kafkick.batch.api;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <b>진행 중인 검증 실행의 중간 상태.</b>
 *
 * <p>{@code VerifyReportView} 와 일부러 다른 타입이다. 그쪽은 <b>마감된</b> 실행만 싣고
 * 매니페스트 대조까지 붙는 <i>확정 판정</i>이라, 진행 중 값을 그 그릇에 담으면 화면이
 * "절반만 센 검출" 을 최종 결과로 그린다. 게이트와 {@code dump-verify-report.sh} 가
 * 같은 질의를 쓰므로 그 혼동은 증적까지 오염시킨다.
 *
 * <p><b>{@code findingCount} 는 {@code verification_runs.finding_count} 가 아니다.</b>
 * 그 컬럼은 판정 Step 이 마감할 때 한 번 쓰이므로 진행 중에는 비어 있다. 여기서는
 * {@code verification_findings} 를 직접 센다 — 규칙 Step 이 커밋할 때마다 늘어난다
 * (실측: 60만 발급 오염셋에서 0 → 10 → 56 → 184 → 300 → 800 으로 계단식).
 *
 * @param status {@code RUNNING} · {@code DONE}. 배치 메타의 잡 상태가 아니라 <b>이 실행
 *               행에서 파생</b>한 값이다 — {@code verification_runs} 에 잡 실행 식별자가
 *               없어서 조인하면 시작 시각으로 맞춰야 하는데, 그 짝짓기는 같은 초에 두 실행이
 *               들어오면 틀린다. 잡 자체의 상태는 {@code /batch/runs} 가 답한다.
 */
public record VerifyProgressView(
        long runId,
        DatasetType dataset,
        ScopeType scope,
        int attempt,
        LocalDateTime asOf,
        String status,
        VerdictType verdict,
        int findingCount,
        Map<FindingType, Integer> byType,
        LocalDateTime startedAt,
        LocalDateTime finishedAt) {

    /** 아직 판정이 안 난 실행. 화면은 이 값으로 폴링을 계속할지 정한다. */
    public static final String RUNNING = "RUNNING";

    /** 판정이 커밋된 실행. 이 뒤로는 수치가 안 변한다. */
    public static final String DONE = "DONE";

    public static VerifyProgressView of(
            VerificationRun run, int findingCount, Map<FindingType, Integer> counted) {

        // 여섯 유형을 항상 다 싣는다. 0 인 유형을 빼면 화면이 "아직 안 센 것" 과
        // "세었는데 없는 것" 을 구별하지 못한다 — VerifyReportView 와 같은 규율이다.
        Map<FindingType, Integer> filled = new LinkedHashMap<>();
        for (FindingType type : FindingType.values()) {
            filled.put(type, counted == null ? 0 : counted.getOrDefault(type, 0));
        }

        // 판정과 마감 시각을 함께 본다. 하나만 보면 마감 직전 순간에 둘이 갈린다.
        boolean done = run.verdict() != null && run.finishedAt() != null;

        return new VerifyProgressView(
                run.id(), run.dataset(), run.scope(), run.attempt(), run.asOf(),
                done ? DONE : RUNNING, run.verdict(), findingCount,
                Collections.unmodifiableMap(filled), run.startedAt(), run.finishedAt());
    }
}
