package com.kafkick.batch.api;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import java.time.Duration;
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
 * @param status {@code RUNNING} · {@code STALE} · {@code DONE}. 배치 메타의 잡 상태가
 *               아니라 <b>이 실행 행에서 파생</b>한 값이다 — {@code verification_runs} 에 잡
 *               실행 식별자가 없어서 조인하면 시작 시각으로 맞춰야 하는데, 그 짝짓기는 같은
 *               초에 두 실행이 들어오면 틀린다. 잡 자체의 상태는 {@code /batch/runs} 가 답한다.
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

    /** 아직 판정이 안 났고, 아직 돌고 있을 만한 실행. 화면은 이 값에서만 폴링을 잇는다. */
    public static final String RUNNING = "RUNNING";

    /**
     * 판정이 안 났는데 <b>돌고 있다고 보기엔 너무 오래된</b> 실행.
     *
     * <p>이 값이 없으면 화면이 영원히 폴링한다 — {@code finalizeRunStep} 앞에서 죽은 잡은
     * {@code verdict} 를 못 쓰고, 그 행을 닫아 주는 경로가 없어서 열린 채로 남는다.
     * 판정 전에 죽는 것은 이 저장소에서 <b>정상 경로</b>다(얼림 가드·역전 검사가 일부러
     * 그렇게 죽인다). 그래서 "안 끝났다" 와 "안 끝날 것이다" 를 갈라야 한다.
     *
     * <p>경계는 {@code batch.stuck-job-after-ms}(기본 30분)를 그대로 쓴다 —
     * {@code RunningJobProbe} 가 "멈춘 잡" 을 판정하는 값과 같아야 두 화면이 안 갈린다.
     */
    public static final String STALE = "STALE";

    /**
     * 판정이 커밋된 실행.
     *
     * <p><b>수치가 안 변한다고 보장하지는 않는다.</b> 마감된 실행에 검출을 더 붙이는 것을
     * 막는 상태 검사도 DB 제약도 없다 — 배치가 그러지 않을 뿐이다. 화면은 이 값을
     * <i>"판정이 났으니 폴링을 멈춰도 된다"</i> 로만 읽어야 한다.
     */
    public static final String DONE = "DONE";

    public static VerifyProgressView of(
            VerificationRun run, int findingCount, Map<FindingType, Integer> counted,
            LocalDateTime now, Duration stuckAfter) {

        // 여섯 유형을 항상 다 싣는다. 0 인 유형을 빼면 화면이 "아직 안 센 것" 과
        // "세었는데 없는 것" 을 구별하지 못한다 — VerifyReportView 와 같은 규율이다.
        Map<FindingType, Integer> filled = new LinkedHashMap<>();
        for (FindingType type : FindingType.values()) {
            filled.put(type, counted == null ? 0 : counted.getOrDefault(type, 0));
        }

        // **verdict IS NULL 이 곧 "판정을 못 냈다" 다.** 이 저장소가 여러 곳에서 쓰는 등식이고
        // (CleanupJdbcAdapter#abandonedRunIds), finished_at 을 함께 걸지 않는다 — 닫혔는데
        // 판정이 비어 있는 행도 "판정을 못 낸" 것이다. 그 조합은 VerificationMetricsUnknown 이 본다.
        String status;
        if (run.verdict() != null) {
            status = DONE;
        } else {
            // 판정 전에 죽은 잡은 이 행을 열린 채로 남긴다. 나이로 가르지 않으면
            // 화면이 그 행을 영원히 폴링한다.
            status = run.startedAt().isBefore(now.minus(stuckAfter)) ? STALE : RUNNING;
        }

        return new VerifyProgressView(
                run.id(), run.dataset(), run.scope(), run.attempt(), run.asOf(),
                status, run.verdict(), findingCount,
                Collections.unmodifiableMap(filled), run.startedAt(), run.finishedAt());
    }
}
