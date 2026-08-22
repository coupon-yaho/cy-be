// 검증 실행 하나의 현재 상태입니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.core.job.JobExecution;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;

/**
 * <b>두 축을 한 응답에 담되 섞지 않는다.</b>
 *
 * <ul>
 *   <li>{@code status} — 잡이 <b>돌았는가</b>. Spring Batch 의 {@code BatchStatus}
 *   <li>{@code verdict} — 데이터가 <b>맞는가</b>. {@code verification_runs.verdict}
 * </ul>
 *
 * <p><b>둘은 독립이다.</b> Step 체인이 {@code finalizeRunStep → statsAggregateStep} 이라
 * 통계 Step 이 죽으면 <b>잡은 {@code FAILED} 인데 verdict 는 이미 커밋돼 있다.</b> 반대로
 * {@code verdict = FAIL} 인 실행은 잡이 {@code COMPLETED} 다 — 불일치는 실행 실패가 아니라
 * <b>판정 결과</b>이기 때문이다. 한 축으로 접으면 오진한다.
 *
 * <p><b>{@code exitCode} 는 안 싣는다.</b> {@code SimpleJob} 이 잡 종료 코드를 <b>마지막
 * Step 값으로 대입</b>하는데, {@code statsAggregateStep} 은 CORRUPT 에서 항상
 * {@code "SKIPPED"} 를 세운다. 그러면 <b>정상적으로 끝난 오염셋 검증이
 * {@code exitCode: "SKIPPED"}</b> 로 보인다 — 성공 판정에 쓰면 안 되는 값을 이름 없이
 * 노출하면 누군가 반드시 그것으로 분기한다. 성공은 {@code verdict} + {@code statsStatus} 다.
 *
 * <p><b>{@code runId} 가 {@code null} 인 것도 정보다.</b> 아직 판정 단계에 못 갔거나,
 * 가드에 걸려 끝까지 못 가는 실행이라는 뜻이다.
 */
public record VerifyRunView(
        long executionId,
        Long runId,
        String status,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        VerdictType verdict,
        StatsStatus statsStatus,
        Integer findingCount,
        DatasetType dataset,
        ScopeType scope,
        Integer attempt,
        LocalDateTime asOf,
        String datasetFingerprint,
        String findingsChecksum,
        String failure
) {

    /** 우리가 정의한 도메인 에러코드. 원문에서 이것만 그대로 통과시킨다. */
    private static final Pattern DOMAIN_CODE = Pattern.compile("VERIFICATION-\\d{3}");

    /** 그 밖에는 예외 <b>이름</b>만 남긴다. 메시지에는 SQL 조각이 섞일 수 있다. */
    private static final Pattern EXCEPTION_TYPE =
            Pattern.compile("([A-Za-z]+(?:Exception|Error))");

    public static VerifyRunView of(long executionId, JobExecution execution, Long runId,
            Optional<VerificationRun> run) {
        return new VerifyRunView(
                executionId,
                runId,
                execution.getStatus().name(),
                execution.getStartTime(),
                execution.getEndTime(),
                run.map(VerificationRun::verdict).orElse(null),
                run.map(VerificationRun::statsStatus).orElse(null),
                run.map(VerificationRun::findingCount).orElse(null),
                run.map(VerificationRun::dataset).orElse(null),
                run.map(VerificationRun::scope).orElse(null),
                run.map(VerificationRun::attempt).orElse(null),
                run.map(VerificationRun::asOf).orElse(null),
                run.map(VerificationRun::datasetFingerprint).orElse(null),
                run.map(VerificationRun::findingsChecksum).orElse(null),
                firstFailure(execution));
    }

    /**
     * <b>어느 Step 에서 죽었는지와, 우리 도메인 코드가 있으면 그것을 싣는다.</b>
     *
     * <p><b>{@code getAllFailureExceptions()} 를 쓰면 안 된다 — 언제나 비어 있다.</b>
     * 그 필드는 영속되지 않고 이 {@code JobExecution} 은 DB 에서 새로 만든 객체다.
     * 처음에 그것을 썼다가 <b>모든 실패가 {@code failure: null} 로 나가는 것</b>을 실측으로
     * 확인했다 — 트리거를 연 이유가 <i>"폴링해야 원인을 안다"</i> 를 없애는 것이었는데
     * 절반만 됐던 자리다.
     *
     * <p><b>그렇다고 {@code exitDescription} 을 그대로 실을 수도 없다.</b> 거기에는
     * 스택트레이스가 통째로 들어가고 첫 줄에도 SQL 조각·드라이버 오류·제약 이름이 섞인다.
     * 이 API 에는 인증이 없다. 그래서 <b>허용 목록만 통과시킨다</b> — 그 안에서 우리가
     * 정의한 {@code VERIFICATION-0xx} 코드를 찾아 싣고, 없으면 예외 <b>클래스 이름</b>만
     * 남긴다. 자세한 원인은 서버 로그가 진다.
     */
    private static String firstFailure(JobExecution execution) {
        return execution.getStepExecutions().stream()
                .filter(step -> step.getStatus().isUnsuccessful())
                .findFirst()
                .map(step -> step.getStepName() + ": "
                        + summarize(step.getExitStatus().getExitDescription()))
                .orElse(null);
    }

    /** 도메인 에러코드가 보이면 그것을, 아니면 예외 클래스 이름만. 둘 다 없으면 {@code null}. */
    private static String summarize(String description) {
        if (description == null || description.isBlank()) {
            return "원인이 기록되지 않았습니다";
        }
        Matcher code = DOMAIN_CODE.matcher(description);
        if (code.find()) {
            return code.group();
        }
        Matcher type = EXCEPTION_TYPE.matcher(description);
        return type.find() ? type.group(1) : "알 수 없는 오류";
    }
}
