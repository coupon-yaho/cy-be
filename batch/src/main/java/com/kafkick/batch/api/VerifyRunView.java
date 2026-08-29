// 검증 실행 하나의 현재 상태입니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.core.job.JobExecution;

import com.kafkick.batch.config.BatchTimeAxis;
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
                // **배치 메타 축을 그대로 내보내면 응답 안에 축이 둘이 된다.** 바로 아래
                // asOf 는 도메인 값(UTC)인데 이 둘만 JVM 기본 존이면, 사고를 시간축으로
                // 맞춰 볼 때 어느 API 를 열었느냐로 답이 오프셋만큼 갈린다 —
                // /verify/report 는 DB 행(UTC)을 싣는다. 같은 실행이 두 답을 내면 안 된다.
                // 실행 행이 있으면 그 값을 쓰고(이미 도메인 축이다), 없으면 여기서 옮긴다.
                run.map(VerificationRun::startedAt)
                        .orElseGet(() -> startTimeOnDomainAxis(execution)),
                endTimeOnDomainAxis(execution),
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
     * 이 API 에는 사용자 인증이 없다 — 공유 비밀 관문(CY-742)은 소지만 묻고 <b>누가</b> 불렀는지는
     * 안 가른다. 그래서 <b>허용 목록만 통과시킨다</b> — 그 안에서 우리가
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

    /**
     * <b>{@code START_TIME} 은 잡이 실제로 시작하기 전까지 {@code null} 이다.</b>
     * {@code AbstractJob.execute} 가 <b>태스크 실행기 스레드 안에서</b> 찍고(6.0.4 바이트코드),
     * 상태가 {@code STOPPING} 이면 <b>아예 안 찍는다</b>. 트리거는 비동기라
     * ({@code VerifyExecutorConfig}) {@code 202} 를 받은 클라이언트가 곧바로 폴링하면 그 창에
     * 들어간다. 실행기가 거절해 {@code FAILED} 로 남은 행은 <b>영원히</b> {@code null} 이다.
     *
     * <p><b>여기서 던지면 인증 없는 조회 API 가 규약에 없는 500 을 낸다.</b> 이 API 를 연 이유가
     * <i>"폴링해야 원인을 안다"</i> 를 없애는 것인데, 그 폴링의 <b>첫 호출</b>이 깨진다.
     * {@code BatchTimeAxis} 는 널가드를 일부러 뺐다 — 잡 안의 호출부는 도달 불가라서다.
     * <b>여기는 그 전제가 안 맞으므로 부르는 쪽이 본다.</b>
     */
    private static LocalDateTime startTimeOnDomainAxis(JobExecution execution) {
        LocalDateTime startTime = execution.getStartTime();
        return startTime == null ? null : BatchTimeAxis.onDomainAxis(startTime);
    }

    /**
     * <b>종료 시각은 두 출처가 갈린다.</b> 도는 중이면 배치 메타의 {@code getEndTime()} 이
     * {@code null} 이고, 끝났으면 값이 있다. 실행 행의 {@code finishedAt} 은 <b>판정을 낸
     * 뒤에만</b> 채워지므로(통계 Step 이 남아 있어도 잡은 안 끝난 상태다) 둘을 그냥 바꿔치기하면
     * <i>"끝났는데 종료가 비어 있다"</i> 가 나온다. 배치 메타를 먼저 보고 옮긴다.
     */
    private static LocalDateTime endTimeOnDomainAxis(JobExecution execution) {
        LocalDateTime endTime = execution.getEndTime();
        return endTime == null ? null : BatchTimeAxis.onDomainAxis(endTime);
    }

}
