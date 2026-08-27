package com.kafkick.batch.api;

import java.time.Duration;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.config.RunningJobProbe.StuckRun;
import com.kafkick.batch.config.VerifyExecutorConfig;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * <b>{@code verifyJob} 의 중단을 오케스트레이션한다.</b> 진도가 멈춘 실행만 멈춘다.
 *
 * <p><b>{@code api} 에 둔다.</b> {@code ExpireRecoveryService} 와 같은 이유다 — README 의
 * 패키지 표를 팀 규칙으로 쓰고 코드가 거기 맞춘다. 이것은 {@code VerifyTriggerController}
 * 전용 오케스트레이션이다.
 *
 * <p><b>왜 컨트롤러가 아니라 여기인가.</b> 판정과 쓰기 사이를 트랜잭션으로 묶어야 하는데,
 * 그러려면 SQL 과 {@code @Transactional} 이 필요하다. 컨트롤러에 SQL 을 넣는 것은 이
 * 저장소의 패턴이 아니고, 형제({@code ExpireRecoveryService})가 이미 이 모양이다.
 */
@Service
public class VerifyStopService {

    private static final Logger log =
            LoggerFactory.getLogger(VerifyStopService.class);

    /**
     * <b>진도 조건을 쓰기 문장 안에 다시 건다.</b> 판정과 쓰기가 따로면 그 사이에 실행이
     * 되살아날 수 있고 — 락 대기가 풀리는 경우가 그렇다 — 그때 <b>살아 있는 검증을
     * {@code STOPPED} 로 올린다.</b> 그 순간 만료·정리가 이 실행에 물러나기를 그만두는데
     * 스레드는 아직 돌아, V1·V3·V5 가 반쯤 쓰인 상태를 읽고 <b>예외 없이 조용히 틀린 답</b>을
     * 낸다. {@code ExpireRecoveryService.CLAIM} 과 같은 장치다.
     *
     * <p><b>{@code STOPPING} 을 뺀다.</b> 만료 쪽은 넣지만 이쪽은 안 넣는다 —
     * {@code SimpleJobOperator.stop} 이 {@code STARTED}·{@code STARTING} 만 받고 나머지에
     * {@code JobExecutionNotRunningException} 을 던진다(6.0.4 바이트코드로 확인). 넣으면
     * 선점은 성공했는데 바로 다음 줄이 던져 <b>아무것도 안 한 채 VERSION 만 올린다.</b>
     *
     * <p><b>폴백까지 그대로 옮긴다.</b> {@code MAX(LAST_UPDATED)} → {@code START_TIME} →
     * {@code CREATE_TIME} 은 {@link RunningJobProbe} 의 판정과 같아야 한다. 갈리면 판정과
     * 쓰기가 다른 컬럼을 본다.
     */
    static final String CLAIM = """
            UPDATE BATCH_JOB_EXECUTION je
               SET je.VERSION = je.VERSION + 1
             WHERE je.JOB_EXECUTION_ID = :id
               AND je.STATUS IN ('STARTING','STARTED')
               AND COALESCE((SELECT MAX(se.LAST_UPDATED) FROM BATCH_STEP_EXECUTION se
                              WHERE se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID),
                            je.START_TIME, je.CREATE_TIME) <= :stuckBefore
            """;

    /**
     * <b>{@code force=true} 의 선점문.</b> 진도 조건을 뺀 것 말고는 {@link #CLAIM} 과 같다.
     *
     * <p><b>왜 여는가.</b> {@code stop} 은 죽이는 것이 아니라 협조적 중단이다 —
     * {@code terminateOnly} 를 세우고 다음 청크 경계에서 멈춘다. 그런데 이 저장소에는
     * <b>잡 전체 데드라인이 없고</b>({@code VerifyTriggerController} 의 근거 참고)
     * {@code replayStep} 은 느리게라도 커밋하는 한 <b>영원히 시체가 안 된다.</b> 즉 시체
     * 게이트만 두면 잘못 건 300만 행 {@code CORRUPT FULL} 을 <b>끝날 때까지 못 세운다</b> —
     * 그 사이 만료 크론이 {@code issuances.updated_at} 을 찍고, 그 {@code asOf} 는
     * 재시딩 말고 복구가 없다. 막아서 생기는 손해가 더 크다.
     *
     * <p><b>왜 기본값이 아닌가.</b> 도는 검증을 멈추면 그 순간 만료·정리가 물러나기를
     * 그만두는데 스레드는 아직 돈다. 파라미터 이름 자체가 방어라, 우발적 호출은 여전히
     * {@code VERIFICATION-019} 로 막힌다.
     */
    static final String FORCE_CLAIM = """
            UPDATE BATCH_JOB_EXECUTION je
               SET je.VERSION = je.VERSION + 1
             WHERE je.JOB_EXECUTION_ID = :id
               AND je.STATUS IN ('STARTING','STARTED')
            """;

    /** 선점에 진 뒤 <b>현재 읽기</b>로 상태를 본다. 스냅샷 읽기는 옛 값을 준다(RR). */
    private static final String CURRENT_STATUS = """
            SELECT STATUS FROM BATCH_JOB_EXECUTION
             WHERE JOB_EXECUTION_ID = :id FOR SHARE
            """;

    private final JobOperator verifyJobOperator;
    private final JobRepository jobRepository;
    private final RunningJobProbe runningJobs;
    private final JdbcClient jdbcClient;

    public VerifyStopService(
            @Qualifier(VerifyExecutorConfig.OPERATOR) JobOperator verifyJobOperator,
            JobRepository jobRepository, RunningJobProbe runningJobs, JdbcClient jdbcClient) {
        this.verifyJobOperator = verifyJobOperator;
        this.jobRepository = jobRepository;
        this.runningJobs = runningJobs;
        this.jdbcClient = jdbcClient;
    }

    /** 중단 신호를 보냈는가. {@code signalled} 는 {@code JobOperator.stop} 의 반환이다. */
    public record Outcome(long executionId, boolean signalled) {
    }

    /**
     * <b>타임아웃을 건다.</b> 선점문이 행 X 락을 잡은 채 {@code stop} 의 메타 쓰기까지
     * 이어진다. 인증 없는 POST 라 같은 실행 번호로 요청이 몰리면 뒤엣것들이
     * {@code innodb_lock_wait_timeout}(기본 50초)까지 커넥션을 물고 기다리고, 그동안
     * {@code @Scheduled} 여덟이 풀에서 커넥션을 못 얻는다.
     * {@code ExpireRecoveryService.recover} 와 같은 근거다.
     */
    @Transactional(timeout = 10)
    public Outcome stop(long executionId, boolean force) {
        JobExecution execution = requireVerifyExecution(executionId);

        // **끝난 실행은 "시체가 아니다" 가 아니라 "이미 끝났다" 로 답한다.** 둘 다 409 지만
        // 사람이 할 일이 다르다 — 앞은 기다리는 것이고 뒤는 아무것도 안 하는 것이다.
        //
        // ⚠️ isRunning() 이 아니라 stop 이 실제로 받는 집합으로 가른다. BatchStatus.isRunning()
        //    은 STOPPING 에도 참인데 SimpleJobOperator.stop 은 그것을 거절한다 — isRunning()
        //    으로 가르면 STOPPING 행이 "N분 뒤부터 멈출 수 있습니다" 를 받고, 기다려도
        //    stop 은 영원히 안 먹는다. 정답은 abandon 이다.
        BatchStatus status = execution.getStatus();
        if (status != BatchStatus.STARTED && status != BatchStatus.STARTING) {
            throw new BusinessException(VerificationErrorCode.VERIFY_NOT_RUNNING,
                    "지금=" + status + " executionId=" + executionId);
        }

        // **force 는 진도 조건만 뺀다.** 상태 조건은 남긴다 — 그 사이에 스스로 끝난 실행에
        // 쓰면 종료 이력을 덮어쓴다.
        int claimed;
        if (force) {
            claimed = jdbcClient.sql(FORCE_CLAIM).param("id", executionId).update();
            if (claimed > 0) {
                // **선점에 성공한 뒤에 남긴다.** 앞에 두면 그 사이 스스로 끝나 거절된
                // 요청까지 "강제 중단했다" 로 기록되고, 감사 때 일어나지 않은 정합성
                // 위험을 일어난 것으로 읽는다.
                log.warn("도는 검증을 강제로 중단합니다. 이 시점부터 만료·정리가 이 실행의 "
                        + "입력을 건드릴 수 있고, 여기까지 쓴 asof_state 는 판정에 "
                        + "못 씁니다. executionId={} 지금={}", executionId, status);
            }
        } else {
            LocalDateTime stuckBefore = requireStuck(execution);
            claimed = jdbcClient.sql(CLAIM)
                    .param("id", executionId)
                    .param("stuckBefore", stuckBefore)
                    .update();
        }

        if (claimed == 0) {
            // **현재 읽기로 본다.** 같은 트랜잭션의 평범한 SELECT 는 RR 스냅샷이라 그 사이의
            // 커밋을 못 본다 — 그러면 자기모순 응답이 나가고 운영자는 손 SQL 로 돌아간다.
            String current = jdbcClient.sql(CURRENT_STATUS)
                    .param("id", executionId).query(String.class).single();
            BatchStatus reread = BatchStatus.valueOf(current);
            // ⚠️ **앞 검사와 같은 집합으로 가른다.** isRunning() 으로 가르면 STOPPING 이
            //    여기서는 019("기다리면 된다")를 받는데 앞 검사에서는 015("이미 끝났다")를
            //    받는다 — 경쟁이 어디서 끝나느냐에 따라 같은 상태의 처방이 갈린다.
            if (reread == BatchStatus.STARTED || reread == BatchStatus.STARTING) {
                // 0행인데 아직 도는 중 = 그 사이 진도를 냈다. 살아난 실행을 멈추면 안 된다.
                // force 로는 여기 못 온다 — 진도 조건이 없어 상태만 맞으면 늘 1행이다.
                throw new BusinessException(
                        VerificationErrorCode.VERIFY_EXECUTION_NOT_STUCK,
                        "선점에 실패했고 실행이 아직 돌고 있습니다. 지금=" + current
                                + " executionId=" + executionId);
            }
            // 그 사이 스스로 끝났거나 STOPPING 이다. 어느 쪽이든 stop 은 안 먹는다 —
            // 앞 검사가 같은 상태에 내는 답과 같아야 한다.
            throw new BusinessException(VerificationErrorCode.VERIFY_NOT_RUNNING,
                    "그 사이 실행이 끝났습니다. 지금=" + current
                            + " executionId=" + executionId);
        }

        try {
            // 선점으로 오른 VERSION 을 반영한 객체로 써야 한다 — 안 그러면 JobRepository 의
            // 낙관적 락(WHERE VERSION = ?)이 우리 자신의 선점에 진다.
            boolean signalled = verifyJobOperator.stop(requireVerifyExecution(executionId));
            return new Outcome(executionId, signalled);
        } catch (JobExecutionNotRunningException e) {
            // 선점을 통과했는데 여기 오면 STATUS 집합이 갈린 것이다. @Transactional 이라
            // 선점의 VERSION 증가까지 함께 롤백된다.
            throw new BusinessException(VerificationErrorCode.VERIFY_NOT_RUNNING,
                    "executionId=" + executionId, e);
        }
    }

    /**
     * <b>시체가 아니면 거절하고, 언제부터 가능한지 로그에 남긴다.</b> 응답 문구는 카탈로그
     * 것만 나간다 — 이 API 에는 인증이 없다({@code BatchApiExceptionHandler}).
     *
     * @return 판정에 쓴 절대 시각. 선점문이 같은 값을 다시 건다.
     */
    private LocalDateTime requireStuck(JobExecution execution) {
        for (StuckRun candidate : runningJobs.stuckExecutions(VerifyJobConfig.JOB_NAME)) {
            if (candidate.execution().getId() == execution.getId()) {
                return candidate.stuckBefore();
            }
        }
        throw new BusinessException(VerificationErrorCode.VERIFY_EXECUTION_NOT_STUCK,
                "지금=" + execution.getStatus() + " " + remainingUntilStuck(execution)
                        + " executionId=" + execution.getId());
    }

    /**
     * <b>언제부터 멈출 수 있는지.</b> 뺄셈은 {@link RunningJobProbe#untilStuck} 이 자기
     * 좌표계에서 한다 — 배치 메타의 시각은 JVM 기본 존으로 찍히므로 여기서 UTC 시계로
     * 빼면 KST JVM 에서 아홉 시간 반이 남았다고 답한다.
     */
    private String remainingUntilStuck(JobExecution execution) {
        Duration left = runningJobs.untilStuck(execution);
        if (left.isNegative() || left.isZero()) {
            return "곧 시체로 판정됩니다. 잠시 뒤 다시 부르십시오.";
        }
        return "약 " + left.toMinutes() + "분 뒤부터 멈출 수 있습니다.";
    }

    /**
     * <b>없는 실행과 남의 잡을 같은 404 로 접는다.</b> {@code getJobExecution} 은 없는 id 에
     * {@code null} 이 아니라 {@code EmptyResultDataAccessException} 을 던진다.
     * {@code VerifyTriggerController} 의 같은 이름 메서드와 판정이 같아야 한다.
     */
    private JobExecution requireVerifyExecution(long executionId) {
        JobExecution execution;
        try {
            execution = jobRepository.getJobExecution(executionId);
        } catch (EmptyResultDataAccessException e) {
            throw new BusinessException(VerificationErrorCode.VERIFY_EXECUTION_NOT_FOUND,
                    "executionId=" + executionId, e);
        }
        if (execution == null
                || !VerifyJobConfig.JOB_NAME.equals(execution.getJobInstance().getJobName())) {
            throw new BusinessException(VerificationErrorCode.VERIFY_EXECUTION_NOT_FOUND,
                    "executionId=" + executionId);
        }
        return execution;
    }
}
