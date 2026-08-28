// 진도가 멈춘 cleanupJob 실행을 걷어냅니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.batch.config.BatchJobRepositoryConfig;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.config.RunningJobProbe.StuckRun;
import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * <b>{@code BatchStuckExecution} 은 세 잡에 다 뜨는데 복구 경로는 둘뿐이었다.</b>
 * {@code expireJob} 은 {@link ExpireRecoveryService}, {@code verifyJob} 은
 * {@link VerifyStopService} 가 지는데 {@code cleanupJob} 만 <b>손 SQL 이 유일한 길</b>이었다.
 * 알림이 그 사실을 명시했고 {@code docs/13} 이 항목으로 들고 있었다.
 *
 * <p><b>왜 만료식 한 방인가 — 검증식 {@code stop → abandon} 2단계가 아니라.</b> 넷을 재 봤다:
 * <ul>
 *   <li><b>지킬 살아 있는 입력이 없다.</b> 검증이 2단계인 것은 도는 실행을 섣불리 닫으면
 *       {@code asof_state} 를 읽는 판정이 반쯤 쓰인 상태를 보기 때문인데, 정리는 그런
 *       입력을 안 만든다
 *   <li><b>업무 데이터를 안 쓴다.</b> 지우는 것은 검증 파생 행과 배치 메타뿐이다
 *       ({@code CleanupJdbcAdapter})
 *   <li><b>막고 있는 것도 없다.</b> {@code blockingExecutions(cleanupJob)} 호출자가
 *       <b>0</b> 이라 아무도 정리에 물러나지 않고, 스케줄러가 슬롯마다 새 {@code firedAt} 을
 *       실어 <b>새 인스턴스</b>를 만들므로 다음 실행도 안 막힌다
 *   <li><b>시체 오판 위험이 구조적으로 낮다.</b> {@code batch.cleanup.step-timeout-ms}
 *       (120초)가 {@code batch.stuck-job-after-ms}(30분)보다 훨씬 짧아 <b>락 대기로 30분을
 *       침묵할 수 없다</b> — 그 전에 Step 데드라인이 잡을 죽인다. {@code verifyJob} 이
 *       2단계인 이유(느리게라도 커밋하면 영원히 시체가 안 된다)가 여기엔 없다
 * </ul>
 * 그래서 남는 피해는 <b>영원히 우는 알림</b>과 <b>안 닫히는 행</b>이고, 그 둘은 한 방으로 끝난다.
 *
 * <p><b>이것은 정상 절차가 아니라 복구다.</b> 걷어낸 실행이 지운 것은 안 되돌린다 — 정리는
 * 청크마다 커밋하고 <b>이미 지운 것은 그대로 정당하다</b>(지울 대상이었으니 지웠다).
 * 남은 것은 다음 슬롯이 다시 고른다. 진도는 실행 문맥에 있었으므로 함께 사라지지만,
 * 대상 목록을 얼려 두지 않는 설계라 다시 고르는 데 문제가 없다.
 */
@Service
public class CleanupRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(CleanupRecoveryService.class);

    private final JobRepository jobRepository;
    private final JobOperator jobOperator;
    private final RunningJobProbe runningJobs;
    private final JdbcClient jdbcClient;

    public CleanupRecoveryService(JobRepository jobRepository,
            @Qualifier(BatchJobRepositoryConfig.SHARED_OPERATOR) JobOperator jobOperator,
            RunningJobProbe runningJobs, JdbcClient jdbcClient) {
        this.jobRepository = jobRepository;
        this.jobOperator = jobOperator;
        this.runningJobs = runningJobs;
        this.jdbcClient = jdbcClient;
    }

    /** 걷어낸 결과. {@code alreadyRecovered} 면 이 호출은 아무것도 안 썼다. */
    public record Outcome(String status, boolean alreadyRecovered) {
    }

    /**
     * <b>타임아웃을 건다.</b> 선점문이 행 X 락을 잡은 채 {@code recover} 의 메타 쓰기까지
     * 이어진다. 인증 없는 POST 라 같은 번호로 요청이 몰리면 뒤엣것들이
     * {@code innodb_lock_wait_timeout} 까지 커넥션을 물고 기다린다.
     * {@link ExpireRecoveryService#recover} 와 같은 근거다.
     */
    @Transactional(timeoutString = "${batch.admin.recover-timeout-seconds:10}")
    public Outcome recover(long executionId) {
        JobExecution execution = requireCleanupExecution(executionId);

        // **"걷어낸 뒤" 와 "그냥 끝난 것" 을 가른다.** recover 가 만드는 것은 언제나 FAILED
        // 이므로 그것만 재시도로 본다. COMPLETED 를 여기 넣으면 번호를 한 자리 잘못 친
        // 운영자가 어제 정상 완료한 실행에 200 "이미 처리됨" 을 받고 창을 닫는다.
        if (execution.getStatus() == BatchStatus.FAILED && execution.getEndTime() != null) {
            log.info("이미 걷어낸 정리 실행입니다. 아무것도 안 했습니다. executionId={}", executionId);
            return new Outcome(execution.getStatus().name(), true);
        }
        if (!execution.getStatus().isRunning() && execution.getEndTime() != null) {
            throw new BusinessException(VerificationErrorCode.CLEANUP_EXECUTION_NOT_STUCK,
                    "이미 끝난 실행입니다. 지금=" + execution.getStatus()
                            + " executionId=" + executionId);
        }

        LocalDateTime stuckBefore = requireStuck(execution);

        if (jdbcClient.sql(StuckRunClaim.CLAIM)
                .param("id", executionId)
                .param("stuckBefore", stuckBefore)
                .update() == 0) {
            // **현재 읽기로 본다.** 같은 트랜잭션의 평범한 SELECT 는 RR 스냅샷이라 앞 요청이
            // 커밋한 FAILED 를 못 본다 — 그러면 자기모순 응답이 나간다.
            String status = jdbcClient.sql(StuckRunClaim.CURRENT_STATUS)
                    .param("id", executionId).query(String.class).single();
            if (BatchStatus.valueOf(status).isRunning()) {
                throw new BusinessException(VerificationErrorCode.CLEANUP_EXECUTION_NOT_STUCK,
                        "선점에 실패했고 실행이 아직 돌고 있습니다. 지금=" + status
                                + " executionId=" + executionId);
            }
            if (BatchStatus.valueOf(status) != BatchStatus.FAILED) {
                throw new BusinessException(VerificationErrorCode.CLEANUP_EXECUTION_NOT_STUCK,
                        "그 사이 실행이 끝났습니다. 지금=" + status
                                + " executionId=" + executionId);
            }
            log.info("다른 요청이 먼저 걷어냈습니다. executionId={} status={}", executionId, status);
            return new Outcome(status, true);
        }

        // 선점으로 오른 VERSION 을 반영한 객체로 써야 한다.
        JobExecution recovered = jobOperator.recover(requireCleanupExecution(executionId));
        if (recovered.getStatus().isRunning() || recovered.getEndTime() == null) {
            // recover 가 아무것도 안 하고 돌아온 갈래다. 200 을 내면 이 행이 영원히 안 걷히고
            // BatchStuckExecution 이 계속 운다. @Transactional 이라 선점도 함께 롤백된다.
            throw new IllegalStateException("recover 가 실행을 닫지 못했습니다. status="
                    + recovered.getStatus() + " executionId=" + executionId);
        }
        log.warn("정리 실행을 걷어냈습니다. 진도가 멈춘 실행을 FAILED 로 닫는 복구 절차입니다. "
                + "남은 대상은 다음 슬롯이 다시 고릅니다. executionId={}", executionId);
        return new Outcome(recovered.getStatus().name(), false);
    }

    /**
     * <b>시체로 판정된 실행에만 쓰기를 한다.</b> 판정에 쓴 <b>절대 시각</b>을 돌려준다 —
     * 선점문이 같은 조건을 SQL 로 다시 걸어 판정과 쓰기를 원자적으로 만든다.
     */
    private LocalDateTime requireStuck(JobExecution execution) {
        return runningJobs.stuckExecutions(CleanupJobConfig.JOB_NAME).stream()
                .filter(candidate -> candidate.execution().getId() == execution.getId())
                .map(StuckRun::stuckBefore)
                .findFirst()
                .orElseThrow(() -> new BusinessException(
                        VerificationErrorCode.CLEANUP_EXECUTION_NOT_STUCK,
                        "지금=" + execution.getStatus() + ". /runs/stuck 이 비어 있으면 "
                                + "걷어낼 것이 없습니다. executionId=" + execution.getId()));
    }

    /**
     * <b>없는 실행과 남의 잡을 같은 404 로 접는다.</b> 남의 잡이라고 알려 주면 인증 없는
     * 이 API 가 배치 메타의 실행 번호 공간을 훑는 수단이 된다.
     */
    private JobExecution requireCleanupExecution(long executionId) {
        JobExecution execution;
        try {
            execution = jobRepository.getJobExecution(executionId);
        } catch (EmptyResultDataAccessException e) {
            throw new BusinessException(VerificationErrorCode.CLEANUP_EXECUTION_NOT_FOUND,
                    "executionId=" + executionId, e);
        }
        if (execution == null
                || !CleanupJobConfig.JOB_NAME.equals(execution.getJobInstance().getJobName())) {
            throw new BusinessException(VerificationErrorCode.CLEANUP_EXECUTION_NOT_FOUND,
                    "executionId=" + executionId);
        }
        return execution;
    }
}
