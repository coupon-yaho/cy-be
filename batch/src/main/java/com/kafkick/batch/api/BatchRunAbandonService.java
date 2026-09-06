// 범용 관제의 시체 회수. 잡 이름을 모른 채 실행 id 하나로 돈다.
package com.kafkick.batch.api;

import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.batch.config.BatchJobRepositoryConfig;
import com.kafkick.core.batch.exception.BatchControlErrorCode;
import com.kafkick.core.support.exception.BusinessException;

/**
 * <b>죽은 실행을 버린다 — 잡 이름을 모른 채로.</b>
 *
 * <p>{@code expireJob}·{@code verifyJob}·{@code cleanupJob} 은 각자 회수 API 가 있다.
 * 그런데 <b>범용 관제가 범용인 이유</b>는 Spring Batch 메타데이터만 읽어 잡 이름을 모른다는
 * 것인데, 조치만 잡별 API 에 기대면 <b>주제가 바뀌는 순간 걷을 방법이 사라진다.</b>
 * 새 도메인의 잡에는 그 API 가 없다.
 *
 * <h2>{@link VerifyAbandonService} 를 그대로 따른다</h2>
 *
 * <p>⚠️ 첫 판은 이 프로토콜을 <b>손으로 다시 짰다</b> — 한 요청 안에서 {@code stop} 과
 * {@code abandon} 을 이어 부르고, 선점 없이 자바 쪽 검사만 했다. 리뷰가 세 갈래로 짚었고
 * <b>셋 다 같은 뿌리</b>였다: 이 저장소가 CY-429·CY-678·CY-697 에 걸쳐 이미 푼 것을
 * 안 쓰고 다시 만든 것.
 *
 * <ul>
 *   <li><b>{@code stop} 을 여기서 안 부른다.</b> 이어 부르면 {@code stop} 만 성공한 채
 *       {@code abandon} 이 실패하는 중간 상태가 생기고, 그 상태({@code STOPPED})가 다음
 *       요청의 첫 검사에서 거절되어 <b>범용 경로로는 끝낼 수 없게</b> 된다.</li>
 *   <li><b>{@code STOPPING} 도 받는다.</b> 기존 {@code stop} 으로 신호만 남기고 프로세스가
 *       죽은 실행이 그 상태로 남는다 — 못 받으면 그 시체가 영원히 남는다.</li>
 *   <li><b>선점 UPDATE 로 검사와 쓰기 사이를 닫는다.</b> {@code update(JobExecution)} 에는
 *       낙관적 락이 사실상 없어, 없으면 동시 요청 둘이 모두 검사를 통과해
 *       {@code END_TIME} 을 두 번 쓴다.</li>
 * </ul>
 */
@Service
public class BatchRunAbandonService {

    private static final Logger log = LoggerFactory.getLogger(BatchRunAbandonService.class);

    /**
     * <b>중단된 것만 버린다.</b>
     *
     * <p>Spring Batch 는 {@code status.isLessThan(STOPPING)} 일 때만 거부한다 —
     * 순서가 {@code COMPLETED(0)·STARTING(1)·STARTED(2)·STOPPING(3)·STOPPED(4)·FAILED(5)·
     * ABANDONED(6)} 이라 <b>{@code FAILED}·{@code ABANDONED} 가 프레임워크를 통과한다.</b>
     * 그대로 두면 실패 이력을 {@code ABANDONED} 로 덮고 {@code END_TIME} 을 현재로 다시
     * 쓴다 — 이 저장소는 실행 이력을 판정 근거로 삼는다(docs/11).
     */
    private static final Set<BatchStatus> ABANDONABLE =
            EnumSet.of(BatchStatus.STOPPING, BatchStatus.STOPPED);

    private final JobOperator jobOperator;
    private final JobRepository jobRepository;
    private final JdbcClient jdbcClient;

    public BatchRunAbandonService(
            @Qualifier(BatchJobRepositoryConfig.SHARED_OPERATOR) JobOperator jobOperator,
            JobRepository jobRepository,
            JdbcClient jdbcClient) {
        this.jobOperator = jobOperator;
        this.jobRepository = jobRepository;
        this.jdbcClient = jdbcClient;
    }

    /**
     * @param executionId 버릴 실행
     * @return 버린 실행과 그 상태
     * @throws BusinessException 없으면 {@code BATCH-001}, 중단된 상태가 아니거나 선점에
     *         지면 {@code BATCH-005}
     */
    @Transactional(timeoutString = "${batch.admin.recover-timeout-seconds:10}")
    public Abandoned abandon(long executionId) {
        JobExecution execution = require(executionId);

        if (!ABANDONABLE.contains(execution.getStatus())) {
            throw new BusinessException(BatchControlErrorCode.NOT_ABANDONABLE,
                    "지금=" + execution.getStatus() + ". 도는 실행이면 먼저 stop 을 부르고, "
                            + "이미 끝난 실행이면 버릴 것이 없습니다. executionId=" + executionId);
        }
        // **검사와 쓰기 사이를 닫는다.** 형제 넷(verify stop·verify abandon·expire recover·
        // cleanup recover)이 전부 이 모양이다.
        if (jdbcClient.sql(StuckRunClaim.ABANDON_CLAIM).param("id", executionId).update() == 0) {
            String current = jdbcClient.sql(StuckRunClaim.CURRENT_STATUS)
                    .param("id", executionId).query(String.class).single();
            throw new BusinessException(BatchControlErrorCode.NOT_ABANDONABLE,
                    "선점에 실패했습니다. 그 사이 상태가 바뀌었습니다. 지금=" + current
                            + " executionId=" + executionId);
        }
        try {
            // 선점으로 오른 VERSION 을 반영한 객체로 써야 한다.
            jobOperator.abandon(require(executionId));
            log.warn("배치 실행을 버렸습니다. 하드킬로 남은 행을 걷어내는 복구 절차입니다. "
                    + "executionId={}", executionId);
            return new Abandoned(executionId, BatchStatus.ABANDONED.name());
        } catch (JobExecutionAlreadyRunningException raced) {
            // 위 검사와 이 호출 사이에 상태가 바뀐 경우다. 같은 답을 준다.
            throw new BusinessException(BatchControlErrorCode.NOT_ABANDONABLE,
                    "먼저 stop 으로 중단 신호를 보내십시오. executionId=" + executionId);
        }
    }

    /** {@code JobRepository.getJobExecution} 은 없는 id 에 예외를 던진다(실측). */
    private JobExecution require(long executionId) {
        try {
            return jobRepository.getJobExecution(executionId);
        } catch (EmptyResultDataAccessException absent) {
            throw new BusinessException(BatchControlErrorCode.EXECUTION_NOT_FOUND,
                    "jobExecutionId=" + executionId);
        }
    }

    /**
     * @param jobExecutionId 버린 실행
     * @param status 버린 뒤 상태. 정상이면 {@code ABANDONED}
     */
    public record Abandoned(long jobExecutionId, String status) {
    }
}
