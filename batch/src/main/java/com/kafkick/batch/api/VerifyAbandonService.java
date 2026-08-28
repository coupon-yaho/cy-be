// 중단된 verifyJob 실행을 "없던 것으로" 닫습니다.
package com.kafkick.batch.api;

import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.batch.config.VerifyExecutorConfig;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * <b>{@code stop} 다음 단계를 진다.</b> {@link VerifyStopService} 와 나란히 서면
 * {@code stop → abandon} 2단계가 코드 구조로도 읽힌다.
 *
 * <p><b>왜 컨트롤러가 아닌가.</b> {@code ExpireRecoveryService} 의 첫 문단이 그 규칙을
 * 적어 뒀다 — <i>"복구는 배치 메타에 대한 다단 쓰기다. 컨트롤러에 두면 트랜잭션 경계를 걸
 * 자리가 없다."</i> 이 경로만 컨트롤러에 남아 형제 둘과 갈려 있었다(CY-697 이 옮겼다).
 */
@Service
public class VerifyAbandonService {

    private static final Logger log = LoggerFactory.getLogger(VerifyAbandonService.class);

    /**
     * <b>버릴 수 있는 상태.</b> {@code STOPPING} 은 신호를 받고 아직 안 멈춘 것,
     * {@code STOPPED} 는 멈춘 것이다 — 하드킬로 잡이 이미 없으면 {@code stop} 한 번에
     * 곧바로 뒤쪽이 된다. {@code FAILED}·{@code COMPLETED} 는 <b>버릴 것이 없고</b>,
     * 덮어쓰면 실행 이력이라는 판정 근거를 바꾸는 일이 된다.
     */
    private static final Set<BatchStatus> ABANDONABLE =
            EnumSet.of(BatchStatus.STOPPING, BatchStatus.STOPPED);

    private final JobOperator verifyJobOperator;
    private final JobRepository jobRepository;

    public VerifyAbandonService(
            @Qualifier(VerifyExecutorConfig.OPERATOR) JobOperator verifyJobOperator,
            JobRepository jobRepository) {
        this.verifyJobOperator = verifyJobOperator;
        this.jobRepository = jobRepository;
    }

    /**
     * <b>데드라인을 건다.</b> 형제 {@link VerifyStopService#stop} ·
     * {@code ExpireRecoveryService#recover} 와 같은 근거다 — 인증 없는 POST 라 같은 번호로
     * 요청이 몰리면 커넥션을 물고 기다린다.
     */
    @Transactional(timeoutString = "${batch.admin.recover-timeout-seconds:10}")
    public Abandoned abandon(long executionId) {

        JobExecution execution = requireVerifyExecution(executionId);

        // **중단된 것만 버린다.** Spring Batch 는 status.isLessThan(STOPPING) 일 때만
        // 거부한다. BatchStatus 순서가 COMPLETED(0)·STARTING(1)·STARTED(2)·STOPPING(3)·
        // STOPPED(4)·FAILED(5)·ABANDONED(6) 이라, 통과하는 것은 **STOPPING·STOPPED·FAILED·
        // ABANDONED 넷**이고 우리는 뒤의 둘을 막는다 — COMPLETED 는 프레임워크가 이미 막는다(6.0.4 에서 직접 찍었다. 한때 여기
        // 주석이 COMPLETED 도 통과한다고 적었는데 거짓이었고, CY-429 가 같은 문장을
        // 복제한 뒤 돌연변이 테스트가 잡았다). 그대로 두면 실패 이력을 ABANDONED 로
        // 덮어쓰고 END_TIME 을 현재로 다시 쓴다. 이 저장소는
        // 실행 이력을 판정 근거로 삼으므로(docs/11) 그것은 증거를 조용히 바꾸는 일이다.
        //
        // STOPPING 과 STOPPED 를 둘 다 받는다. stop 은 도는 잡이 있으면 STOPPING 을
        // 남기고 청크 경계에서 STOPPED 로 가지만, **하드킬로 잡이 이미 없으면 곧바로
        // STOPPED** 가 된다(실측). 후자가 이 엔드포인트의 주 사용처다.
        //
        // 끝난 실행에는 버릴 것도 없다 — 그것은 트리거를 막지 않는다.
        if (!ABANDONABLE.contains(execution.getStatus())) {
            throw new BusinessException(VerificationErrorCode.VERIFY_NOT_ABANDONABLE,
                    "중단된 실행만 버릴 수 있습니다. 지금=" + execution.getStatus()
                            + ". 도는 실행이면 먼저 stop 을 부르고, 이미 끝난 실행이면 "
                            + "버릴 것이 없습니다(트리거를 막지 않습니다). executionId=" + executionId);
        }
        try {
            verifyJobOperator.abandon(execution);
            log.warn("검증 실행을 버렸습니다. 하드킬로 남은 행을 걷어내는 복구 절차입니다. "
                    + "executionId={}", executionId);
            // stop 과 달리 **완료 동작**이라 200 이다. 202 + StopRequested 를 재사용하면
            // "신호만 보냈다" 는 그쪽 뜻이 여기서는 거짓이 된다.
            return new Abandoned(executionId, BatchStatus.ABANDONED.name());
        } catch (org.springframework.batch.core.launch.JobExecutionAlreadyRunningException e) {
            // 위 검사와 이 호출 사이에 상태가 바뀐 경우다. 같은 답을 준다.
            throw new BusinessException(VerificationErrorCode.VERIFY_NOT_ABANDONABLE,
                    "먼저 stop 으로 중단 신호를 보내십시오. executionId=" + executionId, e);
        }
    }

    /**
     * <b>없는 실행과 남의 잡을 같은 404 로 접는다.</b> {@code VerifyTriggerController} 의
     * 같은 이름 메서드와 판정이 같아야 한다.
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
