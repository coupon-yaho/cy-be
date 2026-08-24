// 진도가 멈춘 만료 실행을 FAILED 로 닫는 복구 절차입니다. 트랜잭션 경계가 여기 있습니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.batch.config.BatchJobRepositoryConfig;
import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.config.RunningJobProbe.StuckRun;
import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.core.support.exception.BusinessException;

/**
 * <b>복구는 배치 메타에 대한 다단 쓰기다 — 컨트롤러에 두면 트랜잭션 경계를 걸 자리가 없다.</b>
 *
 * <p><b>{@code api} 에 둔다.</b> 한때 {@code config} 에 뒀는데 README 의 패키지 표가
 * 그 자리를 <i>"기동 가드, 지표, 전용 실행기"</i> 로 적어 뒀다 — 이 저장소는 그 표를
 * 팀 규칙으로 쓰고 코드가 거기 맞춘다. 이것은 {@code ExpireAdminController} 전용
 * 오케스트레이션이고 {@code ExpireRunView}·{@code Recovered} 와 같은 계층이다.
 *
 * <p>{@code JobRepository} 프록시의 트랜잭션 속성은 {@code create*} 와
 * {@code getLastJobExecution*} 이 {@code REQUIRES_NEW} 이고 나머지({@code *})는
 * {@code PROPAGATION_REQUIRED} 다. 그래서 <b>이 메서드가 트랜잭션을 열면
 * {@code recover} 안의 Step·Job 쓰기가 전부 그 하나에 합류한다</b> — 중간에 끊겨도 통째로
 * 롤백되어 실행이 {@code STARTED} 로 남고, 재시도가 정상 동작한다.
 *
 * <h2>멱등을 상태에서 유도한다 — 플래그를 안 남긴다</h2>
 *
 * <p>{@code recover} 는 실행 문맥에 {@code batch.recovered} 를 넣는다. 한때 그것을
 * {@code updateExecutionContext} 로 영속시켜 멱등의 근거로 삼았는데, <b>독이 든 사탕이었다</b> —
 * {@code TaskExecutorJobLauncher} 가 다음 실행을 만들 때 <b>직전 실행의 문맥을 그대로
 * 복사한다</b>(6.0.4 바이트코드: {@code getLastJobExecution} → {@code getExecutionContext} →
 * {@code createJobExecution(instance, params, 그 문맥)}). 즉 한 번 걷어낸 뒤 같은
 * {@code asOf} 로 다시 돌리면 <b>새 실행이 태어날 때부터 그 플래그를 들고 있고</b>, 그것이
 * 다시 시체가 되면 복구가 아무것도 안 한 채 200 을 낸다 — 영원히 못 걷는다.
 *
 * <p>{@code ExpireStepContext} 가 Step 문맥에 <b>세대를 함께 싣는</b> 이유가 정확히 이것이고,
 * 이 저장소는 그 사고를 이미 한 번 겪었다. 그래서 <b>플래그를 아예 안 쓴다</b> —
 * {@code FAILED}({@code isRunning()} 이 거짓) + {@code END_TIME} 이 있으면 이미 닫힌 것이다.
 * 그 둘은 실행마다 새로 생기므로 상속되지 않는다.
 *
 * <h2>동시 요청은 조건부 갱신이 가른다 — 낙관적 락이 아니다</h2>
 *
 * <p><b>{@code update(JobExecution)} 에는 낙관적 락이 사실상 없다.</b> 쓰기 직전에
 * {@code synchronizeStatus} 가 자기 {@code VERSION} 을 DB 값으로 <b>다시 맞추므로</b>
 * {@code WHERE ... AND VERSION = ?} 가 항상 통과한다(6.0.4 바이트코드). 버전 검사가 걸리는
 * 것은 {@code update(StepExecution)} 뿐이라, <b>Step 행이 하나도 없는 실행</b>
 * ({@code STARTING} 에서 죽은 모양)에는 검사가 한 개도 없다 — 동시 요청 둘이 모두 성공해
 * {@code END_TIME} 을 두 번 쓴다.
 *
 * <p>그래서 <b>조건부 갱신으로 선점한다.</b> 그 한 문장이 행 X 락을 잡아 같은 트랜잭션이
 * 끝날 때까지 뒤 요청을 세우고, 뒤 요청은 그때 {@code STATUS} 가 이미 바뀐 것을 보고
 * 0행을 받는다. {@code affected rows} 검사가 이 API 의 유일한 상호배제다 —
 * 만료 잡 자신이 {@code releaseStock} 에서 쓰는 것과 같은 방식이다.
 */
@Service
public class ExpireRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(ExpireRecoveryService.class);

    /**
     * <b>선점 문장. 이 API 의 유일한 상호배제다.</b>
     *
     * <p>{@code STATUS} 집합은 {@code findRunningJobExecutions} 가 보는 것과 <b>같아야
     * 한다</b>({@code JdbcJobExecutionDao.GET_RUNNING_EXECUTION_FOR_INSTANCE} 의 상수와
     * 글자까지 같다). 하나라도 빠지면 그 상태의 시체가 <b>아무것도 안 한 채 200</b> 을
     * 받는다 — {@code STARTING} 에서 죽은 행이 이 API 의 주 대상이라 특히 그렇다.
     *
     * <p><b>폴백까지 그대로 옮긴다.</b> {@code RunningJobProbe.lastProgress} 가
     * {@code MAX(LAST_UPDATED)} → {@code START_TIME} → {@code CREATE_TIME} 순으로
     * 떨어지는데, {@code NOT EXISTS} 로만 쓰면 <b>Step 행이 없는 실행에서 무조건 참</b>이
     * 되어 방금 뜬 {@code STARTING} 실행도 닫는다. 그 모양이 이 API 의 주 대상이라 특히
     * 위험하다.
     *
     * <p><b>진도 조건을 여기 함께 건다.</b> 판정({@code requireStuck})과 쓰기가 따로면
     * 그 사이에 실행이 되살아날 수 있고 — 락 대기가 풀리는 경우가 그렇다 — 그때
     * <b>살아 있는 만료를 {@code FAILED} 로 닫는다.</b> 만료는 재고를 쓰는 유일한 잡이라
     * 중간에 끊기면 다음 검증의 판정 근거가 흔들린다. 임계는 여전히
     * {@link RunningJobProbe} 가 소유하고 여기는 절대 시각만 받는다.
     */
    // 같은 패키지의 테스트가 이 문장의 조건을 직접 잰다. 흐름 테스트로는 판정과 쓰기
    // 사이의 창을 재현할 수 없어서(마이크로초), 문장 자체를 못 박는다.
    static final String CLAIM = """
            UPDATE BATCH_JOB_EXECUTION je
               SET je.VERSION = je.VERSION + 1
             WHERE je.JOB_EXECUTION_ID = :id
               AND je.STATUS IN ('STARTING','STARTED','STOPPING')
               AND COALESCE((SELECT MAX(se.LAST_UPDATED) FROM BATCH_STEP_EXECUTION se
                              WHERE se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID),
                            je.START_TIME, je.CREATE_TIME) <= :stuckBefore
            """;

    /** 선점에 진 뒤 <b>현재 읽기</b>로 상태를 본다. 스냅샷 읽기는 옛 값을 준다(RR). */
    private static final String CURRENT_STATUS = """
            SELECT STATUS FROM BATCH_JOB_EXECUTION
             WHERE JOB_EXECUTION_ID = :id FOR SHARE
            """;

    private final JobRepository jobRepository;
    private final JobOperator jobOperator;
    private final RunningJobProbe runningJobs;
    private final JdbcClient jdbcClient;

    public ExpireRecoveryService(JobRepository jobRepository,
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
     * <b>타임아웃을 건다.</b> 이 트랜잭션은 선점문이 행 X 락을 잡은 채 {@code recover} 의
     * Step·Job 쓰기까지 이어진다 — <b>이번 티켓이 만든 락 보유 구간</b>이다. 인증 없는
     * POST 라 같은 실행 번호로 요청이 몰리면 뒤엣것들이
     * {@code innodb_lock_wait_timeout}(기본 50초)까지 커넥션을 물고 기다리고, 그동안
     * {@code @Scheduled} 여덟이 풀(13)에서 커넥션을 못 얻는다. <b>DB 가 아플 때 불리는
     * 진단 도구가 정확히 그때 배치를 멈추는 모양</b>이라 여기만 먼저 막는다.
     */
    @Transactional(timeout = 10)
    public Outcome recover(long executionId) {
        JobExecution execution = requireExpireExecution(executionId);

        // **"걷어낸 뒤" 와 "그냥 끝난 것" 을 가른다.** recover 가 만드는 것은 언제나
        // FAILED 이므로 그것만 재시도로 본다. COMPLETED 를 여기 넣으면 실행 번호를 한 자리
        // 잘못 친 운영자가 어제 정상 완료한 실행에 200 "이미 처리됨" 을 받고 창을 닫는다 —
        // 진짜 시체는 그대로 남고, 그동안 만료↔검증 상호 배제가 꺼져 있다.
        if (execution.getStatus() == BatchStatus.FAILED && execution.getEndTime() != null) {
            log.info("이미 걷어낸 만료 실행입니다. 아무것도 안 했습니다. executionId={}",
                    executionId);
            return new Outcome(execution.getStatus().name(), true);
        }
        if (!execution.getStatus().isRunning() && execution.getEndTime() != null) {
            throw new BusinessException(ExpirationErrorCode.EXPIRE_EXECUTION_NOT_STUCK,
                    "이미 끝난 실행입니다. 지금=" + execution.getStatus()
                            + " executionId=" + executionId);
        }

        LocalDateTime stuckBefore = requireStuck(execution);

        if (jdbcClient.sql(CLAIM)
                .param("id", executionId)
                .param("stuckBefore", stuckBefore)
                .update() == 0) {
            // **현재 읽기로 본다.** 같은 트랜잭션의 평범한 SELECT 는 RR 스냅샷이라 앞 요청이
            // 커밋한 FAILED 를 못 본다 — 그러면 {status:STARTED, alreadyRecovered:true} 라는
            // 자기모순 응답이 나가고, 운영자는 손 SQL 로 되돌아간다.
            String status = jdbcClient.sql(CURRENT_STATUS)
                    .param("id", executionId).query(String.class).single();
            if (BatchStatus.valueOf(status).isRunning()) {
                // 0행인데 아직 도는 중 = 그 사이 진도를 냈거나 STATUS 집합이 갈렸다.
                // 어느 쪽이든 쓰면 안 되고, 조용한 성공으로 접어도 안 된다.
                throw new BusinessException(ExpirationErrorCode.EXPIRE_EXECUTION_NOT_STUCK,
                        "선점에 실패했고 실행이 아직 돌고 있습니다. 지금=" + status
                                + " executionId=" + executionId);
            }
            if (BatchStatus.valueOf(status) != BatchStatus.FAILED) {
                // 그 사이 정상 종료했다. 아무도 걷어내지 않았으므로 성공이 아니다 —
                // 선점 전 경로가 같은 상태에 409 를 내는 것과 답이 같아야 한다.
                //
                // **이 갈래는 테스트가 못 잡는다.** 선점 전 검사가 종단 상태를 먼저
                // 막으므로, 여기 오려면 그 읽기와 선점 사이에 잡이 스스로 끝나야 한다 —
                // 밖에서 결정적으로 만들 수 없는 창이다. 돌연변이도 살아남는 것을
                // 확인했다. 그래도 두는 이유는 답이 갈리면 안 되기 때문이다.
                throw new BusinessException(ExpirationErrorCode.EXPIRE_EXECUTION_NOT_STUCK,
                        "그 사이 실행이 끝났습니다. 지금=" + status
                                + " executionId=" + executionId);
            }
            log.info("다른 요청이 먼저 걷어냈습니다. executionId={} status={}",
                    executionId, status);
            return new Outcome(status, true);
        }

        // 선점으로 오른 VERSION 을 반영한 객체로 써야 한다.
        JobExecution recovered = jobOperator.recover(requireExpireExecution(executionId));
        if (recovered.getStatus().isRunning() || recovered.getEndTime() == null) {
            // recover 가 아무것도 안 하고 돌아온 갈래다(문맥의 batch.recovered 또는 종단 상태).
            // 200 을 내면 이 행이 영원히 안 걷히고 그동안 상호 배제가 꺼져 있다.
            // @Transactional 이라 선점의 VERSION 증가까지 함께 롤백된다.
            throw new IllegalStateException("recover 가 실행을 닫지 못했습니다. status="
                    + recovered.getStatus() + " executionId=" + executionId);
        }
        log.warn("만료 실행을 걷어냈습니다. 진도가 멈춘 실행을 FAILED 로 닫는 복구 절차입니다. "
                + "남은 만료 대상은 다음 배치 창이 가져갑니다. executionId={}", executionId);
        return new Outcome(recovered.getStatus().name(), false);
    }

    /**
     * <b>시체로 판정된 실행에만 쓰기를 한다.</b> 손 SQL 이 임계를 직접 적어야 했던 자리이고,
     * 그 숫자가 코드와 갈리는 순간 운영자가 살아 있는 만료를 걷어낸다.
     *
     * <p>판정에 쓴 <b>절대 시각</b>을 돌려준다 — 선점문이 같은 조건을 SQL 로 다시 걸어
     * 판정과 쓰기를 원자적으로 만든다.
     */
    private LocalDateTime requireStuck(JobExecution execution) {
        return runningJobs.stuckExecutions(ExpireStepContext.JOB_NAME).stream()
                .filter(candidate -> candidate.execution().getId() == execution.getId())
                .map(StuckRun::stuckBefore)
                .findFirst()
                .orElseThrow(() -> new BusinessException(
                        ExpirationErrorCode.EXPIRE_EXECUTION_NOT_STUCK,
                        "지금=" + execution.getStatus() + ". /runs/stuck 이 비어 있으면 "
                                + "걷어낼 것이 없습니다. executionId=" + execution.getId()));
    }

    /**
     * <b>없는 실행과 남의 잡을 같은 404 로 접는다.</b> 남의 잡이라고 알려 주면 인증 없는
     * 이 API 가 배치 메타의 실행 번호 공간을 훑는 수단이 된다.
     *
     * <p>{@code getJobExecution} 은 없는 id 에 {@code null} 이 아니라
     * {@code EmptyResultDataAccessException} 을 던진다({@code getJobInstanceId} 의
     * {@code queryForObject} 가 DAO 의 try 블록 밖이다).
     */
    private JobExecution requireExpireExecution(long executionId) {
        JobExecution execution;
        try {
            execution = jobRepository.getJobExecution(executionId);
        } catch (EmptyResultDataAccessException e) {
            throw new BusinessException(ExpirationErrorCode.EXPIRE_EXECUTION_NOT_FOUND,
                    "executionId=" + executionId, e);
        }
        if (execution == null
                || !ExpireStepContext.JOB_NAME.equals(execution.getJobInstance().getJobName())) {
            throw new BusinessException(ExpirationErrorCode.EXPIRE_EXECUTION_NOT_FOUND,
                    "executionId=" + executionId);
        }
        return execution;
    }
}
