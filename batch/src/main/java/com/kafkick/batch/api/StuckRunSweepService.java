// 시체 한 건을 잡 이름 없이 걷어냅니다. 트랜잭션 경계가 여기 있습니다.
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.batch.config.BatchJobRepositoryConfig;

/**
 * <b>{@link ExpireRecoveryService} 를 잡 이름 없이 다시 쓴 것이다.</b> 그쪽은 만료 실행만
 * 받고 만료 전용 오류 코드를 내는데, 자동 스윕은 <b>어떤 잡이든</b> 걷어야 한다 — 새 도메인의
 * 잡에는 잡별 회수 API 가 없다({@link BatchRunAbandonService} 가 같은 이유로 범용이다).
 *
 * <h2>{@code abandon} 이 아니라 {@code recover} 다 — 자동으로 할 수 있는 것은 이쪽뿐이다</h2>
 *
 * <p>{@link BatchHistoryController#stuck()} 이 이미 경고해 뒀다: {@code ABANDONED} 는
 * {@code COMPLETED} 와 같은 취급이라 그 {@code JobInstance} 를 <b>같은 파라미터로 영원히 못
 * 돌린다.</b> 만료는 {@code asOf} 가 식별 파라미터라 <b>그 크론 슬롯이 통째로 사라진다</b> —
 * 사람이 눌러 놓고 아는 것과 스윕이 새벽에 조용히 하는 것은 무게가 다르다.
 *
 * <p>{@code recover} 는 {@code FAILED} 로 닫는다. 재시작이 되고 이력이 남는다.
 * 그래서 <b>자동화해도 되돌릴 수 있는 조치</b>다. {@code abandon} 은 사람이 부르는
 * 마지막 통로로 남는다.
 *
 * <h2>{@code SimpleJobOperator.recover} 실측(6.0.4 바이트코드)</h2>
 *
 * <ol>
 *   <li>실행 문맥에 {@code batch.recovered} 가 있으면 <b>WARN 만 남기고 그대로 돌려준다</b></li>
 *   <li>상태가 {@code COMPLETED}·{@code ABANDONED}·{@code UNKNOWN} 이면 역시 <b>아무것도
 *       안 한다</b></li>
 *   <li>그 밖이면 {@code isRunning()} 인 Step 을 {@code FAILED} + {@code END_TIME} 으로 닫고
 *       실행도 닫는다</li>
 * </ol>
 *
 * <p><b>2번 집합에 {@code FAILED}·{@code STOPPED} 가 없다</b> — 이미 끝난 실행에 부르면
 * {@code END_TIME} 을 <b>지금으로 다시 쓴다.</b> 여기서 그것을 막는 것은 자바 검사가 아니라
 * <b>입력</b>이다: 대상이 {@link com.kafkick.batch.config.RunningJobProbe#stuckExecutions}
 * 에서만 오고, 그 조회의 바탕인 {@code JdbcJobExecutionDao} 가
 * {@code E.STATUS IN ('STARTING','STARTED','STOPPING')} 으로 이미 자른다(같은 바이트코드에서
 * 확인). 끝난 실행은 애초에 목록에 없다.
 *
 * <h2>선점에 지는 것은 오류가 아니다</h2>
 *
 * <p>{@link ExpireRecoveryService} 는 선점에 지면 던진다 — 사람이 실행 번호를 대고 부른
 * 요청이라 <i>"당신이 시킨 것이 안 됐다"</i> 를 말해야 하기 때문이다. <b>스윕은 아무도
 * 시키지 않았다.</b> 그 사이 잡이 진도를 냈거나 운영자가 먼저 눌렀으면 그것이 정상이므로
 * {@code false} 만 돌려준다 — 던지면 그 한 건이 나머지 스윕을 끊는다.
 *
 * <h2>지표를 스케줄러가 아니라 여기서 든다</h2>
 *
 * <p>처음에는 {@code StuckRunSweeper} 가 셌다. 그러면 <b>스케줄러가 꺼진 형상에서 그
 * 미터가 아예 안 태어난다</b> — {@code increase(cy_batch_stuck_recovered_total[15m])} 가
 * <i>0</i> 이 아니라 <b>빈 결과</b>가 되어 {@code BatchStuckAutoRecovered} 가 영원히 안
 * 울리고, 관제 화면에서 <i>"안 걷혔다"</i> 와 <i>"계측이 없다"</i> 가 같은 모양이 된다.
 * {@code BatchMetricExposureTest} 가 그 상태를 잡았다.
 *
 * <p>이 빈은 {@code @Service} 라 조건이 없다. 조치를 하는 자리가 그 조치를 세는 것이
 * 맞기도 하다 — 손으로 부르는 경로가 나중에 생겨도 같은 수에 합류한다.
 */
@Service
public class StuckRunSweepService {

    private static final Logger log = LoggerFactory.getLogger(StuckRunSweepService.class);

    private final JobRepository jobRepository;
    private final JobOperator jobOperator;
    private final JdbcClient jdbcClient;
    private final Counter recovered;

    public StuckRunSweepService(JobRepository jobRepository,
            @Qualifier(BatchJobRepositoryConfig.SHARED_OPERATOR) JobOperator jobOperator,
            JdbcClient jdbcClient, MeterRegistry registry) {
        this.jobRepository = jobRepository;
        this.jobOperator = jobOperator;
        this.jdbcClient = jdbcClient;
        this.recovered = Counter.builder("cy_batch_stuck_recovered_total")
                .description("자동으로 걷어낸 시체 수. 자동 조치가 조용하면 사고를 덮는다")
                .register(registry);
    }

    /**
     * <b>시체 한 건을 {@code FAILED} 로 닫는다.</b>
     *
     * <p><b>건마다 트랜잭션이다.</b> 스윕 전체를 한 트랜잭션으로 묶으면 한 건이 실패할 때
     * 이미 걷은 것까지 되돌아가고, 그동안 선점문이 잡은 행 X 락이 스윕이 끝날 때까지
     * 살아 있어 <b>그 실행을 손으로 걷으려는 운영자가 락 대기에 걸린다.</b>
     * 그래서 반복은 부르는 쪽({@code StuckRunSweeper})에 두고 여기는 한 건만 진다.
     *
     * @param executionId 걷을 실행
     * @param stuckBefore 판정에 쓴 <b>절대 시각</b>. 선점문이 같은 조건을 SQL 로 다시 걸어
     *                    판정과 쓰기 사이를 닫는다 — 그 사이에 실행이 되살아날 수 있다
     * @return 실제로 닫았으면 {@code true}. 선점에 졌거나 실행이 사라졌으면 {@code false}
     */
    @Transactional(timeoutString = "${batch.admin.recover-timeout-seconds:10}")
    public boolean recover(long executionId, LocalDateTime stuckBefore) {
        if (StuckRunClaim.claim(jdbcClient, StuckRunClaim.CLAIM, executionId, stuckBefore) == 0) {
            log.info("시체 스윕이 선점에 졌습니다. 그 사이 진도를 냈거나 누가 먼저 걷었습니다. "
                    + "executionId={}", executionId);
            return false;
        }
        JobExecution claimed = find(executionId);
        if (claimed == null) {
            // 선점이 1행이었는데 못 읽는다 = 배치 메타가 그 사이 지워졌다(정리 잡의 보존
            // 창). 스윕이 할 일은 없고, 던지면 나머지 잡의 시체가 안 걷힌다.
            log.warn("선점한 실행이 사라졌습니다. executionId={}", executionId);
            return false;
        }
        // 선점으로 오른 VERSION 을 반영한 객체로 써야 한다.
        JobExecution closed = jobOperator.recover(claimed);
        if (closed.getStatus().isRunning() || closed.getEndTime() == null) {
            // recover 가 아무것도 안 하고 돌아온 갈래다(문맥의 batch.recovered 또는 종단 상태).
            // @Transactional 이라 선점의 VERSION 증가까지 함께 롤백되어, 다음 스윕이 같은
            // 조건으로 다시 만난다 — 조용히 성공으로 접으면 그 행이 영원히 안 걷힌다.
            throw new IllegalStateException("recover 가 실행을 닫지 못했습니다. status="
                    + closed.getStatus() + " executionId=" + executionId);
        }
        this.recovered.increment();
        log.warn("진도가 멈춘 실행을 자동으로 걷어냈습니다. {} 로 닫았으므로 재시작할 수 "
                        + "있습니다. jobName={} executionId={}",
                BatchStatus.FAILED, closed.getJobInstance().getJobName(), executionId);
        return true;
    }

    /** {@code JobRepository.getJobExecution} 은 없는 id 에 예외를 던진다(실측). */
    private JobExecution find(long executionId) {
        try {
            return jobRepository.getJobExecution(executionId);
        } catch (EmptyResultDataAccessException absent) {
            return null;
        }
    }
}
