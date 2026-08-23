// 실행 중인(또는 진도가 멈춘) 잡을 배치 메타에 심어 두는 테스트 픽스처입니다.
package com.kafkick.batch.config;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * <b>진짜 잡을 띄워 경쟁시키지 않는다.</b> 그러면 테스트가 그 잡의 소요 시간에 기대게 되고,
 * CI 가 느린 날 <i>"이미 끝나서 안 막혔다"</i> 로 무작위로 깨진다.
 *
 * <p>가드가 읽는 것은 {@code BATCH_JOB_EXECUTION} 의 상태와
 * {@code BATCH_STEP_EXECUTION.LAST_UPDATED} 뿐이므로 <b>그 행들을 직접 만든다.</b>
 *
 * <p><b>반드시 닫는다.</b> Testcontainers MySQL 을 테스트 클래스들이 공유하므로, 남겨 두면
 * 아무 상관 없는 테스트가 <i>"그 잡이 도는 중"</i> 으로 거절당한다.
 */
public final class RunningJobFixture implements AutoCloseable {

    private final JobRepository jobRepository;
    private final JobInstance instance;
    private final JobExecution execution;
    private JdbcClient jdbcClient;

    private RunningJobFixture(JobRepository jobRepository, JobInstance instance,
            JobExecution execution) {
        this.jobRepository = jobRepository;
        this.instance = instance;
        this.execution = execution;
    }

    /**
     * 방금 떠서 방금 진도를 낸 실행.
     *
     * @param key 인스턴스를 가르는 파라미터. {@code JOB_INST_UN} 이 (잡 이름, 파라미터 키)에
     *            걸려 있어 같은 값을 두 번 쓰면 두 번째 심기가 깨진다
     */
    public static RunningJobFixture plant(JobRepository jobRepository, JdbcClient jdbcClient,
            String jobName, LocalDateTime key) {
        return plant(jobRepository, jdbcClient, jobName, key, Duration.ZERO, Duration.ZERO);
    }

    /**
     * 시작 시각과 <b>마지막 진도 시각을 따로</b> 지정해 심는다.
     *
     * <p>둘을 가르는 것이 이 픽스처의 핵심이다 — <i>"오래 돌지만 진도가 나가는 잡"</i>과
     * <i>"죽어서 진도가 멈춘 잡"</i>을 구분해야 가드가 제대로 재진다.
     *
     * <p><b>진도는 SQL 로 되돌린다.</b> {@code JobRepository.update(StepExecution)} 은 못 쓴다 —
     * 그것이 {@code lastUpdated} 를 <b>자기가 {@code LocalDateTime.now()} 로 다시 찍기</b>
     * 때문이다(6.0.4 바이트코드로 확인). 그리고 그 사실이야말로 이 컬럼을
     * <b>하트비트로 쓸 수 있다는 증거</b>다.
     *
     * <p>⚠️ <b>절대 시각을 SQL 로 넣으면 안 된다.</b> 드라이버가 쓰기에서 지역시각을 UTC 로
     * 옮기는데, 우리가 넣은 값은 그 변환을 안 거치고 읽기 변환만 한 번 더 먹는다 —
     * KST 기기에서 <b>아홉 시간 미래</b>로 되살아난다(실측했다: 넣은 값 {@code 08:46},
     * 읽은 값 {@code 17:46}). 그래서 프레임워크가 방금 찍은 값을 <b>상대로 민다</b>.
     * 그러면 좌표계를 아예 안 건드린다.
     *
     * @param startedAgo  실행이 뜬 지 얼마나 됐나
     * @param progressAgo 마지막 청크 커밋이 얼마나 오래됐나. 이것이 <b>살았는지 죽었는지</b>를
     *                    가른다. <b>초 단위만 유효하다</b> — 아래 {@code INTERVAL … SECOND} 가
     *                    밀리초를 버린다
     */
    public static RunningJobFixture plant(JobRepository jobRepository, JdbcClient jdbcClient,
            String jobName, LocalDateTime key,
            Duration startedAgo, Duration progressAgo) {
        RunningJobFixture fixture = plantWithoutStep(
                jobRepository, jobName, key, LocalDateTime.now().minus(startedAgo));

        StepExecution step =
                jobRepository.createStepExecution("plantedStep", fixture.execution);
        step.setStatus(BatchStatus.STARTED);
        jobRepository.update(step);

        fixture.jdbcClient = jdbcClient;

        // **여기서 던지면 픽스처가 호출자에게 안 간다** — try-with-resources 에 들어가지
        // 못하므로 close() 도 안 불리고, 방금 심은 두 행이 공유 컨테이너에 그대로 남는다.
        // 그러면 아무 상관 없는 다음 테스트가 "그 잡이 도는 중" 으로 거절당한다 —
        // 이 클래스가 막으려던 바로 그 상태다. 던지기 전에 치운다.
        try {
            int backdated = jdbcClient.sql(
                            "UPDATE BATCH_STEP_EXECUTION "
                            + "SET LAST_UPDATED = "
                            + "DATE_SUB(LAST_UPDATED, INTERVAL :seconds SECOND) "
                            + "WHERE STEP_EXECUTION_ID = :id")
                    .param("seconds", progressAgo.toSeconds())
                    .param("id", step.getId())
                    .update();
            if (backdated != 1) {
                // 조용히 0행이 되면 진도가 현재 시각으로 남아, "시체" 테스트가 이유 없이
                // 빨개진다 — 원인을 찾는 데 오래 걸린다.
                throw new IllegalStateException(
                        "심은 Step 의 진도를 되돌리지 못했습니다. stepExecutionId="
                                + step.getId());
            }
        } catch (RuntimeException e) {
            fixture.close();
            throw e;
        }
        return fixture;
    }

    /**
     * <b>{@code STARTING} 에서 죽어 Step 이 하나도 없는 실행</b>을 심는다.
     *
     * <p>이 갈래가 있어야 프로브의 폴백({@code startTime} → {@code createTime})이 재진다.
     * 폴백이 없으면 이런 행이 <b>영원히</b> 가드를 켜 두고, 만료 실행에는 {@code abandon}
     * 경로가 없어 SQL 을 직접 치는 수밖에 없다.
     */
    public static RunningJobFixture plantWithoutStep(JobRepository jobRepository, String jobName,
            LocalDateTime key, LocalDateTime startedAt) {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", key)
                .toJobParameters();

        JobInstance instance = jobRepository.createJobInstance(jobName, parameters);
        JobExecution execution =
                jobRepository.createJobExecution(instance, parameters, new ExecutionContext());

        // createJobExecution 은 STARTING 으로 만든다. 그것도 가드가 보는 상태지만, 실제로
        // 막는 상황은 대부분 STARTED 이므로 그쪽을 재현한다.
        execution.setStatus(BatchStatus.STARTED);
        execution.setStartTime(startedAt);
        jobRepository.update(execution);

        return new RunningJobFixture(jobRepository, instance, execution);
    }

    /**
     * <b>이미 끝난 성공 실행</b>을 심는다. 마지막 성공 시각 축이 쓰는 모양이다.
     *
     * <p>{@code END_TIME} 은 <b>프레임워크 쓰기 경로로</b> 넣는다 — 드라이버가 지역시각을
     * UTC 로 옮기고 읽을 때 되돌리므로 그 경로로 넣어야 왕복이 맞는다. SQL 로 절대 시각을
     * 넣으면 읽기 변환만 한 번 더 먹어 <b>아홉 시간 미래</b>로 되살아난다(실측했다).
     */
    public static RunningJobFixture plantCompleted(JobRepository jobRepository, String jobName,
            LocalDateTime key, Duration endedAgo) {
        // now() 를 한 번만 읽는다. 두 번 읽으면 시작 시각과 종료 시각이 서로 다른 기준을
        // 갖고, 창 경계(7일) 테스트가 그 사이 오차만큼 흔들린다.
        //
        // 고정 시계(Clock.fixed)는 여기서 쓸 수 없다. 조회 창이 **DB 의 NOW()** 를 기준으로
        // 자르므로, 심는 값이 그 시계와 같은 흐름 위에 있어야 한다 — 고정 시계로 심으면
        // 테스트가 도는 시점에 따라 창 안팎이 뒤집힌다. 그래서 상대(Duration)로 받는다.
        LocalDateTime endedAt = LocalDateTime.now().minus(endedAgo);
        RunningJobFixture fixture = plantWithoutStep(
                jobRepository, jobName, key, endedAt.minusMinutes(1));

        fixture.execution.setStatus(BatchStatus.COMPLETED);
        fixture.execution.setEndTime(endedAt);
        jobRepository.update(fixture.execution);

        return fixture;
    }

    /** 심은 실행의 종료 시각. 게이지가 이 값을 에폭으로 내야 한다. */
    public LocalDateTime endTime() {
        return execution.getEndTime();
    }

    public long executionId() {
        return execution.getId();
    }

    /**
     * <b>SQL 로 지운다.</b> {@code JobRepository} 의 삭제 API 는 낙관적 락이라 들고 있는
     * 객체의 {@code VERSION} 이 DB 와 맞아야 하는데, 위에서 {@code LAST_UPDATED} 를 SQL 로
     * 되돌린 뒤로는 그 전제가 성립하지 않는다. 그대로 부르면
     * {@code OptimisticLockingFailureException} 이 나고, <b>정리가 안 된 채 남은 행이</b>
     * 아무 상관 없는 다음 테스트를 <i>"그 잡이 도는 중"</i> 으로 막는다.
     *
     * <p>순서는 FK 를 따른다. 픽스처가 만든 것만 지우므로 다른 테스트의 행은 안 건드린다.
     */
    @Override
    public void close() {
        if (jdbcClient != null) {
            long executionId = execution.getId();
            jdbcClient.sql("DELETE FROM BATCH_STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID IN "
                            + "(SELECT STEP_EXECUTION_ID FROM BATCH_STEP_EXECUTION "
                            + " WHERE JOB_EXECUTION_ID = :id)")
                    .param("id", executionId).update();
            jdbcClient.sql("DELETE FROM BATCH_STEP_EXECUTION WHERE JOB_EXECUTION_ID = :id")
                    .param("id", executionId).update();
        }
        JobExecution current = jobRepository.getJobExecution(execution.getId());
        jobRepository.deleteJobExecution(current);
        jobRepository.deleteJobInstance(instance);
    }
}
