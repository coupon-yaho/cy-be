package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * {@link BatchJobRepositoryConfig} 가 <b>실제로 붙어서</b> 배치 메타가 DB 에 쌓이는지 본다.
 *
 * <p>이 단언이 없으면 이력 조회 API 가 <b>항상 빈 목록을 돌려주면서 통과한다</b> — 조회 대상이
 * 애초에 없기 때문이다. 실제로 그런 상태를 한 번 만들었다: 조건을
 * {@code @ConditionalOnBean} 으로 썼더니 그 조건이 항상 거짓이라 설정이 통째로 안 붙었는데,
 * 당시 batch 테스트 87개가 전부 초록불이었다.
 *
 * <p><b>이 브랜치에는 업무 배치가 없어 테스트 스코프 잡으로 확인한다.</b>
 * {@code feature/CY-15} 에는 같은 성질을 보는 {@code BatchJobRepositoryTest} 가 실제 잡으로
 * 이미 있다 — 합류하면 이 파일은 지운다. 파일 이름을 달리 둔 것은 같은 경로·같은 이름으로
 * 부딪히지 않게 하기 위해서다.
 */
@SpringBootTest(properties = "spring.flyway.enabled=true")
@Import({ MySqlContainerConfig.class, BatchMetadataPersistenceTest.ProbeJob.class })
class BatchMetadataPersistenceTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class ProbeJob {
        @Bean
        Job metadataProbeJob(JobRepository repository,
                @Qualifier("transactionManager") PlatformTransactionManager tx) {
            Step step = new StepBuilder("metadataProbeStep", repository)
                    .tasklet((contribution, context) -> RepeatStatus.FINISHED, tx)
                    .build();
            return new JobBuilder("metadataProbeJob", repository).start(step).build();
        }
    }

    @Autowired
    JobOperator jobOperator;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    Job metadataProbeJob;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    @Qualifier("transactionManager")
    PlatformTransactionManager transactionManager;

    /**
     * CY-5 병합으로 뒤집힌 계약을 못박는다. {@code MainDataSourceConfig} 는
     * {@code EntityManagerFactory} 가 있으면 JPA 매니저를, 없으면 JDBC 매니저를 고른다.
     * 엔티티가 없던 시절 batch 는 후자였고, 지금은 전자다.
     *
     * <p><b>그냥 넘길 변화가 아니라서 값으로 남긴다.</b> 배치 메타 쓰기가 이 매니저를 타고,
     * {@code @EnableJdbcJobRepository} 는 {@code isolationLevelForCreate=READ_COMMITTED} 를
     * 요구한다. JPA 매니저가 그 격리 수준을 실제로 적용하는지는 위
     * "두 스레드가 같은 파라미터로 동시에 시작해도 인스턴스는 하나다" 가 검증한다 — 실측으로
     * 그 테스트가 이 매니저 위에서 통과한다.
     *
     * <p>이 단언이 깨지는 날은 둘 중 하나다. 엔티티가 사라졌거나, 누가 Spring Batch 전용
     * JDBC 매니저를 따로 만들어 물렸거나. 어느 쪽이든 위 동시성 테스트를 함께 확인할 것.
     */
    @Test
    @DisplayName("배치 메타는 JPA 트랜잭션 매니저 위에서 돈다 — 엔티티가 생기며 뒤집혔다")
    void batchMetadataRunsOnTheJpaTransactionManager() {
        assertThat(transactionManager).isInstanceOf(JpaTransactionManager.class);
    }

    private int instanceCount() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ?",
                Integer.class, "metadataProbeJob");
    }

    @Test
    @DisplayName("잡을 돌리면 BATCH_JOB_EXECUTION 에 행이 쌓인다")
    void metadataRowsArePersisted() throws Exception {
        Integer before = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class);

        JobExecution execution = jobOperator.start(metadataProbeJob,
                new JobParametersBuilder().addString("runId", "rows-1").toJobParameters());

        assertThat(execution.getStatus().name()).isEqualTo("COMPLETED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class))
                .isEqualTo(before + 1);
    }

    /**
     * <b>격리 수준을 내린 것이 실제로 일하는지</b>는 두 스레드가 겨뤄야만 보인다.
     *
     * <p>{@link BatchJobRepositoryConfig} 는 {@code isolationLevelForCreate} 를 애너테이션
     * 기본값 {@code SERIALIZABLE} 에서 {@code READ_COMMITTED} 로 <b>내려</b> 두었고,
     * 그 근거로 적힌 실측표는 {@code feature/CY-15} 에서 잰 값이다. 이 브랜치에는 그것을
     * 지키는 단언이 <b>없었다</b> — 단일 스레드 테스트로는 gap 락 경합 자체가 안 나서
     * 기본값으로 되돌려도 전부 초록불이다.
     *
     * <p><b>진 쪽이 받는 타입이 인터리빙에 따라 셋으로 갈린다.</b> 인스턴스 생성이
     * READ COMMITTED 라 진 쪽의 SELECT 가 안 막힌다 — 상대가 아직 커밋 전이면 INSERT 까지
     * 가서 1062({@code DuplicateKey}), 이미 커밋했으면 그 앞의 {@code Assert.state} 에서
     * {@code IllegalState}, <b>잡까지 끝냈으면</b> 바깥에서
     * {@code JobInstanceAlreadyComplete} 다. 셋 다 "중복 방지가 일했다" 이고 잡 실행 진입점이
     * 전부 INFO 로 받는다({@code ExpireScheduler}, feature/CY-15).
     *
     * <p><b>기본값으로 되돌리면 gap 락 경합이라 잠금 실패 계열이 온다.</b> 실측하면
     * {@code CannotAcquireLockException} 이었다 — 처음에 {@code DeadlockLoserDataAccessException}
     * 만 배제했는데 그 형제 타입이라 안 걸린다. 둘의 공통 부모인
     * {@code PessimisticLockingFailureException} 으로 배제해야 격리를 되돌린 것이 정확히 잡힌다.
     *
     * <p>그 계열은 잡 실행 진입점의 INFO 갈래를 타지 못하고 ERROR 로 나간다 — 중복 방지가
     * 제 일을 한 것인데 사고처럼 보인다. 그것이 격리를 내린 이유다.
     *
     * <p>CY-15 의 {@code BatchJobRepositoryTest} 가 실제 만료 잡으로 같은 것을 본다.
     * 합류하면 이 메서드는 그쪽이 대신한다.
     */
    @Test
    @DisplayName("두 스레드가 같은 파라미터로 동시에 시작해도 인스턴스는 하나다")
    void twoSimultaneousStartsProduceExactlyOneInstance() throws Exception {
        // 같은 컨텍스트를 쓰는 앞 테스트가 이미 인스턴스를 남긴다. 절대 수가 아니라
        // 이 경주가 만든 증가분을 본다 — 절대 수로 두면 테스트 순서에 결과가 매달린다.
        int before = instanceCount();
        int racers = 2;
        CountDownLatch fire = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        List<Future<Object>> results = new ArrayList<>();
        try {
            for (int i = 0; i < racers; i++) {
                results.add(pool.submit(() -> {
                    fire.await();
                    try {
                        return jobOperator.start(metadataProbeJob, new JobParametersBuilder()
                                .addString("runId", "race-1").toJobParameters());
                    } catch (Exception e) {
                        return e;
                    }
                }));
            }
            fire.countDown();

            List<Object> outcomes = new ArrayList<>();
            for (Future<Object> future : results) {
                outcomes.add(future.get(60, TimeUnit.SECONDS));
            }

            assertThat(outcomes).filteredOn(JobExecution.class::isInstance)
                    .as("이긴 쪽은 끝까지 돌아야 한다 — 상태를 안 보면 잡이 항상 FAILED 인 "
                            + "돌연변이가 여기를 통과한다")
                    .singleElement()
                    .extracting(o -> ((JobExecution) o).getStatus().name())
                    .isEqualTo("COMPLETED");
            assertThat(outcomes).filteredOn(Throwable.class::isInstance)
                    .as("진 쪽이 받는 타입은 인터리빙에 따라 셋으로 갈린다(위 설명). "
                            + "DeadlockLoser 가 오면 격리가 기본값으로 되돌아간 것이다")
                    .singleElement()
                    .isInstanceOfAny(DuplicateKeyException.class, IllegalStateException.class,
                            JobInstanceAlreadyCompleteException.class)
                    .isNotInstanceOf(PessimisticLockingFailureException.class);
            assertThat(instanceCount() - before)
                    .as("막았다는 사실이 DB 에 하나로 남아야 한다. JOB_INST_UN 을 지우면 2 가 된다")
                    .isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("기본 ResourcelessJobRepository 로 떨어지지 않았다")
    void jobRepositoryIsJdbcBacked() {
        // 이름으로 본다. 프록시가 씌워져 구현 타입이 안 보인다.
        assertThat(jobRepository.getClass().getName()).doesNotContain("Resourceless");
    }
}
