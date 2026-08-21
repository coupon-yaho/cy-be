// 배치 메타데이터가 실제로 DB 에 남는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>이 저장소가 배선되지 않으면 배치 메타데이터가 어디에도 안 남는다.</b>
 *
 * <p>Spring Batch 6 의 기본값은 {@code ResourcelessJobRepository} 이고 Boot 4 의 배치
 * 자동설정이 그것을 그대로 쓴다. 실측으로 확인했다 — 배선 전에는 이랬다.
 *
 * <pre>
 *   repoClass  = ResourcelessJobRepository
 *   instanceId = 1            ← 파라미터가 무엇이든 항상 1
 *   BATCH_JOB_INSTANCE = 0    BATCH_JOB_EXECUTION = 0    BATCH_STEP_EXECUTION = 0
 * </pre>
 *
 * <p><b>이것을 아무도 안 보고 있었다는 것이 진짜 문제였다.</b> 잡은 초록으로 끝나고,
 * {@code V2__batch_metadata.sql} 이 만든 아홉 테이블은 조용히 비어 있었다. 그 상태에서
 * 코드 주석 셋이 거짓이 된다 — 중복 방지, 실행 이력, 진도 이어받기.
 *
 * <p><b>이 테스트가 확인하는 것은 앞 둘이다.</b> 진도 이어받기는 실제 잡을 중간에 죽여야 해서
 * {@code ExpireJobRestartTest} 가 잰다 — 여기서 그것까지 확인한다고 적어 두면, 그 클래스를
 * 끄는 사람이 "저쪽이 지킨다" 고 믿게 된다.
 *
 * <p>클래스 이름만 보는 단언을 하나 남긴 이유는 진단 속도다. 나머지 단언이 깨지면
 * 원인 후보가 여럿이지만, 이것이 함께 깨지면 배선이 풀린 것으로 바로 좁혀진다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false"
})
@Import(MySqlContainerConfig.class)
class BatchJobRepositoryTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private ApplicationContext context;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        new VerificationSeed(jdbcClient).clear();
    }

    @Test
    @DisplayName("실행 이력이 BATCH_* 테이블에 실제로 남는다")
    void writesMetadataToTheDatabase() throws Exception {
        assertThat(jobRepository)
                .as("기본값으로 돌아가면 아래 단언이 전부 0행으로 깨진다. "
                        + "그때 원인을 여기서 바로 읽으라고 남긴 단언이다")
                .isNotInstanceOf(ResourcelessJobRepository.class);

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(rowCount("BATCH_JOB_INSTANCE"))
                .as("인스턴스가 남아야 같은 파라미터의 재실행을 막을 수 있다")
                .isEqualTo(1);
        assertThat(rowCount("BATCH_JOB_EXECUTION"))
                .as("**언제 돌았나가 여기 남는다.** 운영자가 볼 유일한 곳이다")
                .isEqualTo(1);
        assertThat(rowCount("BATCH_STEP_EXECUTION"))
                .as("알림 규칙이 WRITE_COUNT 로 처리량을 보라고 안내하는 그 테이블이다")
                .isEqualTo(1);
    }

    /**
     * <b>중복 방지가 <i>프로세스 밖</i>에서도 사는지 본다.</b>
     *
     * <p>예외가 나는 것만으로는 부족하다 — {@code ResourcelessJobRepository} 도 <b>같은
     * JVM 안에서는</b> 인스턴스를 기억해서 두 번째 실행을 막는다(배선을 지우고 돌려 확인했다).
     * 즉 이 예외 단언 하나는 배선이 풀린 것을 못 잡는다.
     *
     * <p>갈리는 것은 <b>그 사실이 DB 에 있느냐</b>다. 배치를 여러 대로 띄우면 서로의 메모리를
     * 못 보므로, {@code BATCH_JOB_INSTANCE} 에 행이 있어야만 중복 방지가 인스턴스 경계를
     * 넘는다. {@code ExpireScheduler} 가 {@code asOf} 를 <b>크론 슬롯</b>에서 뽑는 근거가
     * 그것이다({@code CronSlot}) — 노드마다 실행이 밀리는 정도가 달라도 같은 슬롯이면 같은 값이다.
     */
    @Test
    @DisplayName("같은 asOf 로 완료된 실행은 다시 돌지 않고, 그 사실이 DB 에 남는다")
    void refusesToRerunACompletedAsOf() throws Exception {
        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThatThrownBy(this::launch)
                .isInstanceOf(JobInstanceAlreadyCompleteException.class);

        assertThat(rowCount("BATCH_JOB_INSTANCE"))
                .as("**막았다는 사실이 DB 에 있어야 다른 인스턴스도 그것을 본다.** "
                        + "메모리에만 있으면 배치를 두 대로 늘리는 순간 둘 다 돈다")
                .isEqualTo(1);
    }

    /**
     * <b>{@code JobOperator} 의 동기 실행 전제를 여기서 지킨다.</b>
     *
     * <p>{@code BatchRegistrar} 는 이름이 {@code taskExecutor} 인 빈 <b>정의가 있으면</b>
     * 그것을 {@code JobOperator} 에 물린다. 그러면 {@code start} 가 즉시 {@code STARTED} 를
     * 돌려주고, {@code ExpireScheduler} 의 겹침 방지 — 크론 트리거가 앞 실행이 끝난 뒤에야
     * 다음을 잡는 것 — 가 통째로 사라져 만료가 자기 자신과 겹친다.
     *
     * <p><b>전용 {@code SyncTaskExecutor} 빈으로 못 박아 봤다가 되돌렸다.</b>
     * {@code Executor} 타입 빈이 하나라도 생기면 Boot 의 {@code applicationTaskExecutor} 가
     * 조건에서 떨어져 사라진다(실측). 그래서 배선을 건드리는 대신 <b>그 이름의 빈이 없다는
     * 것</b>과 <b>Boot 기본 실행기가 살아 있다는 것</b>을 여기서 함께 단언한다.
     */
    @Test
    @DisplayName("taskExecutor 라는 이름의 빈이 없고, Boot 기본 실행기는 살아 있다")
    void keepsJobOperatorSynchronousWithoutStealingBootsExecutor() {
        assertThat(context.containsBean("taskExecutor"))
                .as("이 이름의 빈이 생기면 JobOperator 가 비동기가 되고 ExpireScheduler 의 "
                        + "겹침 방지가 죽는다. 스프링 예제가 관례로 쓰는 이름이라 실제로 들어온다")
                .isFalse();
        assertThat(context.containsBean("applicationTaskExecutor"))
                .as("**여기를 뺏으면 웹 계층이 다친다.** Executor 빈을 하나 정의하면 Boot 의 "
                        + "이 빈이 조건에서 떨어져 MVC 비동기가 요청당 스레드로 폴백하고 "
                        + "spring.task.execution.* 이 죽는다")
                .isTrue();
    }

    /**
     * <b>순차 재실행 거부만으로는 다중 노드를 못 지킨다.</b> 위 테스트는 한 스레드가 끝낸 뒤
     * 다시 부른다 — {@code ResourcelessJobRepository} 도 같은 JVM 안에서는 그것을 막았다.
     *
     * <p>실제로 지켜야 하는 것은 <b>두 노드가 같은 순간에 같은 {@code asOf} 로 시작하는 것</b>
     * 이다. 그때 막는 것은 격리 수준이 아니라 {@code V2__batch_metadata.sql} 의
     * {@code JOB_INST_UN UNIQUE (JOB_NAME, JOB_KEY)} 이고, 그 인덱스를 지우면 두 노드가 각자
     * 인스턴스를 만든다.
     *
     * <p><b>예외 타입이 갈리는 것도 여기서 확인된다.</b> 인스턴스 생성 격리를
     * {@code READ_COMMITTED} 로 내려 뒀기에 진 쪽이 {@code DuplicateKeyException} 을 받는다 —
     * 기본값 {@code SERIALIZABLE} 이면 gap 락 때문에 데드락({@code 1213})이라 다른 타입이 오고,
     * {@code ExpireScheduler} 의 INFO 갈래를 못 탄다.
     */
    @Test
    @DisplayName("두 노드가 같은 asOf 로 동시에 시작해도 인스턴스는 하나다")
    void twoSimultaneousStartsProduceExactlyOneInstance() throws Exception {
        int racers = 2;
        CountDownLatch fire = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        List<Future<Object>> results = new ArrayList<>();
        try {
            for (int i = 0; i < racers; i++) {
                results.add(pool.submit(() -> {
                    fire.await();
                    try {
                        return launch();
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
                    .as("둘 다 돌면 같은 300만 행을 나란히 훑으며 서로의 X 락을 기다린다")
                    .singleElement()
                    .extracting(o -> ((JobExecution) o).getStatus())
                    .as("이긴 쪽은 끝까지 돌아야 한다 — 상태를 안 보면 잡이 항상 FAILED 인 "
                            + "돌연변이가 여기를 통과한다")
                    .isEqualTo(BatchStatus.COMPLETED);
            assertThat(outcomes).filteredOn(Throwable.class::isInstance)
                    .as("**진 쪽이 받는 타입은 인터리빙에 따라 둘로 갈린다.** 인스턴스 생성이 "
                            + "READ COMMITTED 라 진 쪽의 SELECT 가 안 막히므로, 상대가 아직 "
                            + "커밋 전이면 INSERT 까지 가서 1062(DuplicateKey), 이미 커밋했으면 "
                            + "그 앞의 Assert.state 에서 IllegalState 다. 둘 다 '중복 방지가 "
                            + "일했다' 이고 ExpireScheduler 가 둘 다 INFO 로 받는다.\n"
                            + "격리를 기본값(SERIALIZABLE)으로 되돌리면 gap 락 데드락이라 "
                            + "DeadlockLoser 가 오고, 그것은 ERROR 로 나간다 — 그래서 배제한다")
                    .singleElement()
                    .isInstanceOfAny(DuplicateKeyException.class, IllegalStateException.class)
                    .isNotInstanceOf(DeadlockLoserDataAccessException.class);
            assertThat(rowCount("BATCH_JOB_INSTANCE"))
                    .as("**막았다는 사실이 DB 에 하나로 남아야 한다.** JOB_INST_UN 을 지우면 "
                            + "여기가 2 가 된다")
                    .isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    private JobExecution launch() throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
    }

    private int rowCount(String table) {
        return jdbcClient.sql("SELECT COUNT(*) FROM " + table).query(Integer.class).single();
    }
}
