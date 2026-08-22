// 검증 배치를 비동기로 띄우는 전용 실행기입니다. 공용 JobOperator 는 건드리지 않습니다.
package com.kafkick.batch.config;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import java.util.concurrent.RejectedExecutionException;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * <b>공용 {@link JobOperator} 를 비동기로 바꾸면 만료 배치가 깨진다.</b>
 *
 * <p>{@code ExpireScheduler} 는 동기 실행을 <b>전제</b>한다. 겹침을 막는 것이 크론 트리거의
 * 순차성이기 때문이다 — 스프링의 {@code ReschedulingRunnable} 이 다음 실행을 <b>직전 실행이
 * 끝난 뒤</b> 잡는다. 비동기가 되면 <b>재고를 쓰는 유일한 잡이 자기 자신과 겹친다.</b>
 * 그쪽은 그 사고를 잡으려고 {@code status.isRunning()} 검사와 <i>"JobOperator 의
 * TaskExecutor 를 확인하십시오"</i> 라는 ERROR 로그까지 심어 뒀다.
 *
 * <p>그래서 <b>verify 전용 빈을 따로 둔다.</b> 공용 빈은 그대로 두고, 이 빈을 쓰는 곳만
 * {@code @Qualifier} 로 명시한다. 빈이 둘이 되는 순간 타입 주입이 모호해지므로
 * {@code ExpireScheduler} 쪽에도 명시가 필요해지는데, <b>그게 오히려 낫다</b> —
 * 어느 잡이 어느 실행기를 쓰는지 코드가 답한다.
 *
 * <p><b>왜 202 가 필요한가.</b> {@code verifyJob} 은 300만 전수라 소요를 아직 모른다.
 * 동기로 응답하면 HTTP 타임아웃이 그 상한을 대신 정하게 되는데, 그건 <b>판정이 끝났는지와
 * 무관한 값</b>이다. {@code docs/10} 이 <i>"시간 상한이 없으므로 동기 응답이 원천적으로
 * 불가능"</i> 이라고 적은 자리다.
 */
@Configuration(proxyBeanMethods = false)
public class VerifyExecutorConfig implements DisposableBean {

    /** 빈이 아니다 — 위 {@link #createExecutor()} 의 이유. 그래서 여기서 들고 정리한다. */
    private ThreadPoolTaskExecutor executor;

    /** 빈 이름을 문자열로 두 곳에 적지 않는다. 주입하는 쪽이 이것을 참조한다. */
    public static final String OPERATOR = "verifyJobOperator";

    /**
     * <b>스레드 하나, 큐 없음. 그리고 빈이 아니다.</b>
     *
     * <p><b>{@code @Bean} 으로 빼면 웹 계층이 다친다.</b> {@code Executor} 타입 빈이 하나라도
     * 생기면 Boot 의 {@code applicationTaskExecutor} 가 조건에서 떨어져 사라지고, MVC 비동기가
     * 요청당 스레드로 폴백하며 {@code spring.task.execution.*} 이 죽는다. 이 저장소가
     * <b>이미 한 번 겪고 되돌린 사고</b>이고, {@code BatchJobRepositoryTest} 가 그것을
     * 단언으로 못 박아 뒀다 — 여기서 다시 밟았다가 그 테스트가 잡았다.
     *
     * <p>이름도 조심한다. {@code BatchRegistrar} 는 이름이 {@code taskExecutor} 인 빈 정의가
     * 있으면 그것을 <b>공용 {@code JobOperator} 에 물린다</b> — 그러면 만료 배치까지 비동기가
     * 되어 이 클래스가 막으려는 바로 그 상태가 된다. 빈이 아니면 그 경로도 닫힌다.
     *
     * <p><b>왜 큐가 없나.</b> 큐가 있으면 <i>"받아 놓고 나중에"</i> 가 되는데, 그 사이
     * {@code asOf} 가 지나가 <b>접수 시점과 실행 시점이 다른 데이터를 본다.</b> 판정의 기준
     * 시각이 요청자가 의도한 것과 달라지므로 미루는 것보다 <b>거절이 맞다</b> —
     * 컨트롤러가 그것을 429 로 옮긴다.
     *
     * <p>{@code setWaitForTasksToCompleteOnShutdown} 은 켜지 않는다. 300만 전수가 도는 중에
     * 종료 신호가 오면 <b>기다리는 쪽이 더 나쁘다</b> — 컨테이너가 SIGKILL 로 넘어가 잡이
     * 어느 Step 중간에서 끊기고, 그 상태는 {@code BATCH_STEP_EXECUTION} 에 남지도 않는다.
     * 대신 {@code stop} 엔드포인트로 사람이 먼저 멈춘다.
     */
    private static ThreadPoolTaskExecutor createExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("verify-");
        // 풀이 차면 호출 스레드에서 대신 돌리는 정책을 쓰면 톰캣 스레드가 300만 전수를
        // 동기로 돌게 된다. 던지게 두고 429 로 옮긴다.
        executor.setRejectedExecutionHandler((task, pool) -> {
            throw new RejectedExecutionException("검증이 이미 실행 중입니다.");
        });
        executor.initialize();
        return executor;
    }

    /**
     * <b>{@code start} 가 즉시 {@link org.springframework.batch.core.job.JobExecution} 을
     * 돌려준다.</b> 그 안의 {@code getId()} 가 202 에 실리는 값이다.
     *
     * <p>{@code runId}({@code verification_runs.id})가 아니다 — 그것은 {@code startRunStep}
     * 이 <b>가드 여덟을 전부 통과한 뒤에야</b> 만든다. 202 시점에는 존재하지 않는다.
     */
    @Bean(name = OPERATOR)
    JobOperator verifyJobOperator(JobRepository jobRepository, JobRegistry verifyJobRegistry)
            throws Exception {
        this.executor = createExecutor();
        TaskExecutorJobOperator operator = new TaskExecutorJobOperator();
        operator.setJobRepository(jobRepository);
        operator.setJobRegistry(verifyJobRegistry);
        operator.setTaskExecutor(executor);
        operator.afterPropertiesSet();
        return operator;
    }

    /**
     * 실행기가 빈이 아니라 스프링이 정리해 주지 않는다. 안 닫으면 스레드가 남아
     * <b>테스트 JVM 과 컨테이너 종료가 늘어진다.</b>
     */
    @Override
    public void destroy() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    /**
     * <b>{@code afterPropertiesSet} 이 요구해서 만든다</b>({@code "JobLocator must be
     * provided"}). 이 저장소는 {@code @EnableBatchProcessing} 으로 Boot 의 배치 자동설정을
     * 물러나게 했고, 그러면 {@code JobRegistry} 빈도 함께 사라진다.
     *
     * <p><b>여기서 잡을 직접 등록하지 않는다.</b> {@link MapJobRegistry} 는
     * {@code SmartInitializingSingleton} 이라 <b>컨텍스트의 {@code Job} 빈을 스스로 모은다</b> —
     * 손으로 또 넣으면 {@code DuplicateJobException} 으로 기동이 깨진다(실제로 겪었다).
     *
     * <p>트리거는 {@code start(Job, JobParameters)} 를 쓰므로 이름 조회를 안 타지만,
     * 자동 등록 덕에 {@code getRunningExecutions(jobName)} 같은 것도 그대로 쓸 수 있다.
     */
    @Bean
    JobRegistry verifyJobRegistry() {
        return new MapJobRegistry();
    }
}
