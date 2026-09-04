// SIGTERM 에 잡을 협조적으로 멈춥니다. 안 하면 STARTED 로 굳은 실행이 남습니다.
package com.kafkick.batch.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.JobExecutionShutdownHook;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * <b>시체를 걷는 쪽은 있었는데 덜 만드는 쪽이 없었다.</b>
 *
 * <p>이 저장소는 {@code JobOperator#recover()} 를 이미 쓴다
 * ({@code ExpireRecoveryService} · {@code CleanupRecoveryService}). 그것은 <b>이미 굳은</b>
 * 실행을 {@code FAILED} 로 닫아 재시작 가능하게 만드는 사후 수단이다.
 * 애초에 굳는 것을 줄이는 쪽이 비어 있었다.
 *
 * <p>Spring Batch 6 레퍼런스가 그 인과를 못 박는다 — <i>"우아한 종료가 제대로 수행되지 않으면
 * (즉 JVM 이 갑자기 죽으면) 프레임워크가 실행 상태를 갱신할 기회를 못 얻는다. 그 경우 잡 실행은
 * 재시작할 수 없는 {@code STARTED} 상태로 남는다."</i>
 *
 * <p>컨테이너 SIGTERM(배포·스케일인)은 <b>예측 가능한</b> 종료라 그 기회를 줄 수 있다.
 *
 * <h2>왜 리스너인가 — 기동 시 한 번이 아니다</h2>
 *
 * <p>{@link JobExecutionShutdownHook} 의 생성자가 <b>특정 {@link JobExecution}</b> 을 받는다
 * (6.0.4 바이트코드로 확인: {@code JobExecutionShutdownHook(JobExecution, JobOperator)}).
 * 그래서 기동 시 한 번 등록할 수 없고 <b>실행마다</b> 붙였다 떼야 한다.
 *
 * <p>안 떼면 훅이 실행 수만큼 쌓인다 — {@code Runtime} 이 붙잡고 있는 {@code Thread} 참조라
 * JVM 이 죽을 때까지 안 없어지고, 이미 끝난 실행을 가리키는 죽은 훅이 SIGTERM 때 전부 깨어난다.
 *
 * <h2>왜 잡마다 배선하지 않는가</h2>
 *
 * <p>{@code .listener(...)} 로 잡 빌더에 붙이면 <b>새 잡을 만드는 사람이 빠뜨릴 수 있다.</b>
 * {@link AbstractJob#registerJobExecutionListener} 가 public 이므로 컨텍스트가 다 뜬 뒤
 * {@code Job} 빈 전부에 붙인다 — {@code BatchRunMetrics} 가 {@code List<Job>} 에서 게이지를
 * 자동으로 만드는 것과 같은 방식이고, 그래서 <b>잡을 추가하면 이것도 따라온다.</b>
 *
 * <h2>{@code kill -9} 는 못 막는다</h2>
 *
 * <p>그래서 {@code recover()} 경로를 그대로 둔다. 둘은 대체가 아니라 짝이다 —
 * 이쪽이 <b>덜 만들고</b> 저쪽이 <b>남은 것을 걷는다.</b>
 */
@Component
public class JobShutdownHookListener implements JobExecutionListener, SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(JobShutdownHookListener.class);

    /**
     * 실행별 훅. <b>{@code verifyJob} 은 전용 실행기에서 비동기로 돌고</b> 만료·정리는
     * 스케줄러 스레드에서 도므로 두 잡이 동시에 살아 있을 수 있다.
     */
    private final Map<Long, Thread> hooks = new ConcurrentHashMap<>();

    private final ObjectProvider<JobOperator> jobOperator;
    private final ObjectProvider<Job> jobs;

    /**
     * <b>둘 다 {@link ObjectProvider} 로 받는다.</b> {@code JobOperator} 는 {@code JobRegistry} 를
     * 거쳐 잡을 알고, 이 빈은 그 잡들에 자기를 붙인다 — 생성자에서 직접 받으면 그 고리가
     * 순환이 될 수 있다. 실제로 필요한 시점은 둘 다 컨텍스트가 다 뜬 뒤다.
     */
    public JobShutdownHookListener(
            @Qualifier(BatchJobRepositoryConfig.SHARED_OPERATOR) ObjectProvider<JobOperator> jobOperator,
            ObjectProvider<Job> jobs) {
        this.jobOperator = jobOperator;
        this.jobs = jobs;
    }

    /** 컨텍스트가 다 뜬 뒤 {@code Job} 빈 전부에 자기를 붙인다. */
    @Override
    public void afterSingletonsInstantiated() {
        jobs.stream().forEach(job -> {
            if (job instanceof AbstractJob abstractJob) {
                abstractJob.registerJobExecutionListener(this);
                log.info("종료 훅 리스너를 붙였습니다. job={}", job.getName());
            } else {
                // 붙일 수 없는 구현이 생기면 조용히 넘어가지 않는다 — 그 잡만 SIGTERM 에
                // 시체를 남기는데, 그것은 관제에서 "가끔 굳는다" 로만 보인다.
                log.warn("AbstractJob 이 아니라 종료 훅을 못 붙였습니다. "
                        + "이 잡은 SIGTERM 에 STARTED 로 남습니다. job={} type={}",
                        job.getName(), job.getClass().getName());
            }
        });
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        Thread hook = new JobExecutionShutdownHook(jobExecution, jobOperator.getObject());
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            hooks.put(jobExecution.getId(), hook);
        } catch (IllegalStateException | IllegalArgumentException refused) {
            // JVM 이 이미 종료 중이거나 같은 훅이 이미 붙어 있다. 어느 쪽이든
            // **잡을 실패시키지 않는다** — 관측·정리 장치가 업무를 죽이면 안 된다.
            // 잃는 것은 우아한 종료뿐이고, 그때는 recover() 경로가 받는다.
            log.warn("종료 훅을 못 붙였습니다. 이 실행은 SIGTERM 에 STARTED 로 남을 수 있습니다. "
                    + "executionId={} 사유={}", jobExecution.getId(), refused.toString());
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Thread hook = hooks.remove(jobExecution.getId());
        if (hook == null) {
            return;
        }
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException shuttingDown) {
            // **정상 경로다.** 종료 훅이 잡을 멈추면 afterJob 이 종료 시퀀스 안에서 돌고,
            // 그때 removeShutdownHook 은 언제나 이것을 던진다. 훅은 어차피 곧 사라진다.
            log.debug("종료 중이라 훅을 떼지 않았습니다. executionId={}", jobExecution.getId());
        }
    }

    /** 붙어 있는 훅 수. 누적되지 않는 것을 테스트가 이 값으로 본다. */
    int registeredHookCount() {
        return hooks.size();
    }
}
