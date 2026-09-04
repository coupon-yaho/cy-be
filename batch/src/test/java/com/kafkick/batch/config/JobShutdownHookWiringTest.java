package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>단위 테스트는 배선을 증명하지 못한다.</b> 리스너가 스스로 붙는다는 것은 확인했지만,
 * 실제 컨텍스트에서 <b>어떤 잡이 몇 개 뜨는지</b>는 다른 문제다.
 *
 * <p>이 테스트가 지키는 것 — 잡이 늘어날 때 종료 훅이 <b>자동으로 따라오는 것</b>.
 * 잡 빌더에 손으로 {@code .listener(...)} 를 붙이는 방식이었다면 새 잡을 만드는 사람이
 * 빠뜨릴 수 있고, 그 잡만 SIGTERM 에 {@code STARTED} 시체를 남긴다 —
 * 관제에서는 <i>"가끔 굳는다"</i> 로만 보여서 원인을 찾기 어렵다.
 */
@SpringBootTest(
        properties = {
                // ActuatorExposureTest 와 같은 이유로 실제 설정 파일을 읽는다.
                "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
                "spring.batch.job.enabled=false",
                "batch.scheduling.enabled=false",
                "server.port=0",
                "management.server.port=0"
        })
@Import(MySqlContainerConfig.class)
class JobShutdownHookWiringTest {

    @Autowired
    private List<Job> jobs;

    @Autowired
    private JobShutdownHookListener hookListener;

    @Test
    void everyJobBeanCarriesTheShutdownHookListener() {
        assertThat(jobs).isNotEmpty();

        for (Job job : jobs) {
            assertThat(job)
                    .as("종료 훅을 붙일 수 없는 잡 구현이 생겼습니다: %s", job.getName())
                    .isInstanceOf(AbstractJob.class);
            assertThat(listenersOf((AbstractJob) job))
                    .as("잡 '%s' 에 종료 훅 리스너가 안 붙었습니다. SIGTERM 에 STARTED 로 남습니다.",
                            job.getName())
                    .contains(hookListener);
        }
    }

    /** 잡이 하나도 없으면 위 반복문이 공허하게 통과한다. 실제로 셋이 뜨는 것을 못 박는다. */
    @Test
    void theThreeBatchJobsAreRegistered() {
        assertThat(jobs).extracting(Job::getName)
                .contains("expireJob", "verifyJob", "cleanupJob");
    }

    private static List<Object> listenersOf(AbstractJob job) {
        try {
            var field = AbstractJob.class.getDeclaredField("listener");
            field.setAccessible(true);
            Object composite = field.get(job);
            var listeners = composite.getClass().getDeclaredField("listeners");
            listeners.setAccessible(true);
            Object ordered = listeners.get(composite);
            var iterator = ordered.getClass().getDeclaredMethod("iterator");
            iterator.setAccessible(true);
            List<Object> found = new ArrayList<>();
            ((Iterator<?>) iterator.invoke(ordered)).forEachRemaining(found::add);
            return found;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("리스너 합성체 구조가 바뀌었습니다.", e);
        }
    }
}
