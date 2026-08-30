// 가드가 만료 잡에 실제로 붙어 있는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.LocalDateTime;

import com.kafkick.storage.db.BatchMetadata;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>가드의 값어치는 "잡이 시작하기 전에 부른다" 하나뿐이다.</b>
 *
 * <p>{@code BinlogFormatGuardTest} 는 객체를 직접 만들어 메서드를 부르므로, {@code @Component}
 * 를 지우거나 {@code expireJob} 의 {@code listener(...)} 등록을 빼도 전부 초록으로 통과한다 —
 * STATEMENT 서버에 배포할 때까지 아무도 모른다. 이 저장소가 반복해서 겪은
 * <b>주석으로만 존재하는 전제</b>가 정확히 그 모양이다.
 *
 * <p><b>자리를 두 번 옮긴 것도 이 축 때문이다.</b> {@code ApplicationReadyEvent} 는 포트가
 * 열린 뒤였고, {@code @ConditionalOnProperty(batch.scheduling.enabled)} 는 스케줄러를 끈 채
 * 손으로 트리거하는 <b>권장 운영 절차</b>에서 빈 자체가 안 생겼다. 그 둘 다 이 테스트가
 * 있었다면 그 자리에서 드러났다.
 *
 * <p>여기서는 가드가 <b>불렸는지</b>만 본다. 무엇을 판정하는지는 {@code BinlogFormatGuardTest}
 * 가 STATEMENT·ROW 컨테이너 둘로 잰다 — 공용 컨테이너는 {@code --skip-log-bin} 이라
 * 통과 경로만 밟기 때문이다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false"
})
@Import({MySqlContainerConfig.class, BinlogFormatGuardWiringTest.CountingGuardConfig.class})
class BinlogFormatGuardWiringTest {

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private org.springframework.jdbc.core.simple.JdbcClient jdbcClient;

    /**
     * <b>배치 메타를 비우고 시작한다.</b> MySQL 컨테이너를 JVM 이 공유하게 되면서
     * ({@code MySqlContainerConfig}) 배치 메타도 <b>클래스 사이를 넘어 산다.</b> 여기가 쓰는
     * {@code asOf} 를 다른 테스트가 이미 썼으면 같은 {@code JobInstance} 라
     * {@code JobInstanceAlreadyCompleteException} 이 난다 — 실제로 그렇게 깨졌다.
     *
     * <p><b>{@code removeJobExecutions()} 만으로는 부족하다</b> — 그것은 실행이 없는
     * 인스턴스를 남긴다. 여기가 겪은 실패가 정확히 그 모양이라
     * {@code VerificationSeed.clear()} 를 쓴다 — 그 안에서 {@link BatchMetadata} 도 함께 돈다.
     */
    @org.junit.jupiter.api.BeforeEach
    void clearBatchMetadata() {
        new com.kafkick.storage.db.VerificationSeed(jdbcClient).clear();
    }

    @Test
    @DisplayName("만료 잡이 시작하면 가드가 먼저 불린다 — 스케줄러가 꺼져 있어도")
    void callsGuardBeforeTheExpireJobStarts() throws Exception {
        CountingGuardConfig.CALLS.set(0);

        JobExecution execution = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", LocalDateTime.of(2026, 1, 15, 9, 0))
                .toJobParameters());

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(CountingGuardConfig.CALLS.get())
                .as("**스케줄러를 끈 채 손으로 돌리는 기동이 실제로 쓰인다.** "
                        + "그 경로에서 가드가 안 불리면, 오류 1665 가 나는 서버에서 "
                        + "화면에 뜨는 것은 그 숫자뿐이다")
                .isEqualTo(1);
    }

    /** 실제 가드를 세는 것으로 갈아 끼운다. 판정은 안 하고 호출만 기록한다. */
    @TestConfiguration
    static class CountingGuardConfig {

        static final AtomicInteger CALLS = new AtomicInteger();

        @Bean
        @Primary
        BinlogFormatGuard countingBinlogFormatGuard(JdbcClient jdbcClient) {
            return new BinlogFormatGuard(jdbcClient) {
                @Override
                public void assertReadCommittedIsUsable() {
                    CALLS.incrementAndGet();
                }
            };
        }
    }
}
