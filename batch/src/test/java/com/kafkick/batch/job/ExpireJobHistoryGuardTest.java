// 이력 수가 만료 건수와 어긋났을 때 잡이 멈추는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>정상 흐름에서는 이력 수와 만료 건수가 항상 같다.</b> 그래서 그 짝 검사는 지워도 아무것도
 * 빨개지지 않는다 — 실제로 돌연변이로 확인했다. 도달할 수 없는 가드는 <b>없는 가드와 같다.</b>
 *
 * <p>그래서 어긋난 상태를 만들어 준다. 저장소를 프록시로 감싸 이력 수를 하나 줄여 돌려주면,
 * 잡이 그것을 잡고 멈춰야 한다. 안 멈추면 상태만 바뀌고 이력이 빠진 데이터가 남는데,
 * 그 뒤 검증은 원인을 <i>"이력 없는 발급건"</i> 으로만 보고할 뿐 이 잡을 지목하지 못한다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 ExpirationRepository 프록시에 countPending 을 부른다 —
        // 백그라운드 틱이 호출 기록에 끼면 재현 불가로 빨개진다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.expire.chunk-size=10"
})
@Import({MySqlContainerConfig.class, ExpireJobHistoryGuardTest.ShortHistoryConfig.class})
class ExpireJobHistoryGuardTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    @Test
    @DisplayName("이력 수가 만료 건수와 다르면 잡이 실패한다")
    void failWhenHistoryCountDiverges() throws Exception {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();

        JobExecution execution = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(JobFailures.errorCodesOf(execution))
                .as("**계약은 에러 코드다.** 메시지 문구만 보면 문구를 다듬을 때 깨지고, "
                        + "코드가 바뀌어도 통과한다")
                .contains(ExpirationErrorCode.EXPIRE_HISTORY_COUNT_MISMATCH.getCode());
        assertThat(JobFailures.messagesOf(execution))
                .as("운영자가 로그에서 읽을 문장도 함께 남는다")
                .anyMatch(message -> message.contains("만료 이력 수가 만료 건수와 다릅니다"));
    }

    /** 이력 수만 하나 줄여 돌려준다. 나머지 동작은 실제 저장소 그대로다. */
    @TestConfiguration
    static class ShortHistoryConfig {

        @Bean
        static BeanPostProcessor shortHistory() {
            return ExpirationProxies.decorating((real, method, args) -> {
                Object result = ExpirationProxies.callThrough(real, method, args);
                if ("appendExpireHistories".equals(method.getName())) {
                    return (int) result - 1;
                }
                return result;
            });
        }
    }
}
