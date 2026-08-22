// 만료 배치가 오염 스키마에서 도는 것을 막는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.job.JobFailures;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>만료는 원본을 쓰는 유일한 배치다.</b> 오염셋을 보게 띄우면 오염 유형 2·7
 * (둘 다 {@code status = 'ISSUED'})의 발급건을 {@code EXPIRED} 로 넘기고 이력까지 붙인다.
 * 그러면 {@code expected_findings} 800행과 어긋나는데, <b>그 어긋남이 "검증기가 틀렸다" 로
 * 보인다</b> — 실제 원인은 만료가 한 번 지나간 것이고 그 사실은 어디에도 안 적힌다.
 *
 * <p><b>회차 격리가 이 위험을 넓혔다.</b> 예전에는 첫 오염 회차에서 죽어 그 뒤로는 아무것도
 * 안 건드렸다. 지금은 막힌 회차만 빼고 나머지 전부를 넘긴다.
 *
 * <p>형제는 {@code BinlogFormatGuardWiringTest} 다 — 그쪽도 {@code batch.config} 의
 * 가드를 {@code @SpringBootTest} 로 {@code expireJob} 을 실제로 기동해 검증한다.
 *
 * <p><b>스키마 판정 자체는 여기서 안 잰다.</b> {@code uk_coupon_member} 로 가르는 그 질의는
 * {@code verifyJob} 의 {@code rejectDatasetMismatch} 가 이미 쓰고 있고, 같은 사실을 두 곳이
 * 각자 판정하지 않는 것이 이 가드의 설계다. 여기서 재는 것은 <b>그 판정이 만료를 실제로
 * 멈추는가</b> — 공용 컨테이너는 CLEAN 스키마라 오염 쪽 분기는 포트를 갈아 끼워 밟는다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=100"
})
@ExtendWith(SchemaShapeConfig.class)
@Import({MySqlContainerConfig.class, SchemaShapeConfig.class,
        CleanSchemaGuardTest.FixedClockConfig.class})
class CleanSchemaGuardTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 고정한 "지금". 태스클릿이 {@code asOf} 를 이 값과 견준다. */
    private static final LocalDateTime NOW = AS_OF.plusMinutes(3);

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

    /**
     * <b>한 건이라도 넘어가면 늦다.</b> {@code EXPIRED} 는 종단 상태라 되돌릴 전이가 없고,
     * {@code EXPIRE} 이력도 이미 붙는다. 그래서 첫 청크 전에 죽어야 한다.
     */
    @Test
    @DisplayName("오염 스키마에서는 한 건도 안 넘기고 죽는다")
    void refusesToRunOnCorruptSchema() throws Exception {
        seed.newCoupon();
        long target = expiring();
        seed.overwriteStock(1);

        SchemaShapeConfig.pretendCorrupt();
        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(JobFailures.errorCodesOf(execution))
                .as("**판정이 아니라 실패다.** 데이터가 틀린 것이 아니라 여기서 돌면 안 되는 "
                        + "배치가 돈 것이고, 고칠 곳은 데이터가 아니라 접속 설정이다")
                .contains(ExpirationErrorCode.EXPIRE_ON_CORRUPT_SCHEMA.getCode());
        assertThat(statusOf(target))
                .as("오염 유형 2·7 은 둘 다 ISSUED 다. 여기서 한 건이라도 넘어가면 "
                        + "expected_findings 와 어긋나고 그것이 검증기 버그처럼 보인다")
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(activeCount())
                .as("재고도 그대로여야 한다 — dataset_fingerprint 가 여기에 걸려 있다")
                .isEqualTo(1);
    }

    /**
     * <b>막는 쪽만 보면 항상 던지는 가드도 통과한다.</b> 위 테스트의 짝이다 — 정상 스키마에서
     * 만료가 실제로 도는 것까지 봐야 이 가드가 "만료를 통째로 멈추는" 실패가 아님이 확인된다.
     */
    @Test
    @DisplayName("정상 스키마에서는 그대로 넘긴다")
    void runsNormallyOnCleanSchema() throws Exception {
        seed.newCoupon();
        long target = expiring();
        seed.overwriteStock(1);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(statusOf(target)).isEqualTo(IssuanceStatus.EXPIRED.name());
    }

    private JobExecution launch() throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
    }

    private long expiring() {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();
        return id;
    }

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int activeCount() {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks ORDER BY coupon_id LIMIT 1")
                .query(Integer.class)
                .single();
    }

    /**
     * <b>시계를 고정한다.</b> 태스클릿은 {@code asOf} 가 현재보다 미래면
     * {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는다. 벽시계로 두면 이 테스트의 통과 여부가
     * <b>실행하는 날짜</b>에 딸린다 — 지금은 {@code AS_OF} 가 과거라 통과하지만, 그것은
     * 이 테스트가 지키려는 성질(오염 스키마를 막는가)과 아무 상관이 없는 이유다.
     *
     * <p>운영 {@code TimeConfig} 가 {@code systemUTC} 라 오프셋도 UTC 로 맞춘다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfig {

        @Bean
        @Primary
        Clock fixedClock() {
            return FixedClock.at(NOW);
        }
    }
}
