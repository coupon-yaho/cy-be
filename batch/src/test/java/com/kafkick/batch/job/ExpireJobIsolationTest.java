// 만료 Step 이 실제로 READ COMMITTED 로 도는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
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
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>이 잡이 발급을 막지 않는 유일한 이유가 한 줄이다.</b>
 *
 * <pre>
 * ExpireJobConfig: stepTimeout.setIsolationLevel(ISOLATION_READ_COMMITTED)
 * </pre>
 *
 * <p>그 줄이 사라지면 REPEATABLE READ 로 돌아가고, {@code status='ISSUED'} 를 찾다가 잡는
 * gap 락이 신규 발급 INSERT 를 오류 1205 로 죽인다. 인덱스로는 안 풀린다 —
 * 막는 것이 스캔 범위가 아니라 잠금 위치이기 때문이다({@code docs/12} §3).
 *
 * <p><b>그런데 그 줄을 지켜 주는 것이 없었다.</b> 락을 재는
 * {@code ExpirationLockScopeTest} 는 <b>자기 {@code TransactionTemplate} 에 RC 를 직접 걸고</b>
 * 어댑터 SQL 만 부른다 — {@code ExpireJobConfig} 를 아예 거치지 않는다(그쪽은 storage 모듈이라
 * batch 를 볼 수도 없다). 이 저장소는 인덱스 쪽에서 같은 구멍을 발견하고
 * {@code scansNothingWhenNothingExpires} 를 따로 만들었는데, 격리 쪽에는 그 잣대를 안 댔다.
 *
 * <p><b>설정값이 아니라 실제 커넥션을 본다.</b> {@code TransactionAttribute} 의 필드를 읽으면
 * "설정이 이렇게 적혀 있다" 까지만 확인된다. 트랜잭션 매니저가 그 지정을 커넥션까지
 * 전달하는지는 별개 문제이고, 실제로 {@code JpaTransactionManager} 는
 * {@code HibernateJpaDialect.prepareConnection} 이 켜져 있어야 전달한다.
 * 그래서 잡이 도는 중에 {@code SELECT @@transaction_isolation} 을 읽는다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=1"
})
@Import({MySqlContainerConfig.class, ExpireJobIsolationTest.CaptureIsolationConfig.class})
class ExpireJobIsolationTest {

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
        CaptureIsolationConfig.OBSERVED.clear();
    }

    @Test
    @DisplayName("만료 Step 의 커넥션이 READ COMMITTED 로 돈다")
    void expireStepRunsAtReadCommitted() throws Exception {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();

        jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());

        assertThat(CaptureIsolationConfig.OBSERVED)
                .as("잡이 한 번도 안 돌면 이 단언이 공허해진다")
                .isNotEmpty();
        assertThat(CaptureIsolationConfig.OBSERVED)
                .as("REPEATABLE READ 로 되돌아가면 만료가 도는 동안 신규 발급 INSERT 가 "
                        + "오류 1205 로 죽는다. docs/12 §3·§4 참조")
                .containsOnly("READ-COMMITTED");
    }

    /** 청크 트랜잭션 안에서 실제 커넥션의 격리를 읽는다. */
    @TestConfiguration
    static class CaptureIsolationConfig {

        static final List<String> OBSERVED = new CopyOnWriteArrayList<>();

        @Bean
        static BeanPostProcessor captureIsolation(JdbcClient jdbcClient) {
            return ExpirationProxies.decorating((real, method, args) -> {
                if ("expireBatch".equals(method.getName())) {
                    // JdbcClient 는 진행 중인 트랜잭션의 커넥션을 쓰므로 잡의 실제 격리가 나온다.
                    OBSERVED.add(jdbcClient.sql("SELECT @@transaction_isolation")
                            .query(String.class)
                            .single());
                }
                return ExpirationProxies.callThrough(real, method, args);
            });
        }
    }
}
