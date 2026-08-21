// 재고가 어긋난 회차 하나가 어디까지 막는지 확인합니다.
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>재고가 어긋난 회차 하나가 얼마나 넓게 막는지 잰다.</b>
 *
 * <p>{@code ExpireJobTest.failWhenStockUnderflows} 는 {@code chunk-size=1} 이라
 * <i>"죽은 청크만 되돌아온다"</i> 까지만 보여 준다. 운영 기본값은 <b>1000</b> 이고,
 * 그 값에서는 오염 회차 하나가 <b>같은 청크에 실린 남의 회차까지 함께 되돌린다.</b>
 * 그 크기를 코드로 못 박아 두지 않으면 <i>"한 회차 문제"</i> 로 읽힌다.
 *
 * <p><b>그리고 다음 주기도 같은 자리에서 죽는다.</b> 진도({@code afterId})는 실행 사이로
 * 안 넘어가므로({@code ExpireJobRestartTest}) 다시 {@code id > 0} 부터 훑다가 같은 회차에
 * 도달한다 — <b>그 뒤 id 의 만료가 영구히 밀린다.</b> 만료 누락은 검증 finding 이 아니라서
 * (설계상 관측 지표로 뺐다) 검증 배치도 이것을 안 잡아 준다.
 *
 * <p>지금은 사람이 재고를 손보는 것 말고 방법이 없고, 알림이 그 사실을 아는 유일한 통로다.
 * 이 테스트는 그 대가를 기록해 둔다 — 회차 단위 격리(오염 회차만 건너뛰고 나머지는 진행)를
 * 도입하는 날 이 단언이 뒤집히면서 도입 사실을 알린다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=100"
})
@Import(MySqlContainerConfig.class)
class ExpireUnderflowBlastRadiusTest {

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
    @DisplayName("오염 회차 하나가 같은 청크의 다른 회차까지 되돌리고, 다음 주기도 같은 자리에서 죽는다")
    void oneBrokenCouponBlocksTheWholeChunkAndKeepsBlocking() throws Exception {
        long broken = seed.newCoupon();
        long brokenIssuance = expiring();
        expiring();
        seed.overwriteStock(1);

        long healthy = seed.newCoupon();
        long healthyIssuance = expiring();
        seed.overwriteStock(5);

        JobExecution first = launch();

        assertThat(first.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(statusOf(brokenIssuance))
                .as("오염 회차의 건은 되돌아온다")
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(statusOf(healthyIssuance))
                .as("**멀쩡한 회차 %d 의 건까지 함께 되돌아온다** — 같은 청크라서다. "
                        + "chunk-size 가 1000 이면 그 안의 모든 회차가 여기 걸린다", healthy)
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(activeCountOf(healthy))
                .as("멀쩡한 회차의 재고도 안 돌아왔다")
                .isEqualTo(5);

        // 진도가 실행 사이로 안 넘어가므로 다음 주기도 id > 0 부터 훑다 같은 자리에 닿는다.
        JobExecution second = launchAt(AS_OF.plusMinutes(5));

        assertThat(second.getStatus())
                .as("사람이 재고를 손볼 때까지 매 주기 같은 자리에서 죽는다")
                .isEqualTo(BatchStatus.FAILED);
        assertThat(statusOf(healthyIssuance))
                .as("그동안 멀쩡한 회차의 만료가 영구히 밀린다. "
                        + "만료 누락은 검증 finding 이 아니라 검증도 안 잡아 준다")
                .isEqualTo(IssuanceStatus.ISSUED.name());

        assertThat(broken).as("두 회차가 실제로 갈렸어야 이 시나리오다").isNotEqualTo(healthy);
    }

    /** 기한이 지난 발급건 하나. 현재 회차에 붙는다. */
    private long expiring() {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();
        return id;
    }

    private JobExecution launch() throws Exception {
        return launchAt(AS_OF);
    }

    private JobExecution launchAt(LocalDateTime asOf) throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", asOf)
                .toJobParameters());
    }

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int activeCountOf(long couponId) {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", couponId)
                .query(Integer.class)
                .single();
    }
}
