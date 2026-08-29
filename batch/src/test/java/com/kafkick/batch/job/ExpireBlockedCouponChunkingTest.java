// 막힌 회차가 있는 채로 청크가 이어질 때를 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.FixedClock;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>이 티켓의 논거 둘은 청크가 2개 이상일 때만 뜻이 있다.</b>
 *
 * <ul>
 *   <li><b>막힘을 청크와 무관하게 정의해야 한다</b> — 청크 기준으로 정의하면 제외한 만큼
 *       {@code LIMIT} 자리가 비어 다른 행이 창 안으로 들어온다. 청크가 하나면 <b>빌 자리가
 *       애초에 없어서</b> 이 논거를 지나가지 않는다.</li>
 *   <li><b>한 번 구한 목록이 뒤 청크에서도 유효하다</b> — 뒤 청크가 없으면 확인할 것이 없다.</li>
 * </ul>
 *
 * <p>{@link ExpireBlockedCouponIsolationTest} 는 {@code chunk-size=100} 에 발급건이 서너 건
 * 이라 <b>성한 회차가 언제나 청크 하나로 끝난다.</b> 그래서 그 축을 여기서 따로 잰다 —
 * 같은 클래스에 못 넣는 것은 {@code chunk-size} 가 클래스 단위 프로퍼티라서다.
 *
 * <p><b>부등식이 청크를 이어가며 유지되는지가 진짜 대상이다.</b> 성한 회차는
 * {@code Σ(청크별 만료 수) ≤ 대기 전체 ≤ active_count} 여야 하고, 이것이 깨지면
 * {@code RELEASE_STOCK} 이 걸러 {@code STOCK_UNDERFLOW} 로 잡이 죽는다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 ExpirationRepository 프록시에 countPending 을 부른다 —
        // 백그라운드 틱이 호출 기록에 끼면 재현 불가로 빨개진다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        // 한 건씩 넘긴다 — 성한 회차 3건이 세 청크로 갈린다.
        "batch.expire.chunk-size=1"
})
@Import({MySqlContainerConfig.class, ExpireBlockedCouponChunkingTest.FixedClockConfig.class, ExpireBlockedCouponChunkingTest.CountingConfig.class})
class ExpireBlockedCouponChunkingTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 고정한 "지금". 태스클릿이 asOf 를 이 값과 견주므로 벽시계면 실행 날짜에 딸린다. */
    private static final LocalDateTime NOW = AS_OF.plusDays(1);

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
        CountingConfig.reset();
    }

    @Test
    @DisplayName("청크가 이어져도 막힌 회차는 계속 빠지고 성한 회차는 다 넘어간다")
    void keepsExclusionStableAcrossChunks() throws Exception {
        long broken = seed.newCoupon();
        long brokenFirst = expiring();
        long brokenSecond = expiring();
        seed.overwriteStock(1);

        long healthy = seed.newCoupon();
        long healthyFirst = expiring();
        long healthySecond = expiring();
        long healthyThird = expiring();
        seed.overwriteStock(3);

        JobExecution execution = launch();

        assertThat(execution.getStatus())
                .as("부등식이 청크를 이어가며 깨지면 STOCK_UNDERFLOW 로 FAILED 가 된다")
                .isEqualTo(BatchStatus.COMPLETED);

        assertThat(statusOf(healthyFirst)).isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThat(statusOf(healthySecond)).isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThat(statusOf(healthyThird))
                .as("**세 번째 청크까지 이어져야 여기까지 온다.** 목록이 뒤 청크에서 무효해지면 "
                        + "여기서 막힌 회차가 창 안으로 들어와 잡이 죽는다")
                .isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThat(activeCountOf(healthy))
                .as("세 청크가 각각 1 씩 되돌려 3 → 0")
                .isZero();

        assertThat(statusOf(brokenFirst)).isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(statusOf(brokenSecond))
                .as("제외한 만큼 LIMIT 자리가 비어도 이 회차가 밀려 들어오면 안 된다")
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(activeCountOf(broken)).isEqualTo(1);
    }

    /**
     * <b>실행당 한 번이라는 것이 계약이다.</b> 이 질의는 남은 대기 전체를 회차별로 묶으므로
     * 비용이 대기 건수에 비례한다 — 청크마다 부르면 그것이 청크 수만큼 곱해진다.
     *
     * <p>청크 수를 세어 두는 것이 이 단언의 검출력이다. 청크가 하나뿐이면
     * <i>"한 번 불렀다"</i> 가 저절로 참이라 아무것도 안 지킨다.
     */
    @Test
    @DisplayName("청크가 여러 번 돌아도 제외 목록은 한 번만 구한다")
    void computesExclusionOncePerExecution() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        expiring();
        seed.overwriteStock(3);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(CountingConfig.expireBatchCalls())
                .as("한 건씩 넘기고 마지막에 **후보** 0 을 받아 끝난다(그 청크는 expireBatch 를 부르지 않는다) — 청크가 여럿이어야 아래가 뜻을 갖는다")
                .isGreaterThan(1);
        assertThat(CountingConfig.blockedCouponsCalls())
                .as("청크마다 부르면 남은 대기 전체를 매번 훑는다")
                .isEqualTo(1);
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

    private int activeCountOf(long couponId) {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", couponId)
                .query(Integer.class)
                .single();
    }

    /** 호출만 센다. 결과는 실제 저장소 그대로 흘려보낸다. */
    @TestConfiguration
    static class CountingConfig {

        private static final AtomicInteger BLOCKED = new AtomicInteger();
        private static final AtomicInteger EXPIRE = new AtomicInteger();

        static void reset() {
            BLOCKED.set(0);
            EXPIRE.set(0);
        }

        static int blockedCouponsCalls() {
            return BLOCKED.get();
        }

        static int expireBatchCalls() {
            return EXPIRE.get();
        }

        @Bean
        static BeanPostProcessor countExpirationCalls() {
            return ExpirationProxies.decorating((real, method, args) -> {
                if ("blockedCoupons".equals(method.getName())) {
                    BLOCKED.incrementAndGet();
                } else if ("expireBatch".equals(method.getName())) {
                    EXPIRE.incrementAndGet();
                }
                return ExpirationProxies.callThrough(real, method, args);
            });
        }
    }

    /**
     * <b>시계를 고정한다.</b> 태스클릿은 {@code asOf} 가 현재보다 미래면
     * {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는다. 벽시계로 두면 이 테스트가 재는 축과 무관한
     * 이유(<b>실행하는 날짜</b>)로 결과가 갈린다.
     *
     * <p>운영 {@code TimeConfig} 가 {@code systemUTC} 라는 것은 {@link FixedClock} 이 진다.
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
