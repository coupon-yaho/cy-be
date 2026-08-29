// 재고가 어긋난 회차가 나머지 만료를 막지 않는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
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

import com.kafkick.batch.config.FixedClock;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * <b>이 클래스는 뒤집힌 계약을 지킨다.</b> 예전 이름은 {@code ExpireUnderflowBlastRadiusTest}
 * 였고, 오염 회차 하나가 <b>같은 청크의 남의 회차까지 되돌리고 다음 주기도 같은 자리에서
 * 죽는다</b> 를 단언했다. 그 javadoc 이 <i>"회차 단위 격리를 도입하는 날 이 단언이
 * 뒤집히면서 도입 사실을 알린다"</i> 고 예고해 뒀고, 실제로 그날 빨간불로 알려 줬다.
 *
 * <p><b>왜 뒤집었나.</b> 설계가 정한 것이다.
 *
 * <blockquote>
 * 데이터가 틀렸다는 판정이 나와도 배치는 정상 종료다.<br>
 * 배치가 실패했다고 할 때는 <b>판정을 내지 못한 경우</b>뿐이다.<br>
 * 둘을 같은 알람으로 묶으면 서버를 고칠 상황과 데이터를 볼 상황이 구분되지 않는다.
 * </blockquote>
 *
 * <p>재고가 어긋난 회차는 <b>판정</b>이다 — 우리가 깨뜨린 것이 아니라 이미 어긋나 있었다.
 * 그것으로 배치를 죽이면, 오염 회차 하나 때문에 그 뒤 id 의 만료가 <b>영구히</b> 밀린다.
 * 진도는 JobInstance 안에서만 살고 주기마다 새 인스턴스라, 하루 288번 같은 자리에서 죽는다.
 * 그리고 만료 누락은 검증 finding 이 아니라 <b>검증도 안 잡아 준다.</b>
 *
 * <p>이 프로젝트는 그 상태가 존재한다고 <b>전제한다</b> — CORRUPT 스키마가
 * {@code ck_stock_range} 를 일부러 떼고 오염 유형이 {@code active_count} 를 흔든다.
 * 검증용 DB 를 보게 띄운 배치는 확실히 이 경로에 들어간다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=100"
})
@Import({MySqlContainerConfig.class,
        ExpireBlockedCouponIsolationTest.FixedClockConfig.class})
class ExpireBlockedCouponIsolationTest {

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
    }

    /**
     * <b>같은 청크에 오염 회차와 성한 회차가 함께 실려도 성한 쪽은 넘어간다.</b>
     * 운영 {@code chunk-size} 는 1000 이라, 예전에는 그 안의 모든 회차가 함께 되돌아갔다.
     */
    @Test
    @DisplayName("오염 회차는 건너뛰고 나머지는 넘긴다 — 배치는 정상 종료다")
    void skipsBrokenCouponAndExpiresTheRest() throws Exception {
        long broken = seed.newCoupon();
        long brokenIssuance = expiring();
        expiring();
        seed.overwriteStock(1);

        long healthy = seed.newCoupon();
        long healthyIssuance = expiring();
        seed.overwriteStock(5);

        JobExecution execution = launch(AS_OF);

        assertThat(execution.getStatus())
                .as("**데이터가 틀렸다는 판정은 실패가 아니다.** 배치가 실패했다고 할 때는 "
                        + "판정을 내지 못한 경우뿐이다")
                .isEqualTo(BatchStatus.COMPLETED);
        assertThat(statusOf(brokenIssuance))
                .as("오염 회차 %d 의 건은 손대지 않는다 — 재고가 대기를 못 덮는다", broken)
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(statusOf(healthyIssuance))
                .as("**여기가 예전과 갈린다.** 성한 회차 %d 의 건이 같은 청크에 실렸어도 넘어간다",
                        healthy)
                .isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThat(activeCountOf(healthy))
                .as("성한 회차의 재고는 넘긴 만큼 돌아온다")
                .isEqualTo(4);
        assertThat(activeCountOf(broken))
                .as("오염 회차의 재고는 손대지 않는다 — 부분 처리하면 0이 되어 이상이 안 보인다")
                .isEqualTo(1);
    }

    /**
     * <b>알림이 <i>"저기 보면 있다"</i> 로 사람을 보내는데 실제로 잇는 것이 없으면 안 된다.</b>
     *
     * <p>{@code ExpireSkippingBrokenCoupons} 의 안내가
     * <i>"어느 회차인지는 만료 잡의 WARN 로그에 회차 id 로 남습니다"</i> 다. 그 로그를 아무도
     * 안 잡고 있으면, 누가 로그 한 줄을 다듬는 순간 안내가 조용히 거짓이 되고 <b>그 사실은
     * 새벽 호출 때 알게 된다.</b> 이 저장소가 같은 형태의 결함을 이미 여러 번 냈다 —
     * 두 쪽을 문장으로만 이어 놓고 잇는 것이 아무것도 없던 경우다.
     *
     * <p>여기서는 로그 <b>문구</b>가 아니라 <b>알림이 약속한 내용</b>을 잡는다 — 회차 id 가
     * 실제로 찍히는가. 문구를 통째로 단언하면 표현을 다듬을 때마다 깨진다.
     */
    @Test
    @DisplayName("건너뛴 회차 id 가 WARN 로그에 남는다 — 알림 안내가 가리키는 자리다")
    void logsBlockedCouponIdsForTheAlert() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(ExpireJobConfig.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            long broken = seed.newCoupon();
            expiring();
            expiring();
            seed.overwriteStock(1);

            assertThat(launch(AS_OF).getStatus()).isEqualTo(BatchStatus.COMPLETED);

            assertThat(appender.list)
                    .filteredOn(event -> event.getLevel() == Level.WARN)
                    .as("**배치는 정상 종료다.** 그래서 이 사건을 알리는 통로가 로그와 게이지뿐이다")
                    .isNotEmpty()
                    .anySatisfy(event -> assertThat(event.getFormattedMessage())
                            .as("알림 안내가 약속한 것은 '회차 id 로 남는다' 이다")
                            .contains(String.valueOf(broken)));
        } finally {
            logger.detachAppender(appender);
        }
    }

    /**
     * <b>다음 주기가 같은 자리에서 안 죽는다.</b> 예전에는 진도가 JobInstance 안에서만 살아
     * 매 주기 {@code id > 0} 부터 훑다 같은 회차에 도달해 또 죽었다 — 사람이 재고를 손볼
     * 때까지 그 뒤 id 의 만료가 영구히 밀렸다.
     */
    @Test
    @DisplayName("다음 주기도 정상으로 끝난다 — 영구히 밀리지 않는다")
    void keepsRunningOnLaterCycles() throws Exception {
        seed.newCoupon();
        expiring();
        seed.overwriteStock(0);

        long healthy = seed.newCoupon();
        long healthyIssuance = expiring();
        // 재고를 3 으로 둔다. 1 로 두면 RELEASE_STOCK 의 하한 가드(active_count >= 차감량)가
        // 값을 0 에서 클램프해서 **이중 차감과 정상이 같은 값으로 끝난다.**
        // 3 이면 정상은 2, 이중 차감은 1 로 갈린다 — 그 차이가 아래 단언의 검출력 전부다.
        seed.overwriteStock(3);

        assertThat(launch(AS_OF).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(statusOf(healthyIssuance)).isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThat(activeCountOf(healthy)).isEqualTo(2);

        // 주기마다 asOf 가 달라 새 JobInstance 다. 진도가 없으니 id > 0 부터 다시 훑는다.
        JobExecution second = launch(AS_OF.plusMinutes(5));

        assertThat(second.getStatus())
                .as("오염 회차가 그대로 남아 있어도 다음 주기가 죽지 않는다")
                .isEqualTo(BatchStatus.COMPLETED);
        assertThat(activeCountOf(healthy))
                .as("이미 넘긴 건을 다시 집지 않는다 — 멱등성은 status='ISSUED' 조건이 지킨다. "
                        + "이중 차감이면 1 이 되어 갈린다")
                .isEqualTo(2);
    }

    /**
     * <b>오염 회차가 없으면 아무것도 안 바뀐다.</b> 제외 조건이 성한 회차까지 걸러내면
     * 만료가 통째로 멈추는데, 그 실패는 <b>정상 종료로 보인다</b> — 가장 나쁜 모양이다.
     */
    @Test
    @DisplayName("오염이 없으면 전부 넘어간다 — 제외 조건이 성한 회차를 안 건드린다")
    void expiresEverythingWhenNothingIsBroken() throws Exception {
        long coupon = seed.newCoupon();
        long first = expiring();
        long second = expiring();
        seed.overwriteStock(2);

        assertThat(launch(AS_OF).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(statusOf(first)).isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThat(statusOf(second)).isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThat(activeCountOf(coupon)).isZero();
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

    private JobExecution launch(LocalDateTime asOf) throws Exception {
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

    /**
     * <b>시계를 고정한다.</b> 태스클릿은 {@code asOf} 가 현재보다 미래면
     * {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는다. 벽시계로 두면 이 테스트가 재는 축(회차 격리)과
     * 무관한 이유(<b>실행하는 날짜</b>)로 결과가 갈린다.
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
