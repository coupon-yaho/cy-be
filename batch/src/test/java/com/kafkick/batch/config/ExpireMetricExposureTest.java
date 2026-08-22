// 만료 결과 지표가 관제까지 나가는지 확인합니다.
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
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.schedule.CronSlot;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>잡의 생사만으로는 "성공했는데 아무것도 안 했다" 를 못 잡는다.</b> 기존 알림 셋이 전부
 * 그 축이고, 셋 다 통과하면서 기한 지난 발급건이 계속 쌓이는 상태가 있다.
 *
 * <p><b>게이지를 가르는 것이 이 테스트의 축이다.</b> 막힌 회차의 몫은 설계상 계속
 * 남는다. 합쳐서 알림을 걸면 사람이 재고를 고칠 때까지 24시간 울리고, 그 알림은 곧 무시된다.
 * 갈라야 <i>"배치가 일을 안 한다"</i>(서버를 본다)와 <i>"데이터가 어긋나 있다"</i>(데이터를
 * 본다)가 서로 다른 알림이 된다 — 설계가 정한 그 구분이다.
 *
 * <p><b>노출까지 본다.</b> 값을 게이지에 넣는 것과 그것이 {@code /actuator/prometheus} 로
 * 나가는 것은 다른 일이다. 노출 목록에서 빠지면 관제에서 영원히 안 뜨는데, 알림은 안 울리는
 * 것이 정상처럼 보여서 아무도 눈치채지 못한다.
 *
 * <p><b>{@link BatchMetricExposureTest} 와 책임을 가른다.</b> 저쪽은 <i>규칙 파일이 부르는
 * 이름이 실제로 나가는가</i>(존재), 이쪽은 <i>그 값이 맞는가</i>. 둘이 {@link ActuatorProbe} 와
 * {@code spring.config.location} 관용구를 함께 쓴다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 노출 목록을 여기 복붙하면 실제 설정에서 prometheus 가 빠져도 이 테스트는 초록으로
        // 지나간다 — 관제가 비는 것을 못 잡는다는 뜻이다. BatchMetricExposureTest 와 같은 자리.
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=100",
        "server.port=0",
        "management.server.port=0"
})
@ExtendWith(SchemaShapeConfig.class)
@Import({MySqlContainerConfig.class, SchemaShapeConfig.class,
        ExpireMetricExposureTest.FixedClockConfig.class})
class ExpireMetricExposureTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 고정한 "지금". 조기 발화 관용 폭을 재려면 두 값이 모두 테스트 것이어야 한다. */
    private static final LocalDateTime NOW = AS_OF.plusMinutes(3);

    @LocalManagementPort
    private int managementPort;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private ExpireMetrics metrics;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
        // 지표는 싱글턴이고 스프링 컨텍스트가 메서드 사이에 공유된다. 그리고 record 는
        // **더 오래된 asOf 의 결과를 버린다** — 여기서 안 비우면 벽시계 asOf 를 쓰는 테스트가
        // 먼저 도는 순서에서 뒤 테스트의 record 가 통째로 거부되고, 실패 메시지는
        // "게이지를 안 갈랐다" 로 보인다. JUnit 은 메서드 순서를 보장하지 않는다.
        metrics.markUnknown();
    }

    @Test
    @DisplayName("막힌 몫과 설명 안 되는 몫이 갈려서 나간다")
    void exposesBlockedAndUnexplainedPendingSeparately() throws Exception {
        // 오염 회차 — 대기 2건에 재고 0. 설계상 계속 남는다.
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        // 성한 회차 — 이번 실행이 넘긴다.
        seed.newCoupon();
        expiring();
        seed.overwriteStock(1);

        JobExecution execution = launch();
        assertThat(execution.getStatus())
                .as("데이터가 틀렸다는 판정은 실패가 아니다")
                .isEqualTo(BatchStatus.COMPLETED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_pending"))
                .as("오염 회차의 2건이 남는다")
                .isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_blocked_pending"))
                .as("**그 2건은 설명이 되는 몫이다.** 이것을 안 가르면 누락 알림이 "
                        + "사람이 재고를 고칠 때까지 24시간 울린다")
                .isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_unexplained_pending"))
                .as("**배치는 제 몫을 다 했다.** 남은 2건은 전부 설명이 되는 몫이라 "
                        + "누락 알림은 조용해야 한다")
                .isZero();
        assertThat(metric(body, "cy_expire_blocked_coupons"))
                .as("데이터를 봐야 한다는 알림은 이 값으로 건다")
                .isEqualTo(1.0);
    }

    /**
     * <b>성한 데이터에서는 셋 다 0 이다.</b> 제외 조건이 성한 회차까지 걸러내면 만료가 통째로
     * 멈추는데, 그 실패는 <b>정상 종료로 보인다</b> — 지표가 그것을 드러내는 유일한 통로다.
     */
    @Test
    @DisplayName("오염이 없으면 남는 것도 막힌 것도 없다")
    void exposesZeroWhenNothingIsWrong() throws Exception {
        seed.newCoupon();
        expiring();
        seed.overwriteStock(1);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_pending")).isZero();
        assertThat(metric(body, "cy_expire_blocked_pending")).isZero();
        assertThat(metric(body, "cy_expire_unexplained_pending")).isZero();
        assertThat(metric(body, "cy_expire_blocked_coupons")).isZero();
    }

    /**
     * <b>알림이 보는 것은 이 시계열 하나다.</b> 게이지 둘을 내보내고 관제가
     * {@code pending - blocked_pending} 을 빼면, 두 값을 따로 {@code set} 하는 사이에
     * 스크레이프가 끼어 <b>한쪽만 새 값인 샘플</b>이 나온다. 한 문장에서 세어 여기서 빼야
     * 그 틈이 없다 — 그 성질을 값으로 확인한다.
     */
    @Test
    @DisplayName("설명 안 되는 몫이 뺄셈 없이 한 시계열로 나간다")
    void exposesUnexplainedPendingAsItsOwnSeries() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        seed.newCoupon();
        long stuck = expiring();
        seed.overwriteStock(1);
        // 성한 회차의 건을 창 밖으로 밀어 만료가 못 넘기게 한다 — 진짜 "밀린 만료" 다.
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF.plusYears(1))
                .param("id", stuck)
                .update();

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_pending")).isEqualTo(3.0);
        assertThat(metric(body, "cy_expire_blocked_pending")).isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_unexplained_pending"))
                .as("관제가 빼지 않는다. 여기서 이미 뺀 값이 나가야 스크레이프 틈이 없다")
                .isEqualTo(1.0);
        assertThat(metric(body, "cy_expire_blocked_coupons")).isEqualTo(1.0);
    }

    /**
     * <b>파라미터를 잘못 친 사건이 "서버를 봐라" critical 로 나가면 안 된다.</b>
     *
     * <p>{@code asOf} 를 미래로 주면 잡은 {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는데,
     * {@code afterJob} 은 그래도 돈다. 그 미래 컷으로 세면 <b>기한이 남은 발급건이 전부</b>
     * 대기로 잡혀 수백만이 나가고, 알림은 그것을 <i>"배치가 일을 안 한다"</i> 로 읽는다.
     * 사람이 볼 곳은 서버가 아니라 자기가 친 파라미터인데 말이다.
     *
     * <p>그래서 믿을 수 없는 {@code asOf} 로는 세지 않고 <b>{@code NaN}(모름)</b> 으로 둔다.
     * 잡이 죽은 것 자체는 {@code BatchJobFailed} 가 이미 알린다.
     */
    @Test
    @DisplayName("asOf 가 미래면 세지 않고 '모름' 으로 남긴다")
    void keepsGaugesUnknownWhenAsOfIsInTheFuture() throws Exception {
        seed.newCoupon();
        alive();
        alive();
        seed.overwriteStock(2);

        JobExecution execution = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF.plusYears(1))
                .toJobParameters());

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_pending"))
                .as("여기서 2.0 이 나오면 파라미터 오타가 누락 critical 로 나간다는 뜻이다")
                .isNaN();
        assertThat(metric(body, "cy_expire_unexplained_pending")).isNaN();
        assertThat(metric(body, "cy_expire_blocked_pending"))
                .as("넷을 한 스냅샷으로 묶은 것이 여기서 뜻을 갖는다 — 하나만 살아남으면 "
                        + "서로 다른 실행의 값이 섞인 샘플이 된다")
                .isNaN();
        assertThat(metric(body, "cy_expire_blocked_coupons")).isNaN();
    }

    /**
     * <b>판정할 재료가 없는 실행이 "밀린 것이 있다" 로 나가면 안 된다.</b>
     *
     * <p>{@code CleanSchemaGuard} 가 {@code beforeJob} 에서 세우면 Step 은 안 돌지만
     * {@code afterJob} 은 그대로 돈다. 그때 제외 목록을 못 읽은 것을 <i>"막힌 회차가 없다"</i>
     * 로 읽으면, 오염 DB 의 남은 대기가 <b>전부</b> {@code unexplained} 로 나간다 —
     * 오염셋에 만료 대기가 남아 있는 것은 계약상 <b>정상</b>인데도 말이다
     * ({@code docs/contract.json} 의 {@code not_verified}: <i>"만료 누락 — 별도 관측 지표"</i>).
     *
     * <p>그 알림은 critical 이고 <i>"서버를 봐야 한다"</i> 고 안내하는데, 실제로 고칠 곳은
     * 접속 설정이다. 같은 가드의 javadoc 이 <i>"고칠 곳은 데이터가 아니라 접속 설정"</i> 이라고
     * 적어 두고 지표는 정반대 버킷으로 보내는 모양이 된다. 미래 {@code asOf} 축에서 이미
     * 한 번 막은 것과 <b>같은 종류의 사고</b>다.
     */
    @Test
    @DisplayName("오염 스키마로 죽으면 세지 않고 '모름' 으로 남긴다")
    void keepsGaugesUnknownOnCorruptSchema() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(5);

        SchemaShapeConfig.pretendCorrupt();
        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_unexplained_pending"))
                .as("여기서 2.0 이 나오면 접속 설정 사고가 '서버를 봐라' critical 로 나간다")
                .isNaN();
        assertThat(metric(body, "cy_expire_pending")).isNaN();
        assertThat(metric(body, "cy_expire_blocked_pending")).isNaN();
        assertThat(metric(body, "cy_expire_blocked_coupons"))
                .as("Step 을 시작도 못 했으니 막힌 회차를 **모른다**. 0 은 '없다' 라 거짓말이다")
                .isNaN();
    }

    /**
     * <b>조기 발화한 정상 주기가 감시를 끄면 안 된다.</b>
     *
     * <p>{@code CronSlot} 은 크론이 슬롯 직전에 깨어나는 것을 관용해 <b>최대 2초 미래</b>인
     * {@code asOf} 를 만든다. 태스클릿은 그 폭을 {@link CronSlot#EARLY_FIRE_TOLERANCE} 로
     * 관용하는데 {@code afterJob} 이 엄격 비교를 하면, <b>만료는 다 하고 정상 종료했는데
     * 지표 넷이 통째로 {@code NaN}</b> 이 된다. 그러면 누락 알림의 {@code for} 타이머가
     * 리셋되어 감시가 조용히 꺼진다 — 실제로 그 상태였다.
     *
     * <p>위 테스트의 짝이다. 막는 쪽만 보면 <b>항상 모른다고 하는 리스너</b>도 통과한다.
     */
    @Test
    @DisplayName("조기 발화 관용 폭 안의 asOf 는 정상으로 센다")
    void countsNormallyWhenAsOfIsWithinEarlyFireTolerance() throws Exception {
        seed.newCoupon();
        longExpiredIssuance();
        seed.overwriteStock(1);

        // 스케줄러가 실제로 만드는 값이다. 막으면 정상 주기의 감시가 통째로 꺼진다.
        // **시계를 고정해야 이 값이 실제로 미래다.** 벽시계로 잡으면 평가 시점엔 이미
        // 과거가 되어, 관용 폭을 지워도 이 테스트가 통과한다 — 재는 축을 안 지나간다.
        JobExecution execution = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", NOW.plusSeconds(1))
                .toJobParameters());

        assertThat(execution.getStatus())
                .as("태스클릿은 이 폭을 관용한다 — 여기가 FAILED 면 계약이 한쪽만 바뀐 것이다")
                .isEqualTo(BatchStatus.COMPLETED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_pending"))
                .as("**NaN 이면 정상 주기가 감시를 끈다.** 태스클릿과 리스너가 같은 폭을 봐야 한다")
                .isZero();
        assertThat(metric(body, "cy_expire_blocked_pending")).isZero();
        assertThat(metric(body, "cy_expire_unexplained_pending")).isZero();
        assertThat(metric(body, "cy_expire_blocked_coupons")).isZero();
    }

    /**
     * <b>실패한 과거 실행이 방금 끝난 주기의 값을 지우면 안 된다.</b>
     *
     * <p>밀린 만료를 따라잡으려고 <b>과거 {@code asOf} 로 손 트리거를 치는 것</b>이 이 저장소가
     * 권하는 운영 절차다. 그 실행이 실패하면 {@code afterJob} 은 <i>"모른다"</i> 를 내보내는데,
     * 그것을 순서 없이 적용하면 <b>방금 끝난 주기의 멀쩡한 값이 지워진다.</b>
     *
     * <p>{@code record} 에만 순서를 두고 여기 안 두면 같은 우회로가 열린다 — 성공은 못 덮는데
     * 실패는 덮는 모양이다. 그 사이 {@code ExpireLeavesWorkBehind} 는 {@code NaN > 0} 이
     * 거짓이라 <b>울릴 수 없다.</b>
     */
    @Test
    @DisplayName("실패한 과거 실행이 최신 값을 못 지운다")
    void keepsNewerSnapshotWhenAnOlderRunFails() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);
        String healthy = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(healthy, "cy_expire_blocked_coupons")).isEqualTo(1.0);

        // 과거 asOf 로 친 손 트리거가 오염 스키마를 만나 시작 전에 죽는다.
        SchemaShapeConfig.pretendCorrupt();
        JobExecution stale = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF.minusDays(1))
                .toJobParameters());
        assertThat(stale.getStatus()).isEqualTo(BatchStatus.FAILED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_blocked_coupons"))
                .as("**더 최신 asOf 의 결과가 살아 있어야 한다.** 여기가 NaN 이면 실패한 손 "
                        + "트리거가 주기 실행의 감시를 끈다")
                .isEqualTo(1.0);
        assertThat(metric(body, "cy_expire_pending")).isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_blocked_pending")).isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_unexplained_pending")).isZero();
    }

    /** 기한이 남은 발급건 하나. 미래 asOf 의 컷 안에는 들어온다. */
    private long alive() {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.plusDays(30))
                .param("id", id)
                .update();
        return id;
    }

    /** 어떤 asOf 로도 이미 기한이 지난 발급건. 벽시계로 도는 테스트에 쓴다. */
    private long longExpiredIssuance() {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", LocalDateTime.of(2020, 1, 1, 0, 0))
                .param("id", id)
                .update();
        return id;
    }

    /** 프로메테우스 본문에서 게이지 값 하나를 꺼낸다. 없으면 그 자리에서 실패한다. */
    private static double metric(String body, String name) {
        return body.lines()
                .filter(line -> line.startsWith(name + " ") || line.startsWith(name + "{"))
                .map(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        name + " 가 /actuator/prometheus 에 없다. 노출 목록이나 게이지 등록을 "
                                + "확인해라. 실제로 나간 cy_* 는 " + body.lines()
                                        .filter(line -> line.startsWith("cy_"))
                                        .toList()));
    }

    private long expiring() {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();
        return id;
    }

    private JobExecution launch() throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
    }

    /**
     * <b>시계를 고정한다.</b> 조기 발화 관용 폭은 {@code asOf} 와 {@code now} 의 차로
     * 판정되므로, 한쪽이 벽시계면 <b>그 차가 실행 지연에 딸린다.</b> 둘 다 테스트가 정하는
     * 값이어야 관용 폭을 지우는 돌연변이가 잡힌다.
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
