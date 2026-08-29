// 만료 결과 지표가 관제까지 나가는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

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
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.job.ExpirationProxies;
import com.kafkick.batch.schedule.CronSlot;
import com.kafkick.core.coupon.domain.IssuanceStatus;
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
        // **되읽기 스케줄을 끈다.** @EnableScheduling 이 살아 있어
        // (batch.scheduling.enabled 는 스케줄러 셋만 끈다) 백그라운드 틱이 손으로 부른
        // observe() 와 섞인다 — dropsGaugesWhenTheReadFails 의 누적 카운터 단언과, off()
        // 뒤 스크레이프 왕복 사이의 NaN 단언이 그때 뒤집힌다.
        //
        // 한때 주기를 상한(120초)까지 늘려 두는 것으로 뭉갰는데, 그것은 **확률을 줄인
        // 것이지 창을 닫은 것이 아니다.** 게다가 MAX_REFRESH_MILLIS 가드를 테스트 편의로
        // 쓰는 셈이라 그 가드가 무뎌진다. 이 클래스는 refresh() 를 손으로 부르므로
        // 잃는 축이 없고, 배선이 살아 있다는 것은 VerificationMetricExposureTest 의
        // expirePendingRefreshIsScheduled 가 별도 컨텍스트에서 진다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "server.port=0",
        "management.server.port=0"
})
@ExtendWith(SchemaShapeConfig.class)
@Import({MySqlContainerConfig.class, SchemaShapeConfig.class,
        ExpireMetricExposureTest.CountFailureConfig.class,
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

    /**
     * <b>이제 게이지를 채우는 것은 이 되읽기 하나다</b>(CY-421). 잡이 {@code afterJob} 에서
     * 자기 결과를 밀어 넣던 구조였는데, 그러면 값이 프로세스와 함께 죽어 만료가 일 1회가 된
     * 지금 재기동부터 다음 창까지 백로그 감시가 통째로 꺼진다.
     *
     * <p>그래서 이 클래스의 모든 테스트가 <b>잡을 돌린 뒤 {@link #observe()} 를 부른다</b> —
     * 운영에서는 60초 주기가 그 자리를 대신한다.
     */
    @Autowired
    private ExpirePendingRefresher refresher;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
        // 지표는 싱글턴이고 스프링 컨텍스트가 메서드 사이에 공유된다. record 는 순서를
        // 안 따지므로(CY-421 이 그 규칙을 지웠다 — ExpireMetricsTest.olderAsOfStillReplaces)
        // **앞 테스트의 값이 그대로 남고**, 뒤 테스트가 observe() 전에 스크레이프하면
        // 그것을 읽는다. 실패 메시지는 "게이지를 안 갈랐다" 로 보인다.
        // JUnit 은 메서드 순서를 보장하지 않으므로 재현이 안 된다.
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

        observe();

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

        observe();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_pending")).isZero();
        assertThat(metric(body, "cy_expire_blocked_pending")).isZero();
        assertThat(metric(body, "cy_expire_unexplained_pending")).isZero();
        assertThat(metric(body, "cy_expire_blocked_coupons")).isZero();
        assertThat(metric(body, "cy_expire_clean_schema"))
                .as("1 이 아니면 ExpireMetricsUnknown 의 조인 축이 죽어 밀림 감시가 조용해진다")
                .isEqualTo(1.0);
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

        observe();

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
     * <p>{@code asOf} 를 미래로 주면 잡은 {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는다. 그 미래
     * 컷으로 세면 <b>기한이 남은 발급건이 전부</b> 대기로 잡혀 수백만이 나가고, 알림은 그것을
     * <i>"배치가 일을 안 한다"</i> 로 읽는다. 사람이 볼 곳은 서버가 아니라 자기가 친
     * 파라미터인데 말이다.
     *
     * <p><b>새 계약에서는 그 방어가 구조로 온다</b>(CY-421). 되읽기는 <b>성공한 실행만</b>
     * 읽으므로 믿을 수 없는 {@code asOf} 를 애초에 손에 넣지 못한다 — 한때 리스너가 미래
     * 여부를 직접 판정했는데, 그 판정을 지워도 아무도 모르는 자리였다. 그 축을 여기서 잰다.
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

        observe();

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
     * <p>{@code CleanSchemaGuard} 가 {@code beforeJob} 에서 세우면 잡이 실패로 끝난다.
     * 그때 제외 목록을 못 읽은 것을 <i>"막힌 회차가 없다"</i> 로 읽으면, 오염 DB 의 남은
     * 대기가 <b>전부</b> {@code unexplained} 로 나간다 —
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

        // **먼저 CLEAN 으로 한 번 성공시킨다.** 성공한 실행이 하나도 없으면 되읽기는
        // 오염 스키마 가드를 안 지나고도 NaN 을 내므로, 이 테스트가 "오염 스키마를 보고
        // 있다" 가 아니라 "성공한 실행이 없다" 를 재게 된다 — 그 순서로는 가드를 지워도
        // 초록이었다.
        JobExecution warmUp = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF.minusDays(1))
                .toJobParameters());
        assertThat(warmUp.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        observe();
        assertThat(metric(ActuatorProbe.get(managementPort, "/actuator/prometheus").body(),
                "cy_expire_pending"))
                .as("전제 — 오염으로 바꾸기 전에 **값이** 있어야 한다(NaN 이 아니라)")
                .isZero();

        SchemaShapeConfig.pretendCorrupt();
        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);

        observe();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_unexplained_pending"))
                .as("여기서 2.0 이 나오면 접속 설정 사고가 '서버를 봐라' critical 로 나간다")
                .isNaN();
        // **위 정상셋 단언과 짝이다.** 한쪽만 두면 늘 같은 값을 내는 배선도 통과한다.
        assertThat(metric(body, "cy_expire_clean_schema"))
                .as("1 이면 오염셋 기동 내내 ExpireMetricsUnknown 이 상시 warning 이 된다")
                .isZero();
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
     * 관용한다 — 그 실행은 <b>정상 종료</b>하므로 되읽기가 그것을 마지막 성공으로 집고
     * 값을 낸다. 여기가 {@code NaN} 이면 정상 주기가 감시를 끄는 것이고, 누락 알림의
     * {@code for} 타이머가 리셋되어 감시가 조용히 꺼진다 — 한때 실제로 그 상태였다.
     *
     * <p>위 테스트의 짝이다. 막는 쪽만 보면 <b>항상 모른다고 하는 되읽기</b>도 통과한다.
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

        observe();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_pending"))
                .as("**NaN 이면 정상 주기가 감시를 끈다.** 조기 발화를 관용한 실행도 성공이다")
                .isZero();
        assertThat(metric(body, "cy_expire_blocked_pending")).isZero();
        assertThat(metric(body, "cy_expire_unexplained_pending")).isZero();
        assertThat(metric(body, "cy_expire_blocked_coupons")).isZero();
    }

    /**
     * <b>실패한 과거 실행이 방금 끝난 주기의 값을 지우면 안 된다.</b>
     *
     * <p>되읽기는 <b>성공한 실행만</b> 본다. 그 필터가 없으면 방금 실패한 실행이
     * <i>가장 최신 {@code asOf}</i> 라는 이유로 뽑혀, 판정할 재료가 없다는 이유로
     * 게이지가 통째로 {@code NaN} 이 된다 — <b>실패 한 번이 감시를 끄는</b> 모양이다.
     * 한때 리스너가 실패마다 <i>"모른다"</i> 를 내보내서 같은 우회로가 있었다.
     *
     * <p>그 사이 {@code ExpireLeavesWorkBehind} 는 {@code NaN > 0} 이 거짓이라
     * <b>울릴 수 없다</b> — 그래서 이 축이 감시의 유무를 가른다.
     */
    @Test
    @DisplayName("실패한 실행은 asOf 가 더 최신이어도 값을 못 지운다")
    void ignoresFailedRunsEvenWhenTheirAsOfIsNewer() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);
        observe();
        String healthy = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(healthy, "cy_expire_blocked_coupons")).isEqualTo(1.0);

        // **더 최신 asOf** 로 친 손 트리거가 미래 컷이라 태스클릿에서 죽는다.
        // 과거 asOf 로 두면 정렬만으로도 걸러져 STATUS 필터를 안 지나간다 — 돌연변이로 확인했다.
        // 오염 스키마로 죽이지 않는 이유는 그쪽이 **되읽기의 다른 가드**(오염 스키마)에도
        // 걸려, 이 테스트가 재려는 축을 안 지나가기 때문이다.
        JobExecution failed = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF.plusYears(1))
                .toJobParameters());
        assertThat(failed.getStatus()).isEqualTo(BatchStatus.FAILED);

        observe();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_blocked_coupons"))
                .as("**성공한 실행의 결과가 살아 있어야 한다.** 여기가 NaN 이면 실패한 손 "
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

    /**
     * <b>되읽기는 가장 나중에 끝난 성공 실행을 고른다.</b>
     *
     * <p>한때 {@code asOf} 로 골랐다. 근거는 <i>"밀린 만료를 따라잡으려고 과거 {@code asOf}
     * 로 손 트리거를 치는 것이 권장 절차"</i> 였는데 — <b>만료를 손으로 띄우는 경로가 이
     * 저장소에 없다.</b> 없는 시나리오 때문에 문자열 정렬과 {@code PARAMETER_TYPE} 전제를
     * 지고 있었고, 그 정렬이 {@code record} 의 순서 규칙과 맞물려 <b>게이지를 영구히
     * 얼리는 문</b>이 됐다(7일 창이 앞을 자르면 최대 {@code asOf} 가 뒤로 간다).
     *
     * <p>⚠️ <b>만료 손 트리거가 생기는 티켓은 이 테스트를 다시 봐야 한다.</b> 그때는 과거
     * {@code asOf} 실행이 나중에 끝날 수 있고, 이 계약대로면 게이지가 <b>더 좁은 창의 더
     * 작은 값</b>을 낸다 — 관제는 그것을 <i>"밀린 것이 없다"</i> 로 읽는다.
     * 크론만 있는 지금은 두 순서가 같아 그 창이 없다.
     */
    @Test
    @DisplayName("나중에 끝난 성공 실행이 이긴다 — 크론만 있는 지금은 곧 최신 asOf 다")
    void picksTheRunThatFinishedLast() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        // 과거 asOf 로 먼저 돌린다. 그 컷에는 아무것도 안 걸려 0 을 낸다.
        JobExecution older = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF.minusDays(10))
                .toJobParameters());
        assertThat(older.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // 그다음 주기 실행. 나중에 끝났으므로 이쪽이 이겨야 한다.
        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        observe();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_blocked_coupons"))
                .as("나중에 끝난 실행을 안 집으면 게이지가 옛 창의 값에 머문다")
                .isEqualTo(1.0);
        assertThat(metric(body, "cy_expire_pending")).isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_blocked_pending")).isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_unexplained_pending")).isZero();
        assertThat(metric(body, "cy_expire_measured_at_seconds"))
                .as("어느 시점의 사실인지가 함께 움직여야 한다")
                .isEqualTo(AS_OF.toEpochSecond(ZoneOffset.UTC));

        // ── 여기까지는 END_TIME 정렬을 재지 못한다 ────────────────────────────
        //
        // 두 실행이 수백 ms 안에 끝나 **END_TIME 순서와 id 순서가 같다.** 그래서
        // `ORDER BY e.END_TIME DESC` 를 통째로 지워도 타이브레이커(JOB_EXECUTION_ID DESC)
        // 가 같은 행을 집어 위 단언이 전부 초록이다 — 테스트 이름이 주장하는 축이
        // 안 걸린다.
        //
        // 두 축을 **반대로 벌려** 다시 잰다. id 가 큰 주기 실행을 먼저 끝난 것으로
        // 되돌리면, END_TIME 정렬은 older(과거 asOf · 대상 0건)를 집어야 한다.
        jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET END_TIME = DATE_SUB(END_TIME, "
                        + "INTERVAL 1 HOUR) WHERE JOB_EXECUTION_ID > :id")
                .param("id", older.getId())
                .update();

        observe();

        String reordered = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(reordered, "cy_expire_measured_at_seconds"))
                .as("END_TIME 항을 지우면 id 순으로 최신 실행을 집어 이 단언이 깨진다")
                .isEqualTo(AS_OF.minusDays(10).toEpochSecond(ZoneOffset.UTC));
        assertThat(metric(reordered, "cy_expire_blocked_coupons"))
                .as("과거 asOf 실행은 아무것도 안 건너뛰었다")
                .isZero();
    }


    /**
     * <b>세다가 실패하면 직전 값을 들고 있으면 안 된다.</b>
     *
     * <p>되읽기가 실패했는데 게이지가 그대로면 관제는 그것을 <b>지금 상태</b>로 읽는다 —
     * 백로그 축에서 그 오해는 <i>"밀린 것이 없다"</i> 이고, 그 알림은 영원히 조용해진다.
     * 0 을 내는 것은 더 나쁘다. 그래서 {@code NaN}(모름) 으로 떨어뜨린다.
     *
     * <p>잡은 이 실패에 <b>영향을 안 받는다.</b> 관측이 잡 밖으로 나온 것이 CY-421 의 요지다 —
     * 한때 이 실패가 {@code afterJob} 안에 있어서, 그 자리를 조금만 잘못 만지면
     * <b>만료는 다 해 놓고 지표 때문에 빨간불</b>이 났다.
     */
    @Test
    @DisplayName("되읽기가 세다 실패하면 게이지를 모름으로 떨어뜨린다")
    void dropsGaugesWhenTheReadFails() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);
        observe();
        assertThat(metric(ActuatorProbe.get(managementPort, "/actuator/prometheus").body(),
                "cy_expire_blocked_coupons"))
                .as("전제 — 떨어뜨릴 값이 먼저 있어야 한다")
                .isEqualTo(1.0);

        CountFailureConfig.on();
        try {
            observe();
        } finally {
            CountFailureConfig.off();
        }

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_unexplained_pending"))
                .as("직전 값을 들고 있으면 관제가 그것을 지금 상태로 읽는다")
                .isNaN();
        assertThat(metric(body, "cy_expire_pending")).isNaN();
        assertThat(metric(body, "cy_expire_blocked_pending")).isNaN();
        assertThat(metric(body, "cy_expire_blocked_coupons")).isNaN();
        assertThat(metric(body, "cy_expire_refresh_failures_total"))
                .as("로그는 감시 수단이 아니다 — 되읽기 실패는 지표로도 나가야 한다")
                .isEqualTo(1.0);
    }

    /**
     * <b>{@code blocked} 는 그 실행에 대한 사실이지 지금 재고 상태가 아니다.</b>
     *
     * <p>되읽기가 {@code blockedCoupons(asOf)} 를 다시 부르면, 04:10 에 어긋났던 재고를
     * 10:00 에 고치는 순간 그 몫이 {@code blocked} 에서 {@code unexplained} 로 옮겨 간다 —
     * <b>데이터를 고쳤더니 서버 critical 이 뜬다.</b> {@code ExpireMetrics} 가 세운
     * <i>"서버를 고칠 상황과 데이터를 볼 상황을 같은 알람으로 묶지 않는다"</i> 를 정면으로
     * 어기고, 만료가 일 1회라 그 오탐이 <b>최대 하루</b> 간다.
     *
     * <p>그래서 목록은 {@code BATCH_STEP_EXECUTION_CONTEXT} 에서 그대로 가져오고, 다시 재는
     * 것은 {@code countPending} 하나다. 그 결정을 무는 것이 이 테스트다.
     */
    @Test
    @DisplayName("실행 뒤에 재고를 고쳐도 막힌 몫은 그 실행의 값 그대로다")
    void keepsBlockedAsTheRunSawIt() throws Exception {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // 사람이 재고를 고쳤다. 이제 blockedCoupons(asOf) 는 이 회차를 안 낸다.
        seed.overwriteStock(5);
        observe();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_expire_blocked_pending"))
                .as("다시 계산하면 여기가 0 이 되고, 그 2건이 unexplained 로 옮겨 간다")
                .isEqualTo(2.0);
        assertThat(metric(body, "cy_expire_unexplained_pending"))
                .as("**데이터를 고친 것이 서버 critical 로 나가면 안 된다.** 남은 2건은 "
                        + "다음 창에서 걷히고, 그 사이는 ExpireSkippingBrokenCoupons 의 몫이다")
                .isZero();
        assertThat(metric(body, "cy_expire_blocked_coupons")).isEqualTo(1.0);
    }

    /** {@code countPending} 만 던지게 한다. 나머지는 실제 저장소 그대로다. */
    @TestConfiguration
    static class CountFailureConfig {

        private static volatile boolean fail;

        static void on() {
            fail = true;
        }

        static void off() {
            fail = false;
        }

        @Bean
        static BeanPostProcessor failCountPending() {
            return ExpirationProxies.decorating((real, method, args) -> {
                if (fail && "countPending".equals(method.getName())) {
                    throw new IllegalStateException("관측 질의가 끊겼다");
                }
                return ExpirationProxies.callThrough(real, method, args);
            });
        }
    }

    /** 어떤 asOf 로도 이미 기한이 지난 발급건. 조기 발화 테스트가 쓴다. */
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

    /**
     * <b>관측 한 바퀴.</b> 운영의 {@code @Scheduled} 주기를 테스트가 손으로 돌린다 —
     * 주기를 기다리면 이 클래스가 시간에 기대게 되고, 재는 축은 주기가 아니라 <b>값</b>이다.
     */
    private void observe() {
        refresher.refresh();
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
