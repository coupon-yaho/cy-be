// 검증 판정이 관제까지 나가는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.OrderUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskHolder;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code verdict = FAIL} 은 정상 종료라 알림이 하나도 안 울렸다.</b> 판정이 DB 에 남을
 * 뿐이라 누가 그 행을 조회하기 전까지 아무도 몰랐다 — 게이트 판정의 본체인데 통로가 없었다.
 *
 * <p><b>이름이 나가는 것과 값이 맞는 것은 다른 일이다.</b> {@link BatchMetricExposureTest} 는
 * 규칙 파일의 이름이 본문에 있는지만 본다 — 갱신기를 안 걸어도, 값이 늘 {@code NaN} 이어도
 * 초록이다. 여기서 <b>값</b>을 본다.
 *
 * <p><b>잡을 돌리지 않는다.</b> 지표는 잡이 미는 것이 아니라 {@code verification_runs} 를
 * 되읽어 채운다 — 검증은 사람이 손으로 드물게 돌려서, 프로세스 게이지로 두면 재배포에
 * 판정이 사라지는데 DB 에는 남아 관제와 진실이 갈리기 때문이다. 그래서 행을 직접 심고
 * 갱신기를 부른다. 그것이 운영에서 실제로 값이 만들어지는 경로다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "server.port=0",
        "management.server.port=0",
        // 테스트는 refresh() 를 손으로 부른다. 그런데 @EnableScheduling 이 살아 있어
        // 기본 60초 주기의 되읽기가 같은 DB 를 함께 읽는다. 지금은 값이 같아 아무 단언도
        // 안 뒤집히지만, "행을 심지 않은 상태에서 특정 값을 단언" 하는 케이스가 붙으면
        // 값의 출처가 둘이 되어 원인을 못 찾는 플레이크가 된다. 배선을 지키는
        // refreshIsWiredAsAScheduledTask 는 등록 여부만 보므로 주기를 늘려도 통과한다.
        //
        // 상한(MAX_REFRESH_MILLIS = 2분)까지만 올린다. 그보다 크면 생성자가 거부한다 —
        // 그 상한은 VerificationMetricsStale 이 발화 조건을 채울 수 있게 지키는 것이고,
        // 테스트 편의로 뚫으면 그 가드가 프로덕션에서만 도는 것이 된다.
        "batch.verify.metrics-refresh-ms=120000"
})
@Import(MySqlContainerConfig.class)
class VerificationMetricExposureTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @LocalManagementPort
    private int managementPort;

    @Autowired
    private VerificationRunRepository runs;

    @Autowired
    private VerificationMetricsRefresher refresher;

    @Autowired
    private VerificationMetrics metrics;

    @Autowired
    private ScheduledTaskHolder taskHolder;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ThreadPoolTaskScheduler taskScheduler;

    @Autowired
    private JdbcClient jdbcClient;

    @BeforeEach
    void setUp() throws Exception {
        jdbcClient.sql("DELETE FROM verification_runs").update();
        metrics.markUnknown();
    }

    /**
     * <b>판정이 없는 것과 통과한 것은 다르다.</b> {@code 0} 을 내면 {@code PASS} 라
     * <b>합격으로 읽혀</b> 알림이 조용해진다 — 아직 한 번도 안 돌린 데이터셋이 통과한
     * 것으로 보이는 모양이다.
     */
    @Test
    @DisplayName("닫힌 실행이 없으면 판정은 '모름' 이다")
    void reportsUnknownWhenNothingHasBeenJudged() throws Exception {
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_verification_verdict", metrics.served()))
                .as("여기가 0 이면 안 돌린 데이터셋이 '통과' 로 읽힌다")
                .isNaN();
        assertThat(metric(body, "cy_verification_findings", metrics.served())).isNaN();
    }

    @Test
    @DisplayName("PASS 는 0, 검출 건수와 함께 나간다")
    void exposesPassVerdict() throws Exception {
        closedRun(metrics.served(), VerdictType.PASS, 0, AS_OF);
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_verification_verdict", metrics.served())).isZero();
        assertThat(metric(body, "cy_verification_findings", metrics.served())).isZero();
    }

    /**
     * <b>이 값이 안 나가면 게이트 판정이 통로 없이 DB 에만 남는다.</b> 그것이 이 단계를
     * 만든 이유다.
     */
    @Test
    @DisplayName("FAIL 은 1, 검출 건수가 함께 나간다")
    void exposesFailVerdictWithFindingCount() throws Exception {
        closedRun(metrics.served(), VerdictType.FAIL, 800, AS_OF);
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_verification_verdict", metrics.served())).isEqualTo(1.0);
        assertThat(metric(body, "cy_verification_findings", metrics.served()))
                .isEqualTo(800.0);
    }

    /**
     * <b>시드가 심은 기준 행을 배치 판정으로 읽으면 안 된다.</b>
     *
     * <p>시드는 {@code verification_runs} 에 <b>완결된 과거 run</b> 을 일부러 심는다 —
     * CLEAN 은 {@code PASS}, CORRUPT 는 {@code FAIL} 과 정답 800행이다. 게이트가 쓰는
     * {@code as_of} 가 그 행에서 나오므로 없앨 수 없다.
     *
     * <p>그것을 판정으로 읽으면 결과가 정확히 뒤집힌다 — <b>검증을 한 번도 안 돌려도
     * "통과" 가 나가고</b>, CORRUPT 쪽은 알림이 영원히 안 꺼진다. 이 알림이 막으려던
     * 상태가 알림 자신에 의해 성립하는 모양이다.
     *
     * <p>{@code rejectExistingRun} 이 같은 함정을 이미 막고 있었다 — <i>"이어받으면
     * 검증기가 한 건도 못 잡아도 정답이 나온다"</i>. 그 방어가 되읽기 경로에는 없었다.
     */
    @Test
    @DisplayName("시드가 심은 행은 판정으로 안 센다")
    void ignoresSeedPlantedRuns() throws Exception {
        seedRun(metrics.served(), VerdictType.PASS, 0, AS_OF);
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_verification_verdict", metrics.served()))
                .as("여기가 0 이면 검증을 한 번도 안 돌리고 '통과' 가 나간다")
                .isNaN();
    }

    /**
     * <b>닫혔는데 판정이 없는 행은 판정이 아니다.</b> {@code verdict} 컬럼이 nullable 이라
     * 그런 행이 있을 수 있는데, {@code FAIL} 로 접으면 가짜 알림이 뜨고 {@code PASS} 로
     * 접으면 합격으로 읽힌다. 둘 다 아니라 모름이다.
     */
    @Test
    @DisplayName("판정이 비어 있는 닫힌 행은 모름이다")
    void ignoresClosedRunsWithoutVerdict() throws Exception {
        closedRun(metrics.served(), null, 0, AS_OF);
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_verification_verdict", metrics.served())).isNaN();
        // 게이지 하나만 보면 SELECT_LATEST_CLOSED 의 `verdict IS NOT NULL` 을 지워도 초록이다
        // — VerdictCode.of(null) 이 어차피 NaN 이라서다. 그런데 findings 는 그 행의 0 을
        // 그대로 내보내므로, 절이 죽으면 verdict=NaN 인데 findings=0 인 샘플이 나간다.
        // 둘을 한 덩어리로 바꾼다는 이 클래스의 전제가 깨진 상태다.
        assertThat(metric(body, "cy_verification_findings", metrics.served()))
                .as("verdict IS NOT NULL 이 빠지면 여기가 0 이 되어 verdict=NaN 과 짝이 안 맞는다")
                .isNaN();
    }

    /**
     * <b>안 닫힌 실행은 판정이 아니다.</b> 돌다 말았거나 지금 도는 중인 행을 섞으면
     * <i>"판정이 없다"</i> 와 <i>"아직 안 끝났다"</i> 가 한 값으로 뭉친다.
     */
    @Test
    @DisplayName("안 닫힌 실행은 판정으로 안 센다")
    void ignoresRunsThatNeverClosed() throws Exception {
        runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, metrics.served(), 1, AS_OF.plusMinutes(1)));
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_verification_verdict", metrics.served()))
                .as("도는 중인 실행을 판정으로 세면 '아직 모른다' 가 '통과' 로 바뀐다")
                .isNaN();
    }

    /**
     * <b>가장 최근에 닫힌 것을 본다.</b> 어제 FAIL 이 오늘 PASS 를 덮으면 고쳐 놓고도
     * 알림이 계속 울린다.
     */
    @Test
    @DisplayName("가장 최근에 닫힌 실행을 쓴다")
    void usesLatestClosedRun() throws Exception {
        closedRun(metrics.served(), VerdictType.FAIL, 3, AS_OF.minusDays(1));
        closedRun(metrics.served(), VerdictType.PASS, 0, AS_OF);
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        assertThat(metric(body, "cy_verification_verdict", metrics.served())).isZero();
        assertThat(metric(body, "cy_verification_findings", metrics.served())).isZero();
    }

    /** {@code scope} 라벨이 붙어 나가는지. 증분이 열릴 때 이름을 안 바꾸려고 미리 단다. */
    @Test
    @DisplayName("dataset 과 scope 라벨이 함께 나간다")
    void tagsDatasetAndScope() throws Exception {
        closedRun(metrics.served(), VerdictType.PASS, 0, AS_OF);
        refresher.refresh();

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
        // 리터럴로 박으면 CORRUPT 스키마 컨테이너에서 이 테스트만 깨진다 —
        // served() 는 uk_coupon_member 존재로 정해진다. 나머지 테스트도 전부 그것을 쓴다.
        assertThat(body)
                .contains("cy_verification_verdict{dataset=\""
                        + metrics.served().name() + "\",scope=\"FULL\"}");
    }

    /**
     * <b>배선이 끊겨도 나머지 테스트는 전부 초록이다.</b> 그것들은 {@code refresh()} 를
     * 손으로 부르기 때문이다 — 실제 운영에서 값을 만드는 것은 스케줄러다.
     *
     * <p><b>{@code batch.scheduling.enabled} 에 안 묶이는 것까지 함께 지킨다.</b> 이 테스트는
     * 그 값이 {@code false} 인 컨텍스트에서 도는데, 그래도 태스크가 <b>있어야</b> 한다.
     * 묶어 버리면 스케줄러를 끈 채 띄우는 기동 — 부하 측정 중이거나 검증만 손으로 돌릴 때 —
     * 에서 지표가 통째로 죽는다. 그때야말로 판정을 봐야 하는 자리라 예외가 아니라 기본이다.
     */
    @Test
    @DisplayName("되읽기가 스케줄 태스크로 실제 등록된다 — 스케줄러를 꺼도")
    void refreshIsWiredAsAScheduledTask() {
        assertThat(taskHolder.getScheduledTasks())
                .as("@Scheduled 가 빠졌거나 @EnableScheduling 이 사라졌다. 그러면 게이지가 "
                        + "영원히 NaN 인데 나머지 테스트는 refresh() 를 직접 불러 초록이다")
                .as("등록된 태스크=%s", taskHolder.getScheduledTasks().stream()
                        .map(task -> task.getTask().getRunnable().toString()).toList())
                // 타입으로 못 본다 — 스프링이 Runnable 을 한 겹 감싸므로
                // ScheduledMethodRunnable 로 instanceof 하면 감싼 것에 걸려 거짓이 된다.
                // 등록 이름은 "<클래스>.<메서드>" 형식이라 그것으로 잇는다.
                .anyMatch(task -> task.getTask().getRunnable().toString()
                        .startsWith(VerificationMetricsRefresher.class.getName() + ".refresh"));
    }

    /**
     * <b>새 되읽기도 같은 방식으로 잇는다.</b> 풀 크기 단언(아래)은 이 태스크가
     * {@code @Scheduled} 에서 빠져도 통과한다 — 그러면 마지막 성공 시각 게이지가 영원히
     * 첫 값에 머물고, {@code ExpireNotSucceeding} 이 <b>배치가 멀쩡한데</b> 뜬다.
     * 나머지 테스트는 {@code refresh()} 를 직접 부르므로 그때도 초록이다.
     */
    @Test
    @DisplayName("배치 실행 지표 되읽기가 @Scheduled 로 실제 등록된다")
    void batchRunMetricsRefreshIsScheduled() {
        assertThat(taskHolder.getScheduledTasks())
                .as("등록된 태스크=%s", taskHolder.getScheduledTasks().stream()
                        .map(task -> task.getTask().getRunnable().toString()).toList())
                .anyMatch(task -> task.getTask().getRunnable().toString()
                        .startsWith(BatchRunMetricsRefresher.class.getName() + ".refresh"));
    }

    /**
     * <b>이 배선이 끊기면 CY-421 이 통째로 되돌아간다.</b> 게이지는 영원히 {@code NaN} 이고
     * {@code ExpireLeavesWorkBehind} 는 {@code NaN > 0} 이 거짓이라 발화할 수 없다 —
     * 정확히 그 티켓 이전 상태다. 그런데 {@code ExpireMetricExposureTest} 는 모든 케이스가
     * {@code refresh()} 를 <b>손으로</b> 부르므로 애너테이션을 지워도 전부 초록이다.
     */
    @Test
    @DisplayName("만료 대기 되읽기가 @Scheduled 로 실제 등록된다")
    void expirePendingRefreshIsScheduled() {
        assertThat(taskHolder.getScheduledTasks())
                .as("등록된 태스크=%s", taskHolder.getScheduledTasks().stream()
                        .map(task -> task.getTask().getRunnable().toString()).toList())
                .anyMatch(task -> task.getTask().getRunnable().toString()
                        .startsWith(ExpirePendingRefresher.class.getName() + ".refresh"));
    }

    /**
     * <b>이 배선이 끊기면 CY-446 의 유일한 감시가 조용히 꺼진다.</b> 게이지는 영원히
     * {@code NaN} 이고 {@code CouponRoundsNotOpening}({@code > 0})은 발화 조건 자체를 못
     * 만든다 — 그러면 회차가 안 열려도 아무도 모른다. 이 되읽기는
     * {@code batch.scheduling.enabled} 와 <b>무관하게</b> 등록돼야 한다(이 컨텍스트가 그것을
     * {@code false} 로 띄운다) — 끈 채로 띄운 배치에서도 <i>"회차가 안 열리고 있다"</i> 는
     * 사실은 참이기 때문이다.
     */
    @Test
    @DisplayName("회차 전이 대기 되읽기가 @Scheduled 로 등록된다 — 스케줄러를 꺼도")
    void couponRoundPendingRefreshIsScheduled() {
        assertThat(taskHolder.getScheduledTasks())
                .as("등록된 태스크=%s", taskHolder.getScheduledTasks().stream()
                        .map(task -> task.getTask().getRunnable().toString()).toList())
                .anyMatch(task -> task.getTask().getRunnable().toString()
                        .startsWith(CouponRoundPendingRefresher.class.getName() + ".refresh"));
    }

    /**
     * <b>스케줄러 풀이 @Scheduled 수를 감당하는지 본다.</b>
     *
     * <p>{@code spring.task.scheduling.pool.size} 는 Boot 가 직접 소비해서 어떤
     * {@code @Value} 에도 리터럴로 안 나온다 — {@code ResolvedBatchConfigTest} 의 키 스캔이
     * <b>구조적으로 못 잡는다.</b> 그래서 문자열이 아니라 <b>빈의 실제 크기</b>로 잇는다.
     *
     * <p>1 이면 만료가 도는 내내 되읽기가 멈추고, 되읽기가 커넥션을 기다리는 동안 만료 크론
     * 발화가 밀린다. 키 경로를 오타 내면 Boot 가 조용히 1 로 폴백하는데, 그때 드러나는 것은
     * <i>"지표가 가끔 안 갱신된다"</i> 뿐이라 원인까지 가는 길이 없다.
     */
    @Test
    @DisplayName("스케줄러 풀이 @Scheduled 수를 감당한다 — 서로를 막지 않게")
    void schedulerPoolFitsScheduledTaskCount() {
        // 정확한 값을 안 단언한다. 셸이나 CI 러너에 BATCH_SCHEDULER_POOL_SIZE 가 떠 있으면
        // 그 값이 들어와 빨개지는데 원인이 코드에 없어 찾기 어렵다. 정확한 값은
        // ResolvedBatchConfigTest 가 키 경로로 지키고, 여기가 막는 것은 "1 로 폴백" 이다.
        //
        // **다만 하한은 @Scheduled 수와 함께 움직여야 한다.** CY-446 이 둘을 더했을 때
        // 이 값이 5 로 남아 초록으로 지나갔다 — 그 순간 이 단언은 아무것도 안 지켰다.
        // CY-470 이 VerifyScheduler 를 더해 여덟이 됐다.
        assertThat(taskScheduler.getScheduledThreadPoolExecutor().getCorePoolSize())
                .as("spring.task.scheduling.pool.size 키 경로가 죽으면 Boot 가 조용히 1 로 "
                        + "폴백한다. 그러면 여덟 @Scheduled 가 스레드 하나를 다툰다")
                .isGreaterThanOrEqualTo(8);
    }

    /**
     * <b>{@link SchemaPresenceGuard} 가 부팅 경로에 실제로 얹히는지 본다.</b>
     *
     * <p>{@code SchemaPresenceGuardTest} 는 인스턴스를 손으로 만들어 {@code run()} 을 부르므로
     * <b>판정 로직</b>만 지킨다. {@code @Component} 를 떼거나 클래스를 컴포넌트 스캔 밖
     * 패키지로 옮기면 <b>전 저장소가 초록인 채로 가드가 안 돈다</b> — 나머지 배치 테스트는
     * 전부 스키마를 갖춘 컨테이너 위에서 통과 경로만 밟기 때문에 빈이 없어도 초록이다.
     *
     * <p>순서도 함께 본다. {@code JobLauncherApplicationRunner} 의 정렬값이 0 이라,
     * 가드에 정렬값을 안 주면 {@code LOWEST_PRECEDENCE} 로 그 뒤에 온다 — 가드가 말하기 전에
     * 잡이 SQL 에러로 죽는다. 지금은 {@code spring.batch.job.enabled} 가 {@code false} 라
     * 안 터지지만, 그 결합을 설정 한 줄에 맡겨 두지 않는다.
     */
    @Test
    @DisplayName("스키마 가드가 러너로 배선되고 잡보다 먼저 온다")
    void schemaGuardRunsBeforeAnyJob() {
        List<ApplicationRunner> runners = context.getBeanProvider(ApplicationRunner.class)
                .orderedStream()
                .toList();

        assertThat(runners)
                .as("@Component 를 떼거나 스캔 밖으로 옮기면 가드가 안 돈다")
                .hasAtLeastOneElementOfType(SchemaPresenceGuard.class);

        // 순서는 정렬값으로 직접 본다. 이 컨텍스트는 spring.batch.job.enabled=false 라
        // JobLauncherApplicationRunner 빈이 아예 없어서, 그 빈을 찾아 인덱스를 비교하는
        // 방식이면 단언이 한 번도 안 돈다 — 지키겠다고 적은 결합이 실제로는 안 지켜진다.
        assertThat(OrderUtils.getOrder(SchemaPresenceGuard.class))
                .as("JobLauncherApplicationRunner 의 정렬값은 0 이다. @Order 를 떼면 "
                        + "LOWEST_PRECEDENCE 라 잡이 먼저 돌고, 가드가 말하기 전에 "
                        + "SQL 에러로 죽는다")
                .isNotNull()
                .isLessThan(0);
    }

    /**
     * <b>주기와 알림 창은 한 덩어리다.</b> {@code VerificationMetricsStale} 은 10분 창에서
     * 실패 증분 3 을 보는데, 주기를 5분으로 올리면 창 안에 두 번밖에 시도하지 않아
     * <b>되읽기가 100% 실패해도 3 을 못 채운다</b>. 그러면 게이지가 낡은 {@code PASS} 를
     * 계속 들고 있는데 아무 알림도 안 뜬다 — 그 카운터를 만든 이유가 통째로 사라진다.
     *
     * <p>가드를 {@code .example} 값만 보는 테스트에 두지 않은 이유가 여기 있다. 이 값이
     * 실제로 커지는 경로는 <b>운영에서 환경변수로 주는 것</b>이고, 그건 설정 파일 검사가
     * 구조적으로 못 본다. 그래서 빈을 만드는 자리에서 막는다.
     */
    @Test
    @DisplayName("되읽기 주기가 알림 창을 넘으면 기동을 거부한다")
    void rejectsARefreshPeriodTheAlertWindowCannotCover() {
        assertThatThrownBy(() -> new VerificationMetricsRefresher(
                runs, metrics, new SimpleMeterRegistry(), transactionManager, 5_000L, 300_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .as("규칙 이름이 없으면 왜 거부됐는지 알 수 없다")
                .hasMessageContaining("VerificationMetricsStale");

        assertThatCode(() -> new VerificationMetricsRefresher(
                runs, metrics, new SimpleMeterRegistry(), transactionManager, 5_000L, 120_000L))
                .as("상한 자체는 통과해야 한다 — 경계에서 막으면 이 클래스가 안 뜬다")
                .doesNotThrowAnyException();
    }


    private void closedRun(DatasetType dataset, VerdictType verdict, int findings,
            LocalDateTime asOf) {
        VerificationRun saved = runs.save(VerificationRun.start(
                asOf, null, ScopeType.FULL, dataset, 1, asOf.plusMinutes(1)));
        runs.update(VerificationRun.restore(
                saved.id(), asOf, null, ScopeType.FULL, dataset, 1,
                verdict, StatsStatus.COMPLETE, findings, "checksum", "fingerprint",
                asOf.plusMinutes(1), asOf.plusMinutes(2)));
    }

    /** 시드가 심는 것과 같은 모양의 행. {@code origin} 만 다르다. */
    private void seedRun(DatasetType dataset, VerdictType verdict, int findings,
            LocalDateTime asOf) {
        closedRun(dataset, verdict, findings, asOf);
        jdbcClient.sql("UPDATE verification_runs SET origin = 'SEED'").update();
    }

    /** 라벨까지 맞춰 게이지 값 하나를 꺼낸다. 없으면 그 자리에서 실패한다. */
    private static double metric(String body, String name, DatasetType dataset) {
        String prefix = name + "{dataset=\"" + dataset.name() + "\",scope=\"FULL\"}";
        return body.lines()
                .filter(line -> line.startsWith(prefix))
                .map(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        prefix + " 가 /actuator/prometheus 에 없다. 실제로 나간 cy_verification_* 은 "
                                + body.lines().filter(l -> l.startsWith("cy_verification")).toList()));
    }
}
