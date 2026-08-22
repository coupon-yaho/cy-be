// 검증 판정이 관제까지 나가는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
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
        "management.server.port=0"
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
        assertThat(body)
                .contains("cy_verification_verdict{dataset=\"CLEAN\",scope=\"FULL\"}");
    }

    /**
     * <b>배선이 끊겨도 나머지 테스트는 전부 초록이다.</b> 그것들은 {@code refresh()} 를
     * 손으로 부르기 때문이다 — 실제 운영에서 값을 만드는 것은 스케줄러다.
     *
     * <p><b>{@code batch.scheduling.enabled} 에 안 묶이는 것까지 함께 지킨다.</b> 이 테스트는
     * 그 값이 {@code false} 인 컨텍스트에서 도는데, 그래도 태스크가 <b>있어야</b> 한다.
     * 묶어 버리면 스케줄러를 끈 채 {@code verifyJob} 을 돌리는 <b>정상 절차</b>에서 지표가
     * 통째로 죽는다 — 그 절차는 {@code rejectRunningSchedulers} 가 강제하는 것이라
     * 예외가 아니라 기본이다.
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
