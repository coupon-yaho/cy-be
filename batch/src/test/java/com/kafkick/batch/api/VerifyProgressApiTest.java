package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.storage.db.MySqlContainerConfig;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

/**
 * 진행 중 실행 조회.
 *
 * <p><b>{@code /reports/latest} 와 갈라 둔 것이 이 API 의 전부다.</b> 그쪽은 마감된 실행만
 * 싣는데, 화면이 "지금 검증" 을 누르고 기다리는 동안 볼 것이 없어서 이 경로를 열었다.
 * 그래서 이 테스트가 지키는 축은 두 가지다 — <b>진행 중에도 답이 나오는가</b>,
 * 그리고 <b>그 답이 마감된 것과 구별되는가</b>.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class VerifyProgressApiTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @LocalServerPort
    private int port;

    @Autowired
    private VerificationRunRepository runs;

    @Autowired
    private VerificationFindingRepository findings;

    @Autowired
    private JdbcClient jdbcClient;

    private VerifyApiProbe probe;

    @BeforeEach
    void setUp() {
        probe = new VerifyApiProbe(port);
        jdbcClient.sql("DELETE FROM verification_findings").update();
        jdbcClient.sql("DELETE FROM verification_runs").update();
    }

    @Test
    @DisplayName("없는 실행은 404 다 — 도메인 코드로 나간다")
    void returnsNotFoundForUnknownRun() throws Exception {
        var response = probe.get("/api/v1/admin/verify/runs/999999/progress");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-003");
    }

    /**
     * <b>이 API 가 존재하는 이유.</b> 판정이 안 난 실행에서도 그때까지 쌓인 검출을 답해야 한다.
     * {@code /reports/latest} 는 이 실행을 아예 안 보여 준다.
     */
    @Test
    @DisplayName("판정 전이어도 그때까지 쌓인 검출을 답한다")
    void answersWhileRunning() throws Exception {
        long runId = openRun();
        appendFindings(runId, FindingType.REPLAY_MISMATCH, 3);

        JsonNode data = VerifyApiProbe.data(
                probe.get("/api/v1/admin/verify/runs/" + runId + "/progress"));

        assertThat(data.path("status").asString()).isEqualTo("RUNNING");
        assertThat(data.path("verdict").isNull())
                .as("판정이 안 났는데 값이 실리면 화면이 중간값을 최종으로 그린다")
                .isTrue();
        assertThat(data.path("findingCount").asInt()).isEqualTo(3);
        assertThat(data.path("byType").path("REPLAY_MISMATCH").asInt()).isEqualTo(3);
    }

    /**
     * <b>{@code verification_runs.finding_count} 를 읽으면 안 된다.</b> 그 컬럼은 판정 Step 이
     * 마감할 때 한 번 쓰이므로 진행 중에는 0 이다. 이 검사가 그 회귀를 잡는다.
     */
    @Test
    @DisplayName("검출 수는 실행 행이 아니라 검출 표에서 센다")
    void countsFromFindingsNotFromRunRow() throws Exception {
        long runId = openRun();
        appendFindings(runId, FindingType.USAGE_MISMATCH, 5);

        JsonNode data = VerifyApiProbe.data(
                probe.get("/api/v1/admin/verify/runs/" + runId + "/progress"));

        int onRunRow = jdbcClient.sql("SELECT finding_count FROM verification_runs WHERE id = :id")
                .param("id", runId).query(Integer.class).single();

        assertThat(onRunRow)
                .as("전제가 깨졌다 — 실행 행이 진행 중에 이미 채워져 있으면 이 검사가 뜻이 없다")
                .isZero();
        assertThat(data.path("findingCount").asInt()).isEqualTo(5);
    }

    @Test
    @DisplayName("마감된 실행은 DONE 이고 판정이 실린다")
    void reportsDoneAfterVerdict() throws Exception {
        long runId = openRun();
        appendFindings(runId, FindingType.REPLAY_MISMATCH, 2);
        close(runId, VerdictType.FAIL, 2);

        JsonNode data = VerifyApiProbe.data(
                probe.get("/api/v1/admin/verify/runs/" + runId + "/progress"));

        assertThat(data.path("status").asString()).isEqualTo("DONE");
        assertThat(data.path("verdict").asString()).isEqualTo("FAIL");
        assertThat(data.path("finishedAt").isNull()).isFalse();
    }

    @Test
    @DisplayName("검출이 0인 규칙도 여섯 개가 다 실린다 — 빠지면 '아직 안 센 것' 과 섞인다")
    void byTypeCarriesEveryRule() throws Exception {
        long runId = openRun();

        JsonNode byType = VerifyApiProbe.data(
                probe.get("/api/v1/admin/verify/runs/" + runId + "/progress")).path("byType");

        assertThat(byType.size()).isEqualTo(FindingType.values().length);
        for (FindingType type : FindingType.values()) {
            assertThat(byType.path(type.name()).asInt())
                    .as("%s 가 응답에 없거나 0이 아니다", type)
                    .isZero();
        }
    }

    /** 판정도 마감 시각도 없는 실행 — 배치가 {@code startRunStep} 을 지난 직후의 모양이다. */
    private long openRun() {
        return runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CORRUPT, 1, AS_OF)).id();
    }

    private void close(long runId, VerdictType verdict, int findingCount) {
        runs.update(VerificationRun.restore(
                runId, AS_OF, null, ScopeType.FULL, DatasetType.CORRUPT, 1,
                verdict, StatsStatus.COMPLETE, findingCount, "checksum", "fingerprint",
                AS_OF, AS_OF.plusMinutes(2), null));
    }

    /**
     * 발급건 단위 유형만 쓴다 — {@code forIssuance} 가 grain 을 검사하므로 회차·이력 단위
     * 유형을 넣으면 팩터리가 거절한다. 이 테스트가 재는 것은 <b>세는 방식</b>이지 유형별
     * 구분이 아니라, 한 grain 으로 통일하는 편이 뜻이 분명하다.
     */
    private void appendFindings(long runId, FindingType type, int count) {
        findings.appendAll(runId, java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> VerificationFinding.forIssuance(type, i + 1L, "기대", "실제"))
                .toList());
    }
}
