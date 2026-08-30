// 리포트 조회를 HTTP 표면에서 확인합니다. 제출물의 실제 계약이 여기입니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

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

import tools.jackson.databind.JsonNode;

/**
 * <b>계약은 자바 객체가 아니라 HTTP 본문이다.</b> {@code VerifyReportViewTest} 는 조립 규칙을
 * DB 없이 재고, 여기서는 <b>실제로 나가는 응답</b>을 본다.
 *
 * <p><b>이 클래스가 없어서 결함 하나가 그대로 통과했다.</b> 새 컨트롤러를
 * {@code BatchApiExceptionHandler} 의 {@code assignableTypes} 에 안 넣어서, 404 로 설계한
 * {@code RUN_NOT_FOUND} 가 <b>500 + 스프링 기본 본문</b>으로 나가고 있었다. 자바 단위
 * 테스트로는 원리적으로 못 본다.
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
class VerifyReportApiTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final String CLEAN_FULL =
            "/api/v1/admin/verify/reports/latest?dataset=CLEAN&scope=FULL";
    private static final String CORRUPT_FULL =
            "/api/v1/admin/verify/reports/latest?dataset=CORRUPT&scope=FULL";

    @LocalServerPort
    private int port;

    @Autowired
    private VerificationRunRepository runs;

    // **포트로 받는다.** batch 는 storage 어댑터를 runtimeOnly 로만 의존한다 —
    // 어댑터 타입을 컴파일 시점에 참조하면 그 경계가 무너진다. 쓰는 것은 appendAll 하나이고
    // 그것은 포트에 있다. 바로 위 runs 도 이미 포트로 받고 있었다.
    @Autowired
    private VerificationFindingRepository findings;

    @Autowired
    private JdbcClient jdbcClient;

    private VerifyApiProbe probe;

    @BeforeEach
    void setUp() {
        probe = new VerifyApiProbe(port);
        jdbcClient.sql("DELETE FROM verification_findings").update();
        jdbcClient.sql("DELETE FROM expected_findings").update();
        jdbcClient.sql("DELETE FROM verification_runs").update();
    }

    /**
     * <b>이 단언이 advice 등록 누락의 회귀 테스트다.</b> 등록이 빠지면 500 이 나온다.
     */
    @Test
    @DisplayName("판정이 없으면 404 다 — 봉투를 씌운 도메인 코드로 나간다")
    void returnsNotFoundWhenNoClosedRun() throws Exception {
        var response = probe.get(CLEAN_FULL);

        assertThat(response.statusCode())
                .as("advice 에 이 컨트롤러가 안 걸리면 500 + 스프링 기본 본문이 나간다")
                .isEqualTo(404);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-003");
    }

    @Test
    @DisplayName("정상셋은 대조가 없다 — 대조할 정답이 없기 때문이다")
    void cleanRunHasNoManifest() throws Exception {
        closedRun(DatasetType.CLEAN, VerdictType.PASS, 0, null);

        JsonNode data = VerifyApiProbe.data(probe.get(CLEAN_FULL));

        assertThat(data.path("manifest").isNull())
                .as("필드가 사라지면 '대조 안 함' 과 '대조해서 비었다' 가 구분이 안 된다")
                .isTrue();
        assertThat(data.path("run").path("dataset").asString()).isEqualTo("CLEAN");
    }

    @Test
    @DisplayName("검출이 0인 규칙도 여섯 개가 다 실린다 — 빠지면 '안 돌렸다' 로 읽힌다")
    void byTypeCarriesEveryRule() throws Exception {
        closedRun(DatasetType.CLEAN, VerdictType.PASS, 0, null);

        JsonNode byType = VerifyApiProbe.data(probe.get(CLEAN_FULL)).path("byType");

        assertThat(byType.size()).isEqualTo(FindingType.values().length);
        for (FindingType type : FindingType.values()) {
            assertThat(byType.path(type.name()).asInt())
                    .as("%s 가 응답에 없거나 0이 아니다", type)
                    .isZero();
        }
    }

    /**
     * <b>이 티켓의 1번 목표다.</b> 제출물은 {@code docs/} 에 커밋돼 diff 되므로, 같은 판정을
     * 두 번 뜨면 <b>바이트가 같아야</b> 한다. 다르면 그것이 <i>"결과가 바뀐 것"</i> 으로 읽힌다.
     */
    @Test
    @DisplayName("같은 판정을 두 번 떠도 본문이 바이트까지 같다")
    void sameRunRendersIdenticalBody() throws Exception {
        long runId = closedRun(DatasetType.CORRUPT, VerdictType.FAIL, 2, 11L);
        findings.appendAll(runId, java.util.List.of(
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 1, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));
        expected(11L, "ILLEGAL_TRANSITION", "HISTORY:1");
        expected(11L, "STOCK_MISMATCH", "COUPON:2");

        assertThat(probe.get(CORRUPT_FULL).body())
                .as("규칙 순서나 목록 순서가 실행마다 갈리면 diff 가 뜻을 잃는다")
                .isEqualTo(probe.get(CORRUPT_FULL).body());
    }

    @Test
    @DisplayName("정답과 정확히 일치하면 matches 가 응답에 true 로 실린다 — 게이트가 읽는 값")
    void matchesIsSerialized() throws Exception {
        long runId = closedRun(DatasetType.CORRUPT, VerdictType.FAIL, 1, 11L);
        findings.appendAll(runId, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));
        expected(11L, "STOCK_MISMATCH", "COUPON:2");

        JsonNode manifest = VerifyApiProbe.data(probe.get(CORRUPT_FULL)).path("manifest");

        assertThat(manifest.has("matches"))
                .as("레코드의 파생 메서드는 Jackson 이 안 싣는다. 없으면 게이트가 "
                        + "jq '.data.manifest.matches' 로 null 을 받고, 그 null 은 "
                        + "'불일치' 와 구분되지 않는다 — 일치한 실행이 불합격으로 읽힌다")
                .isTrue();
        assertThat(manifest.path("matches").asBoolean()).isTrue();
        assertThat(manifest.path("present").asBoolean()).isTrue();
    }

    /**
     * <b>정답 묶음이 사라진 상태.</b> {@code expected_findings} 가 0행이면 대조 SQL 의
     * {@code LEFT JOIN} 이 <b>검출 전부를 오탐으로 뒤집는다.</b> 그대로 실으면
     * <i>"검증기가 전부 오탐했다"</i> 는 제출물이 나가고 {@code verdict} 와 모순된다.
     */
    @Test
    @DisplayName("정답 묶음이 사라졌으면 대조를 접는다 — '오탐 전부' 를 싣지 않는다")
    void doesNotReportEverythingAsUnexpectedWhenManifestGone() throws Exception {
        long runId = closedRun(DatasetType.CORRUPT, VerdictType.FAIL, 1, 11L);
        findings.appendAll(runId, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));
        // expected_findings 를 일부러 안 심는다 — 시드 재주입으로 사라진 상태다.

        JsonNode manifest = VerifyApiProbe.data(probe.get(CORRUPT_FULL)).path("manifest");

        assertThat(manifest.path("present").asBoolean())
                .as("대조 상대가 없다는 사실 자체를 응답에 남긴다")
                .isFalse();
        assertThat(manifest.path("unexpected").size())
                .as("여기 검출이 실리면 '검증기가 오탐했다' 로 읽힌다")
                .isZero();
        assertThat(manifest.path("matches").isNull())
                .as("true 도 false 도 거짓말이다 — false 를 내면 verdict=PASS 옆에 "
                        + "matches=false 가 실려 보는 사람이 어느 쪽을 믿을지 알 수 없다")
                .isTrue();
        assertThat(manifest.path("missingCount").isNull())
                .as("0을 실으면 missingCount == 0 을 보는 쪽이 합격으로 읽는다")
                .isTrue();
    }

    @Test
    @DisplayName("오염셋인데 대조를 안 한 실행은 대조가 통째로 없다")
    void corruptWithoutSeedRunHasNoManifest() throws Exception {
        closedRun(DatasetType.CORRUPT, VerdictType.FAIL, 0, null);

        assertThat(VerifyApiProbe.data(probe.get(CORRUPT_FULL)).path("manifest").isNull())
                .as("대조 Step 까지 못 간 실행이다. 빈 대조로 채우면 '일치' 로 읽힌다")
                .isTrue();
    }

    /**
     * <b>{@code run} 을 통째로 싣는 대가다.</b> {@link VerificationRun} 에 컴포넌트를 더하면
     * <b>아무 결정 없이</b> 이 공개 리포트에 실린다 — 이 뷰는 무엇을 뺄지 고르지 않는다.
     * 그것이 편해서 고른 모양이지만, 편한 만큼 <b>PII 가 새는 경로</b>이기도 하다.
     *
     * <p>그래서 <b>키 목록을 여기 박아 둔다.</b> 컴포넌트를 더한 사람이 이 테스트를 고치면서
     * <i>"이게 제출물에 실려도 되나"</i> 를 한 번 보게 된다. {@code PRD:2143} 이
     * <i>"집계값만. 이름·연락처 금지"</i> 로 정한 그 판단이다.
     */
    @Test
    @DisplayName("제출물에 실리는 키가 정확히 이것뿐이다 — 늘리려면 이 목록을 고쳐야 한다")
    void bodyCarriesExactlyTheDeclaredKeys() throws Exception {
        long runId = closedRun(DatasetType.CORRUPT, VerdictType.FAIL, 1, 11L);
        findings.appendAll(runId, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));
        expected(11L, "STOCK_MISMATCH", "COUPON:2");

        JsonNode data = VerifyApiProbe.data(probe.get(CORRUPT_FULL));

        assertThat(keysOf(data)).containsExactly("schema", "run", "byType", "manifest");
        assertThat(keysOf(data.path("run")))
                .as("VerificationRun 에 컴포넌트가 붙으면 결정 없이 공개 리포트에 실린다")
                .containsExactly(
                        "id", "asOf", "fromTs", "scope", "dataset", "attempt",
                        "verdict", "statsStatus", "findingCount", "findingsChecksum",
                        "datasetFingerprint", "startedAt", "finishedAt", "seedRunId");
        assertThat(keysOf(data.path("manifest")))
                .as("순서까지 고정한다 — @JsonPropertyOrder 가 없으면 파생 프로퍼티의 자리는 "
                        + "Jackson 이 메서드를 발견한 순서이고, JVM 이 그것을 보장하지 않는다. "
                        + "JDK 를 올린 날 코드 변경 없이 diff 가 생기면 그것이 판정 변화로 읽힌다")
                .containsExactly(
                        "present", "seedRunId", "sampleLimit", "expectedCount",
                        "corruptionCount", "expectedDigest",
                        "missingCount", "unexpectedCount", "matches", "truncated",
                        "missing", "unexpected");
    }

    /**
     * <b>스크립트가 이 형식에 통째로 기대고 있다.</b> {@code dump-verify-report.sh} 가
     * {@code finishedAt}·{@code asOf} 를 ISO 8601 문자열로 보고 epoch 로 바꿔 신선도를
     * 판정한다.
     *
     * <p>날짜 모듈이 클래스패스에서 빠지면 Jackson 이 {@code LocalDateTime} 을
     * <b>{@code {"year":2026,"month":8,...}} 객체로</b> 내보낸다. 그러면 스크립트가
     * <b>매일 조용히 실패</b>하고, 커밋 공백은 "머신이 꺼진 날" 과 구분되지 않는다.
     * 자바 쪽 테스트로는 안 드러난다 — 여기가 유일한 자리다.
     */
    @Test
    @DisplayName("시각은 ISO 8601 문자열이다 — 객체로 나가면 덤프가 매일 조용히 실패한다")
    void timestampsSerializeAsIsoStrings() throws Exception {
        closedRun(DatasetType.CLEAN, VerdictType.PASS, 0, null);

        JsonNode run = VerifyApiProbe.data(probe.get(CLEAN_FULL)).path("run");

        for (String field : java.util.List.of("asOf", "startedAt", "finishedAt")) {
            assertThat(run.path(field).isString())
                    .as("%s 가 문자열이 아니다 — 스크립트의 date 파싱이 통째로 깨진다", field)
                    .isTrue();
            assertThat(run.path(field).asString())
                    .as("%s 가 yyyy-MM-ddTHH:mm:ss 로 시작해야 앞 19자를 잘라 쓸 수 있다", field)
                    .matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*");
        }
    }

    /**
     * <b>정렬이 없으면 표본이 뜻을 잃는다.</b> 목록은 {@code SAMPLE_LIMIT} 에서 잘리는데,
     * 순서가 실행마다 갈리면 <b>같은 판정에서 다른 200건</b>이 실려 diff 가
     * <i>"결과가 바뀌었다"</i> 로 읽힌다.
     */
    @Test
    @DisplayName("누락·오탐 목록이 (규칙, target_key) 오름차순이다 — 둘 이상일 때 드러난다")
    void listsComeBackSorted() throws Exception {
        long runId = closedRun(DatasetType.CORRUPT, VerdictType.FAIL, 3, 11L);
        // 검출 셋 — 전부 정답에 없다(오탐). 일부러 뒤섞어 심는다.
        findings.appendAll(runId, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 9, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b"),
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 5, "a", "b")));
        // 정답 둘 — 검출에 없다(누락). 역시 뒤섞어 심는다.
        expected(11L, "STOCK_MISMATCH", "COUPON:70");
        expected(11L, "ILLEGAL_TRANSITION", "HISTORY:40");

        JsonNode manifest = VerifyApiProbe.data(probe.get(CORRUPT_FULL)).path("manifest");

        assertThat(pairs(manifest.path("missing")))
                .as("규칙 이름이 1순위, target_key 가 2순위다")
                .containsExactly("ILLEGAL_TRANSITION/HISTORY:40", "STOCK_MISMATCH/COUPON:70");
        assertThat(pairs(manifest.path("unexpected")))
                .as("COUPON:2 가 COUPON:9 보다 앞이다 — 문자열 비교라 숫자 크기가 아니다")
                .containsExactly("ILLEGAL_TRANSITION/HISTORY:5",
                        "STOCK_MISMATCH/COUPON:2", "STOCK_MISMATCH/COUPON:9");
        assertThat(manifest.path("missingCount").asInt()).isEqualTo(2);
        assertThat(manifest.path("unexpectedCount").asInt()).isEqualTo(3);
        assertThat(manifest.path("truncated").asBoolean())
                .as("다섯 건은 표본 한계에 한참 못 미친다")
                .isFalse();
    }

    private static java.util.Collection<String> keysOf(JsonNode node) {
        return node.propertyNames();
    }

    private static java.util.List<String> pairs(JsonNode array) {
        java.util.List<String> out = new java.util.ArrayList<>();
        array.forEach(key -> out.add(
                key.path("findingType").asString() + "/" + key.path("targetKey").asString()));
        return out;
    }

    private long closedRun(DatasetType dataset, VerdictType verdict, int findingCount,
            Long seedRunId) {
        VerificationRun saved = runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, dataset, 1, AS_OF));
        runs.update(VerificationRun.restore(
                saved.id(), AS_OF, null, ScopeType.FULL, dataset, 1,
                verdict, StatsStatus.COMPLETE, findingCount, "checksum", "fingerprint",
                AS_OF, AS_OF.plusMinutes(2), null));
        if (seedRunId != null) {
            runs.recordComparedManifest(saved.id(), seedRunId);
        }
        return saved.id();
    }

    private void expected(long seedRunId, String findingType, String targetKey) {
        // corrupt_type 은 NOT NULL 이다 — 시드가 어느 오염 유형으로 심었는지이고,
        // 이 테스트가 재는 축(대조 집합)과 무관하므로 아무 유효값이나 넣는다.
        jdbcClient.sql("""
                        INSERT INTO expected_findings
                                    (seed_run_id, corrupt_type, finding_type, target_key,
                                     note, created_at)
                        VALUES (:seedRunId, 1, :findingType, :targetKey, '테스트', :at)
                        """)
                .param("seedRunId", seedRunId)
                .param("findingType", findingType)
                .param("targetKey", targetKey)
                .param("at", AS_OF)
                .update();
    }
}
