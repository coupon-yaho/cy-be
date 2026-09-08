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
    private static final String DIFF = "/api/v1/admin/verify/reports/diff";
    private static final String RESIDUAL = "/api/v1/admin/verify/reports/residual";

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

        assertThat(keysOf(data))
                .containsExactly("schema", "run", "examined", "byType", "manifest");
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

    // ── 잔여 불일치 (CY-947) ────────────────────────────────────────────────

    /**
     * <b>이것이 이 티켓의 전부다.</b> {@code /reports/diff} 는 개수만 맞대므로
     * <i>"그 3건이 그대로"</i> 와 <i>"3건 고쳐지고 새로 3건"</i> 이 <b>같은 응답</b>이다 —
     * 처방이 정반대인데.
     *
     * <p>그래서 <b>두 실행의 검출 수를 같게 두고</b> 그 안에서 갈리는지 본다. diff 로는
     * 그 상태가 {@code delta = 0} 이라 <i>"아무 일도 없었다"</i> 로 보인다.
     */
    @Test
    @DisplayName("개수가 그대로여도 무엇이 남고 무엇이 새로 생겼는지 가른다")
    void residualSplitsWhatDiffCannot() throws Exception {
        long was = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 2, 1);
        findings.appendAll(was, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));

        // 개수는 그대로 2건. 그런데 하나는 그대로 남았고 하나는 바뀌었다.
        long now = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 2, 2);
        findings.appendAll(now, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 3, "a", "b")));

        JsonNode diff = VerifyApiProbe.data(probe.get(DIFF + "?before=" + was + "&after=" + now));
        assertThat(diff.path("totalDelta").asInt())
                .as("전제 — diff 로는 아무 일도 없어 보인다. 그래서 이 조회가 필요하다")
                .isZero();

        JsonNode data = VerifyApiProbe.data(
                probe.get(RESIDUAL + "?before=" + was + "&after=" + now));

        JsonNode stock = ruleOf(data, "STOCK_MISMATCH");
        assertThat(stock.path("persisted").asInt())
                .as("COUPON:1 은 두 실행에 다 있다 — 아무도 안 고치고 있다")
                .isEqualTo(1);
        assertThat(stock.path("introduced").asInt())
                .as("COUPON:3 은 그 사이에 새로 생겼다")
                .isEqualTo(1);
        assertThat(stock.path("resolved").asInt())
                .as("COUPON:2 는 사라졌다")
                .isEqualTo(1);

        assertThat(data.path("remaining").asInt())
                .as("PRD 가 말하는 잔여 불일치 건수는 지속 + 신규다")
                .isEqualTo(2);
        assertThat(data.path("introduced").asInt()).isEqualTo(1);
        assertThat(data.path("resolved").asInt()).isEqualTo(1);

        // **응답이 자기를 설명해야 한다.** diff 가 같은 이유로 같은 것을 싣는다.
        assertThat(data.path("before").path("attempt").asInt()).isEqualTo(1);
        assertThat(data.path("after").path("attempt").asInt()).isEqualTo(2);
        assertThat(data.path("schema").asString()).isNotBlank();
    }

    /**
     * <b>검출이 없는 규칙도 0 으로 낸다.</b> 빼면 <i>"그 규칙을 봤는데 없었다"</i> 와
     * <i>"그 규칙이 아예 안 돌았다"</i> 가 응답에서 같은 모양이 된다.
     */
    @Test
    @DisplayName("검출이 없는 규칙도 0 으로 나온다")
    void residualListsEveryRule() throws Exception {
        long was = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 1, 1);
        findings.appendAll(was, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1, "a", "b")));
        long now = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.PASS, 0, 2);

        JsonNode data = VerifyApiProbe.data(
                probe.get(RESIDUAL + "?before=" + was + "&after=" + now));

        assertThat(data.path("byType").size())
                .as("규칙 여섯이 전부 나와야 한다")
                .isEqualTo(FindingType.values().length);
        assertThat(ruleOf(data, "DUP_PER_MEMBER").path("persisted").asInt()).isZero();
        assertThat(ruleOf(data, "STOCK_MISMATCH").path("resolved").asInt())
                .as("고쳐서 사라졌다")
                .isEqualTo(1);
        assertThat(data.path("remaining").asInt()).isZero();
    }

    private static JsonNode ruleOf(JsonNode data, String type) {
        for (JsonNode rule : data.path("byType")) {
            if (type.equals(rule.path("type").asString())) {
                return rule;
            }
        }
        throw new AssertionError("byType 에 " + type + " 이 없다: " + data.path("byType"));
    }

    // ── 두 실행 맞대기 (CY-944) ──────────────────────────────────────────────

    /**
     * <b>이것이 이 티켓의 전부다.</b> 한 실행의 판정만으로는 <i>"원래 0건이었다"</i> 와
     * <i>"고쳐서 0건이 됐다"</i> 가 구분되지 않는다.
     */
    @Test
    @DisplayName("고치기 전과 뒤를 맞대어 규칙별로 줄어든 수를 낸다")
    void diffShowsWhatWasFixed() throws Exception {
        long was = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 2, 1);
        findings.appendAll(was, java.util.List.of(
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 1, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));

        long now = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 1, 2);
        findings.appendAll(now, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));

        JsonNode data = VerifyApiProbe.data(probe.get(DIFF + "?before=" + was + "&after=" + now));

        assertThat(data.path("before").path("findingCount").asInt()).isEqualTo(2);
        assertThat(data.path("after").path("findingCount").asInt()).isEqualTo(1);
        // **응답이 자기를 설명해야 한다.** 번호만 있으면 저장된 JSON 하나로는 before 가
        // 무엇이었는지 DB 를 다시 물어야 하고, before/after 를 뒤집어 넣은 것도 못 알아챈다.
        assertThat(data.path("before").path("dataset").asString()).isEqualTo("CORRUPT");
        assertThat(data.path("before").path("scope").asString()).isEqualTo("FULL");
        assertThat(data.path("before").path("attempt").asInt()).isEqualTo(1);
        assertThat(data.path("before").path("startedAt").asString()).isNotBlank();
        assertThat(data.path("before").path("verdict").asString()).isEqualTo("FAIL");
        assertThat(data.path("schema").asString())
                .as("dataset 만으로는 정상셋 배치와 운영 배치가 같은 이름표가 된다")
                .isNotBlank();
        assertThat(data.path("totalDelta").asInt())
                .as("음수가 줄어든 것이다 — 대사가 성공하면 음수가 나온다")
                .isEqualTo(-1);

        JsonNode byType = data.path("byType");
        assertThat(byType.size())
                .as("검출이 0인 규칙도 채운다 — 빠지면 '그 규칙을 안 봤다' 와 같아진다")
                .isEqualTo(FindingType.values().length);
        assertThat(ruleOf(byType, FindingType.ILLEGAL_TRANSITION).path("delta").asInt())
                .isEqualTo(-1);
        assertThat(ruleOf(byType, FindingType.STOCK_MISMATCH).path("delta").asInt())
                .as("안 고친 규칙은 0 이어야 한다 — 총합만 보면 이것이 안 보인다")
                .isZero();
    }

    /**
     * <b>판정이 바뀌는 쪽을 태워야 한다.</b> 안 바뀌는 경우만 재면
     * {@code verdictChanged} 를 <b>{@code false} 상수</b>로 바꿔도 통과한다 — 실제로 그
     * 돌연변이가 살아남았다.
     */
    @Test
    @DisplayName("FAIL 에서 PASS 로 가면 판정이 바뀐 것으로 낸다")
    void reportsAVerdictFlip() throws Exception {
        long was = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 1, 1);
        findings.appendAll(was, java.util.List.of(
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 1, "a", "b")));
        long now = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.PASS, 0, 2);

        JsonNode data = VerifyApiProbe.data(probe.get(DIFF + "?before=" + was + "&after=" + now));

        assertThat(data.path("verdictChanged").asBoolean()).isTrue();
        assertThat(data.path("after").path("verdict").asString())
                .as("방향은 두 verdict 로 파생한다 — boolean 하나로 충분하다")
                .isEqualTo("PASS");
        assertThat(data.path("totalDelta").asInt()).isEqualTo(-1);
    }

    /**
     * <b>총합만 내면 제일 위험한 상태가 안 보인다.</b> 한 규칙이 줄고 다른 규칙이 같은
     * 수만큼 늘면 합은 그대로다 — 그것을 <i>"변화 없음"</i> 으로 읽으면 새로 생긴 사고를
     * 놓친다.
     */
    @Test
    @DisplayName("합이 같아도 규칙별로는 갈린다")
    void perRuleDeltasSurviveACancellingTotal() throws Exception {
        long was = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 1, 1);
        findings.appendAll(was, java.util.List.of(
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 1, "a", "b")));

        long now = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 1, 2);
        findings.appendAll(now, java.util.List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2, "a", "b")));

        JsonNode data = VerifyApiProbe.data(probe.get(DIFF + "?before=" + was + "&after=" + now));

        assertThat(data.path("totalDelta").asInt()).isZero();
        assertThat(ruleOf(data.path("byType"), FindingType.ILLEGAL_TRANSITION).path("delta")
                .asInt()).isEqualTo(-1);
        assertThat(ruleOf(data.path("byType"), FindingType.STOCK_MISMATCH).path("delta").asInt())
                .as("총합 0 뒤에 숨은 새 검출이다")
                .isEqualTo(1);
    }

    /**
     * <b>비교 불가는 0 이 아니라 거절이다.</b> 0 으로 내면 화면이 <i>"차이 없음"</i> 으로
     * 읽는다 — 이 저장소가 반복해서 막아 온 모양이다.
     */
    @Test
    @DisplayName("dataset 이 다르면 맞대지 않고 거절한다")
    void refusesRunsThatSawDifferentThings() throws Exception {
        long clean = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);
        long corrupt = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 1, 1);

        var response = probe.get(DIFF + "?before=" + clean + "&after=" + corrupt);

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-025");
    }

    /**
     * <b>{@code dataset} 이 같아도 {@code scope} 가 다르면 거절한다.</b> 앞 시험은 dataset
     * 만 갈라서, {@code scope} 비교를 지워도 통과했다 — 축을 하나씩 태운다.
     */
    @Test
    @DisplayName("scope 만 달라도 맞대지 않는다")
    void refusesRunsWithDifferentScope() throws Exception {
        long full = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);
        // 증분은 시작 시각이 필수다 — 도메인이 그것을 요구한다.
        LocalDateTime from = AS_OF.minusHours(1);
        long incremental = runs.save(VerificationRun.start(
                AS_OF, from, ScopeType.INCREMENTAL, DatasetType.CLEAN, 1, AS_OF)).id();
        runs.update(VerificationRun.restore(
                incremental, AS_OF, from, ScopeType.INCREMENTAL, DatasetType.CLEAN, 1,
                VerdictType.PASS, StatsStatus.COMPLETE, 0, "checksum", "fingerprint",
                AS_OF, AS_OF.plusMinutes(2), null));

        var response = probe.get(DIFF + "?before=" + full + "&after=" + incremental);

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-025");
    }

    /**
     * <b>시드가 심은 기준 행은 실행이 아니다.</b> 그것을 {@code before} 로 받으면
     * <i>"배치가 800건을 고쳤다"</i> 는 거짓 증거가 나온다 — 배치는 아무것도 안 고쳤고
     * 시드의 기준값과 배치 결과를 뺀 것이다. 시드 행의 id 는 낮은 번호라 사람이 제일
     * 먼저 찍어 보는 번호이기도 하다.
     */
    @Test
    @DisplayName("시드가 심은 행은 맞대기 대상이 아니다 — 404 다")
    void seedRowsAreNotVisibleToTheConsole() throws Exception {
        long batchRun = closedRunWithAttempt(DatasetType.CORRUPT, VerdictType.FAIL, 0, 1);
        long seed = plantSeedRun();

        var response = probe.get(DIFF + "?before=" + seed + "&after=" + batchRun);

        assertThat(response.statusCode())
                .as("origin 을 안 걸면 200 이 나오고 '고쳤다' 는 증거가 만들어진다")
                .isEqualTo(404);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-003");
    }

    /**
     * 아직 판정이 없는 실행은 검출 수가 <b>중간값</b>이다 — {@code /reports/latest} 가
     * {@code verdict IS NOT NULL} 을 요구하는 것과 같은 이유다.
     */
    @Test
    @DisplayName("판정이 없는 실행은 맞대지 않는다 — 409 다")
    void refusesARunWithoutAVerdict() throws Exception {
        long closed = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);
        long running = runs.save(VerificationRun.start(
                AS_OF.plusHours(1), null, ScopeType.FULL, DatasetType.CLEAN, 9,
                AS_OF.plusHours(1))).id();

        var response = probe.get(DIFF + "?before=" + closed + "&after=" + running);

        assertThat(response.statusCode())
                .as("파라미터가 아니라 그 실행의 상태다 — 같은 번호로 잠시 뒤 다시 부르면 "
                        + "된다. 400 이면 자동화가 파라미터를 고치는 루프에 빠진다")
                .isEqualTo(409);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-026");
    }

    /**
     * <b>판정만 있고 종료 시각이 없는 행도 안 닫힌 것이다.</b>
     * {@code SELECT_LATEST_CLOSED} 가 둘을 함께 요구한다 — 한쪽만 보면
     * {@code /reports/latest} 가 안 내주는 행을 이 조회가 증적으로 내보낸다.
     */
    @Test
    @DisplayName("종료 시각이 없으면 판정이 있어도 맞대지 않는다")
    void refusesARunWithoutAFinishTime() throws Exception {
        long closed = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);
        long halfOpen = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 2);
        jdbcClient.sql("UPDATE verification_runs SET finished_at = NULL WHERE id = :id")
                .param("id", halfOpen).update();

        var response = probe.get(DIFF + "?before=" + closed + "&after=" + halfOpen);

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-026");
    }

    /**
     * <b>없는 번호와 못 맞대는 실행은 처방이 다르다.</b> 전자는 번호를 다시 찾아야 하고,
     * 후자는 둘 중 하나를 바꿔야 한다.
     */
    @Test
    @DisplayName("없는 실행은 404 다")
    void missingRunIsNotFound() throws Exception {
        long closed = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);

        var response = probe.get(DIFF + "?before=" + closed + "&after=999999");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .isEqualTo("VERIFICATION-003");
    }

    /** 같은 실행을 뺀 값은 언제나 0 이라 <b>아무것도 안 말한다.</b> */
    @Test
    @DisplayName("같은 실행끼리는 맞대지 않는다")
    void refusesTheSameRunTwice() throws Exception {
        long only = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);

        var response = probe.get(DIFF + "?before=" + only + "&after=" + only);

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asString())
                .as("CommonErrorCode.INVALID_INPUT 도 400 이라 상태코드만으로는 안 갈린다")
                .isEqualTo("VERIFICATION-025");
    }

    /**
     * <b>이름을 가른다 — 오버로드로 두면 조용히 틀린다.</b> 기존 {@code closedRun} 의
     * 마지막 인자가 {@code Long seedRunId} 라, {@code closedRun(..., 11)} 처럼 {@code L}
     * 하나를 빠뜨리면 <b>컴파일이 통과한 채 attempt=11 · seedRunId=null</b> 로 간다 —
     * 그리고 그 시험은 대조를 재려던 시험이다. 같은 파일에 {@code 11L} 이 다섯 곳 있다.
     *
     * <p>attempt 를 받는 이유는 {@code uk_run_params} 가
     * {@code (as_of, dataset, scope, attempt)} 라 같은 창에 두 실행을 못 만들기 때문이다 —
     * 맞대기 시험은 <b>정의상 둘</b>이 필요하다.
     */
    private long closedRunWithAttempt(DatasetType dataset, VerdictType verdict,
            int findingCount, int attempt) {
        VerificationRun saved = runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, dataset, attempt, AS_OF));
        runs.update(VerificationRun.restore(
                saved.id(), AS_OF, null, ScopeType.FULL, dataset, attempt,
                verdict, StatsStatus.COMPLETE, findingCount, "checksum", "fingerprint",
                AS_OF, AS_OF.plusMinutes(2), null));
        return saved.id();
    }

    /** {@code VerificationRunHistoryTest} 와 같은 관용 — 포트에는 시드를 심는 길이 없다. */
    private long plantSeedRun() {
        jdbcClient.sql("""
                        INSERT INTO verification_runs
                                    (as_of, scope, dataset, attempt, origin,
                                     verdict, finding_count, started_at, finished_at)
                        VALUES (:asOf, 'FULL', 'CORRUPT', 9, 'SEED',
                                'FAIL', 800, :at, :at)
                        """)
                .param("asOf", AS_OF)
                .param("at", AS_OF)
                .update();
        return jdbcClient.sql("SELECT id FROM verification_runs WHERE origin = 'SEED'")
                .query(Long.class).single();
    }

    private static JsonNode ruleOf(JsonNode byType, FindingType type) {
        return byType.valueStream()
                .filter(rule -> type.name().equals(rule.path("type").asString()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(type + " 가 응답에 없다"));
    }

    /**
     * <b>{@code PASS 0건} 의 분모다.</b> 그 수가 없으면 <i>"다 보고 못 찾았다"</i> 와
     * <i>"거의 아무것도 안 봤다"</i> 가 응답에서 같은 모양이 된다.
     */
    @Test
    @DisplayName("리포트가 무엇을 몇 건 봤는지 함께 낸다")
    void reportCarriesTheExaminedScale() throws Exception {
        long runId = closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);
        jdbcClient.sql("UPDATE verification_runs "
                        + "SET examined_issuance_count = 3000000, examined_history_count = 5340000 "
                        + "WHERE id = :id")
                .param("id", runId).update();

        JsonNode data = VerifyApiProbe.data(probe.get(CLEAN_FULL));

        assertThat(data.path("examined").path("issuanceCount").asLong()).isEqualTo(3_000_000L);
        assertThat(data.path("examined").path("historyCount").asLong()).isEqualTo(5_340_000L);
    }

    /**
     * <b>없는 것을 0 으로 채우지 않는다.</b> 이 컬럼이 생기기 전 실행이 그 상태이고,
     * 0 으로 내면 <i>"안 봤다"</i> 로 읽힌다 — 이 축이 막으려는 바로 그 오독이다.
     */
    @Test
    @DisplayName("규모를 안 남긴 실행은 null 이다 — 0 이 아니다")
    void missingScaleIsNullNotZero() throws Exception {
        closedRunWithAttempt(DatasetType.CLEAN, VerdictType.PASS, 0, 1);

        JsonNode data = VerifyApiProbe.data(probe.get(CLEAN_FULL));

        assertThat(data.path("examined").isNull())
                .as("0 으로 채우면 '안 봤다' 와 구분이 안 된다")
                .isTrue();
    }
}
