package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class VerificationFindingJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired
    private VerificationFindingJdbcAdapter adapter;

    @Autowired
    private VerificationRunJdbcAdapter runAdapter;

    @Autowired
    private JdbcClient jdbcClient;

    private long runId;

    @BeforeEach
    void setUp() {
        runId = newRun(1);
    }

    private long newRun(int attempt) {
        return runAdapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, AS_OF)).id();
    }

    @Test
    @DisplayName("검출 결과를 쌓으면 그대로 들어간다")
    void appendFinding() {
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131,
                "USED-EXPIRE->(없음)", "USED-EXPIRE->EXPIRED")));

        Map<String, Object> row = findByTargetKey("HISTORY:88131");
        assertThat(row.get("finding_type")).isEqualTo("ILLEGAL_TRANSITION");
        assertThat(row.get("expected")).isEqualTo("USED-EXPIRE->(없음)");
        assertThat(row.get("actual")).isEqualTo("USED-EXPIRE->EXPIRED");
    }

    @Test
    @DisplayName("이력 단위 검출은 history_id 만 채운다 — 나머지 다형 컬럼은 NULL 이다")
    void fillOnlyHistoryColumn() {
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));

        Map<String, Object> row = findByTargetKey("HISTORY:88131");
        assertThat(row.get("history_id")).isEqualTo(88131L);
        assertThat(row.get("campaign_id")).isNull();
        assertThat(row.get("member_id")).isNull();
        assertThat(row.get("coupon_id")).isNull();
    }

    @Test
    @DisplayName("회차 검출은 campaign_id 에 들어간다 — 레거시 컬럼명이 회차를 가리킨다")
    void mapCouponToLegacyCouponRoundColumn() {
        adapter.appendAll(runId, List.of(VerificationFinding.forCoupon(
                FindingType.STOCK_MISMATCH, 812, "active_count=9998", "집계=10001")));

        Map<String, Object> row = findByTargetKey("COUPON:812");
        assertThat(row.get("campaign_id")).isEqualTo(812L);
        assertThat(row.get("coupon_id")).isNull();
    }

    @Test
    @DisplayName("발급건 검출은 coupon_id 에 들어간다 — 레거시 컬럼명이 발급건을 가리킨다")
    void mapIssuanceToLegacyCouponColumn() {
        adapter.appendAll(runId, List.of(VerificationFinding.forIssuance(
                FindingType.REPLAY_MISMATCH, 44210, "replay=USED", "status=ISSUED")));

        Map<String, Object> row = findByTargetKey("ISSUANCE:44210");
        assertThat(row.get("coupon_id")).isEqualTo(44210L);
        assertThat(row.get("campaign_id")).isNull();
    }

    @Test
    @DisplayName("회차·회원 검출은 두 컬럼을 함께 채운다")
    void fillCouponAndMemberColumns() {
        adapter.appendAll(runId, List.of(VerificationFinding.forCouponMember(
                FindingType.DUP_PER_MEMBER, 812, 9931, "1건", "2건")));

        Map<String, Object> row = findByTargetKey("COUPON:812|MEMBER:9931");
        assertThat(row.get("campaign_id")).isEqualTo(812L);
        assertThat(row.get("member_id")).isEqualTo(9931L);
    }

    @Test
    @DisplayName("같은 검출을 다시 쌓아도 죽지 않고 한 행으로 남는다 — 청크가 죽은 지점부터 다시 돈다")
    void rewriteOnRestart() {
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));
        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "c", "d")));

        assertThat(countOf(runId)).isEqualTo(1);
        assertThat(findByTargetKey("HISTORY:88131").get("expected")).isEqualTo("c");
    }

    @Test
    @DisplayName("같은 대상이라도 규칙이 다르면 다른 행이다 — 유형 3 이 V1 과 V4 를 함께 울린다")
    void keepSeparateRowsPerFindingType() {
        adapter.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 812, "a", "b"),
                VerificationFinding.forCouponMember(FindingType.DUP_PER_MEMBER, 812, 9931, "c", "d")));

        assertThat(countOf(runId)).isEqualTo(2);
    }

    @Test
    @DisplayName("다른 run 의 같은 검출은 따로 쌓인다 — run 마다 네임스페이스가 갈린다")
    void keepFindingsPerRun() {
        long otherRunId = newRun(2);

        adapter.appendAll(runId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));
        adapter.appendAll(otherRunId, List.of(VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "a", "b")));

        assertThat(countOf(runId)).isEqualTo(1);
        assertThat(countOf(otherRunId)).isEqualTo(1);
    }

    @Test
    @DisplayName("빈 목록은 아무것도 쓰지 않는다")
    void appendNothingForEmptyList() {
        adapter.appendAll(runId, List.of());

        assertThat(countOf(runId)).isZero();
    }

    @Test
    @DisplayName("한 묶음에 800행을 넣어도 다 들어간다 — 오염셋 정답이 그 규모다")
    void appendCorruptSetSizedBatch() {
        List<VerificationFinding> findings = LongStream.rangeClosed(1, 800)
                .mapToObj(id -> VerificationFinding.forHistory(
                        FindingType.ILLEGAL_TRANSITION, id, "a", "b"))
                .toList();

        adapter.appendAll(runId, findings);

        assertThat(countOf(runId)).isEqualTo(800);
    }

    /**
     * <b>800행은 분할 경계를 넘지 않는다.</b> {@code BATCH_SIZE} 가 1000 이라 위 테스트는
     * 루프를 <b>한 번만</b> 돌리고 끝난다 — 두 번째 묶음의 오프셋 계산은 한 번도 실행된 적이 없었다.
     *
     * <p>규칙당 상한 기본값이 10000 이라 실제 실행은 경계를 쉽게 넘는다.
     */
    @Test
    @DisplayName("분할 경계를 넘겨도 다 들어간다 — 두 번째 묶음이 실제로 돈다")
    void appendAcrossBatchBoundary() {
        int size = 1_001;
        List<VerificationFinding> findings = LongStream.rangeClosed(1, size)
                .mapToObj(id -> VerificationFinding.forHistory(
                        FindingType.ILLEGAL_TRANSITION, id, "a", "b"))
                .toList();

        adapter.appendAll(runId, findings);

        assertThat(keysOf(runId))
                .as("건수만 보면 누락과 어긋난 키가 상쇄돼 통과한다 — 이 PR 이 판정에서 버린 바로 그 논리다")
                .containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(1, size)
                        .mapToObj(id -> FindingType.ILLEGAL_TRANSITION + ":HISTORY:" + id)
                        .toList());
    }

    /**
     * <b>제출용 리포트가 읽는 축이다.</b> 판정의 검출 수와 이 합계가 갈리면
     * {@code finding_type} 에 규칙 목록 밖의 값이 들어갔다는 뜻이고, 그때 봐야 할 것은
     * 리포트가 아니라 규칙 쪽이다.
     */
    @Test
    @DisplayName("규칙별로 세면 합계가 전체 검출 수와 같다")
    void countByTypeSumsToTotal() {
        adapter.appendAll(runId, List.of(
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 1, "a", "b"),
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 2, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 3, "a", "b")));

        Map<FindingType, Integer> byType = adapter.countByType(runId);

        assertThat(byType)
                .containsEntry(FindingType.ILLEGAL_TRANSITION, 2)
                .containsEntry(FindingType.STOCK_MISMATCH, 1);
        assertThat(byType.values().stream().mapToInt(Integer::intValue).sum())
                .as("합계가 countOf 와 달라지면 전이표 밖의 finding_type 이 들어간 것이다")
                .isEqualTo(adapter.countOf(runId));
    }

    /**
     * <b>검출이 0인 규칙은 여기서 안 나온다.</b> {@code GROUP BY} 가 없는 것을 못 만들어서다 —
     * 여섯 규칙을 다 보여 주는 것은 {@code VerifyReportView} 의 몫이고, 그 경계를 여기 못 박는다.
     */
    @Test
    @DisplayName("검출이 없는 규칙은 결과에 없다 — 채우는 것은 저장소 일이 아니다")
    void omitsRulesWithoutFindings() {
        adapter.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 3, "a", "b")));

        assertThat(adapter.countByType(runId))
                .hasSize(1)
                .containsOnlyKeys(FindingType.STOCK_MISMATCH);
    }

    /**
     * <b>{@code finding_type} 에 CHECK 제약이 없다</b>({@code varchar(40)} + 주석뿐).
     * 규칙을 하나 더해 행을 쓴 뒤 코드를 되돌리면 이 상태가 된다.
     *
     * <p>{@code valueOf} 를 그냥 부르면 {@code IllegalArgumentException} 이 올라가고,
     * <b>제출물 조회가 500 + 스프링 기본 본문</b>으로 끝난다 — 원인이 어디에도 안 남아
     * 판정을 아예 못 읽는다. 도메인 예외로 바꿔 봉투에 코드를 싣는다.
     *
     * <p><b>조용히 건너뛰면 안 된다.</b> 그러면 규칙별 검출 수가 실제보다 적어지고,
     * 그 리포트가 합격 증거로 쓰인다.
     */
    @Test
    @DisplayName("모르는 규칙 이름이 섞이면 도메인 예외다 — 500 으로 죽으면 원인이 안 남는다")
    void rejectsUnknownFindingType() {
        jdbcClient.sql("""
                        INSERT INTO verification_findings
                                    (run_id, finding_type, target_key, expected, actual)
                        VALUES (:runId, 'V7_FROM_THE_FUTURE', 'COUPON:1', 'e', 'a')
                        """)
                .param("runId", runId)
                .update();

        assertThatThrownBy(() -> adapter.countByType(runId))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("V7_FROM_THE_FUTURE");
    }

    @Test
    @DisplayName("남의 실행 검출은 안 센다")
    void countsOnlyThisRun() {
        long other = newRun(2);
        adapter.appendAll(other, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 3, "a", "b")));

        assertThat(adapter.countByType(runId))
                .as("run_id 조건이 빠지면 제출물이 남의 판정을 싣는다")
                .isEmpty();
    }

    private Map<String, Object> findByTargetKey(String targetKey) {
        return jdbcClient.sql("""
                        SELECT finding_type, target_key, campaign_id, member_id,
                               coupon_id, history_id, expected, actual
                          FROM verification_findings
                         WHERE run_id = :runId AND target_key = :targetKey
                        """)
                .param("runId", runId)
                .param("targetKey", targetKey)
                .query()
                .singleRow();
    }

    /**
     * 검출을 <b>{@code (finding_type, target_key)} 쌍</b>으로 전부. 건수가 아니라 집합을 본다.
     *
     * <p>키가 쌍인 것은 이 저장소 전체의 어휘다 — {@code uk_run_finding} 도, checksum 입력도,
     * 정답 매니페스트 조인도 그 쌍이다. {@code target_key} 만 보면 종류가 틀려도 통과한다.
     * 표기는 {@code ExpectedFindingJdbcAdapterTest} 와 같은 {@code FindingKey#toString} 이다.
     */
    private List<String> keysOf(long targetRunId) {
        return jdbcClient.sql("""
                        SELECT finding_type, target_key
                          FROM verification_findings WHERE run_id = :runId
                        """)
                .param("runId", targetRunId)
                .query((rs, rowNum) ->
                        rs.getString("finding_type") + ":" + rs.getString("target_key"))
                .list();
    }

    private int countOf(long targetRunId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM verification_findings WHERE run_id = :runId")
                .param("runId", targetRunId)
                .query(Integer.class)
                .single();
    }
}
