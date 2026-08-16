package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

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
        runId = runAdapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 1, AS_OF)).id();
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
    void mapCouponToLegacyCampaignColumn() {
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
        long otherRunId = runAdapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 2, AS_OF)).id();

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
        List<VerificationFinding> findings = java.util.stream.LongStream.rangeClosed(1, 800)
                .mapToObj(id -> VerificationFinding.forHistory(
                        FindingType.ILLEGAL_TRANSITION, id, "a", "b"))
                .toList();

        adapter.appendAll(runId, findings);

        assertThat(countOf(runId)).isEqualTo(800);
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

    private int countOf(long targetRunId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM verification_findings WHERE run_id = :runId")
                .param("runId", targetRunId)
                .query(Integer.class)
                .single();
    }
}
