// 정답 매니페스트 대조가 집합 차를 정확히 내는지 확인합니다.
package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingKey;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>합격 조건은 건수가 아니라 집합 일치입니다.</b> 오탐 400 + 누락 400 도 800 이라,
 * 개수만 보면 정확히 검출한 것과 구분되지 않습니다. 그래서 양방향으로 봅니다.
 *
 * <p>이 클래스가 지키는 가장 중요한 것은 <b>조인 키</b>입니다. 식별자 컬럼으로 조인하면
 * {@code NULL = NULL} 이 UNKNOWN 이라 정확히 검출한 행까지 누락으로 뒤집히는데,
 * 그 사고는 "누락이 잔뜩 나온다" 는 모양이라 <b>규칙을 의심하게 만듭니다.</b>
 */
@RepositoryTest
@Import({ExpectedFindingJdbcAdapter.class, VerificationFindingJdbcAdapter.class,
        VerificationRunJdbcAdapter.class})
class ExpectedFindingJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final long SEED_RUN = 1L;

    @Autowired
    private ExpectedFindingJdbcAdapter adapter;

    @Autowired
    private VerificationFindingJdbcAdapter findings;

    @Autowired
    private VerificationRunJdbcAdapter runs;

    @Autowired
    private JdbcClient jdbcClient;

    private long runId;

    @BeforeEach
    void setUp() {
        runId = runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CORRUPT, 1, AS_OF)).id();
    }

    /** 정답 한 건. 식별자 컬럼은 규칙마다 다르게 채워 실제 모양을 흉내 낸다. */
    private void expected(FindingType type, String targetKey, Long campaignId, Long memberId) {
        jdbcClient.sql("""
                        INSERT INTO expected_findings
                            (seed_run_id, corrupt_type, finding_type, target_key,
                             campaign_id, member_id, note, created_at)
                        VALUES (:seedRunId, 1, :findingType, :targetKey,
                                :campaignId, :memberId, '-', :createdAt)
                        """)
                .param("seedRunId", SEED_RUN)
                .param("findingType", type.name())
                .param("targetKey", targetKey)
                .param("campaignId", campaignId)
                .param("memberId", memberId)
                .param("createdAt", AS_OF)
                .update();
    }

    private List<String> keysOf(List<FindingKey> found) {
        return found.stream().map(FindingKey::toString).toList();
    }

    @Test
    @DisplayName("검출이 정답과 같으면 양쪽 다 비어 있다")
    void reportNothingWhenSetsMatch() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 7L, "a", "b")));

        assertThat(adapter.missing(runId, SEED_RUN)).isEmpty();
        assertThat(adapter.unexpected(runId, SEED_RUN)).isEmpty();
    }

    @Test
    @DisplayName("정답에 있는데 못 잡으면 누락으로 나온다")
    void reportMissing() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        expected(FindingType.STOCK_MISMATCH, "COUPON:9", 9L, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 7L, "a", "b")));

        assertThat(keysOf(adapter.missing(runId, SEED_RUN)))
                .containsExactly("STOCK_MISMATCH:COUPON:9");
        assertThat(adapter.unexpected(runId, SEED_RUN)).isEmpty();
    }

    @Test
    @DisplayName("정답에 없는 것을 잡으면 오탐으로 나온다")
    void reportUnexpected() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 7L, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 8L, "a", "b")));

        assertThat(adapter.missing(runId, SEED_RUN)).isEmpty();
        assertThat(keysOf(adapter.unexpected(runId, SEED_RUN)))
                .containsExactly("STOCK_MISMATCH:COUPON:8");
    }

    /**
     * <b>한 방향만 보면 이 경우가 통과한다.</b> 누락 1 · 오탐 1 인데 개수는 정답과 같다 —
     * 오탐 400 + 누락 400 도 800 이라는 말이 이 모양이다.
     */
    @Test
    @DisplayName("개수가 같아도 집합이 다르면 양쪽 다 나온다")
    void reportBothWhenCountsMatchButSetsDiffer() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 8L, "a", "b")));

        assertThat(keysOf(adapter.missing(runId, SEED_RUN)))
                .containsExactly("STOCK_MISMATCH:COUPON:7");
        assertThat(keysOf(adapter.unexpected(runId, SEED_RUN)))
                .containsExactly("STOCK_MISMATCH:COUPON:8");
    }

    /**
     * <b>이 티켓의 함정이다.</b> V1 은 {@code campaign_id} 만, V4 는 {@code history_id} 만 채운다.
     * 식별자 컬럼으로 조인하면 안 쓰는 쪽이 양쪽 다 NULL 이고 SQL 에서 {@code NULL = NULL} 은
     * UNKNOWN 이라 <b>정확히 검출한 행까지 전부 누락으로 뒤집힌다.</b>
     *
     * <p>그 사고는 "누락이 잔뜩 나온다" 는 모양이라 규칙을 의심하게 만든다 — 원인은 조인이다.
     */
    @Test
    @DisplayName("식별자 컬럼이 NULL 이어도 대조가 성립한다 — 다형 FK 로 조인하면 안 되는 이유")
    void matchEvenWhenIdentifierColumnsAreNull() {
        expected(FindingType.ILLEGAL_TRANSITION, "HISTORY:3", null, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forHistory(FindingType.ILLEGAL_TRANSITION, 3L, "a", "b")));

        assertThat(adapter.missing(runId, SEED_RUN))
                .as("campaign_id·member_id 가 양쪽 다 NULL 인데 누락으로 잡히면 조인이 틀린 것이다")
                .isEmpty();
        assertThat(adapter.unexpected(runId, SEED_RUN)).isEmpty();
    }

    @Test
    @DisplayName("같은 키라도 검출 종류가 다르면 서로 다른 것이다")
    void separateByFindingType() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forCouponMember(
                        FindingType.DUP_PER_MEMBER, 7L, 2L, "a", "b")));

        assertThat(keysOf(adapter.missing(runId, SEED_RUN)))
                .containsExactly("STOCK_MISMATCH:COUPON:7");
        assertThat(keysOf(adapter.unexpected(runId, SEED_RUN)))
                .containsExactly("DUP_PER_MEMBER:COUPON:7|MEMBER:2");
    }

    @Test
    @DisplayName("다른 정답 묶음은 섞이지 않는다")
    void isolateBySeedRun() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 7L, "a", "b")));

        assertThat(keysOf(adapter.unexpected(runId, 99L)))
                .as("없는 묶음과 대조하면 검출 전부가 오탐이 된다")
                .containsExactly("STOCK_MISMATCH:COUPON:7");
        assertThat(adapter.exists(99L)).isFalse();
        assertThat(adapter.exists(SEED_RUN)).isTrue();
    }

    /** 다른 실행의 검출은 이 실행의 대조에 안 들어간다. */
    @Test
    @DisplayName("다른 실행의 검출은 섞이지 않는다")
    void isolateByRun() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        long other = runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CORRUPT, 2, AS_OF)).id();
        findings.appendAll(other, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 7L, "a", "b")));

        assertThat(keysOf(adapter.missing(runId, SEED_RUN)))
                .as("다른 실행이 잡은 것은 이 실행의 검출이 아니다")
                .containsExactly("STOCK_MISMATCH:COUPON:7");
    }
}
