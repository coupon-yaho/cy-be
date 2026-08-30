// 정답 매니페스트 대조가 집합 차를 정확히 내는지 확인합니다.
package com.kafkick.storage.db.verification;

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
    private void expected(FindingType type, String targetKey, Long couponId, Long memberId) {
        jdbcClient.sql("""
                        INSERT INTO expected_findings
                            (seed_run_id, corrupt_type, finding_type, target_key,
                             campaign_id, member_id, note, created_at)
                        VALUES (:seedRunId, 1, :findingType, :targetKey,
                                :couponId, :memberId, '-', :createdAt)
                        """)
                .param("seedRunId", SEED_RUN)
                .param("findingType", type.name())
                .param("targetKey", targetKey)
                .param("couponId", couponId)
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

    /**
     * <b>대소문자만 달라도 다른 키다.</b> 컬럼 콜레이션이 {@code utf8mb4_0900_ai_ci} 라
     * 조인 등호가 대소문자를 무시한다 — 그대로 두면 어긋난 키가 <b>매칭으로 세어져 거짓 PASS</b>
     * 가 난다. 검증기가 내면 안 되는 방향의 오류다.
     *
     * <p>참조 구현({@code cy-seed} 의 파이썬 {@code set} 차)은 바이트 정확이라, 여기서
     * 느슨하면 두 구현이 같은 데이터에 다른 판정을 낸다.
     */
    @Test
    @DisplayName("키가 대소문자만 달라도 누락·오탐으로 잡는다 — 콜레이션이 매칭으로 세면 거짓 PASS 다")
    void separateKeysDifferingOnlyByCase() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 7L, "a", "b")));
        jdbcClient.sql("UPDATE verification_findings SET target_key = 'coupon:7' "
                        + "WHERE run_id = :runId")
                .param("runId", runId)
                .update();

        assertThat(keysOf(adapter.missing(runId, SEED_RUN)))
                .as("정답 COUPON:7 을 잡은 검출이 없다")
                .containsExactly("STOCK_MISMATCH:COUPON:7");
        assertThat(keysOf(adapter.unexpected(runId, SEED_RUN)))
                .as("검출 coupon:7 은 정답에 없다")
                .containsExactly("STOCK_MISMATCH:coupon:7");
    }

    /**
     * <b>판정 입력을 얼리는 값이다.</b> 데이터 네 축은 {@code assertFrozenStep} 이 얼리는데
     * 매니페스트는 그 뒤에 읽혀, 실행 중에 주입을 다시 돌리면 같은 데이터에 다른 판정이 나온다.
     * 지문이 <b>내용에만</b> 반응해야 그 상황을 가릴 수 있다.
     */
    @Test
    @DisplayName("같은 집합이면 같은 지문, 한 건만 늘어도 달라진다")
    void foldManifestIntoOneValue() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        String before = adapter.digestOf(SEED_RUN);

        assertThat(adapter.digestOf(SEED_RUN))
                .as("정상 재실행이 거부되면 안 된다")
                .isEqualTo(before);

        expected(FindingType.STOCK_MISMATCH, "COUPON:9", 9L, null);

        assertThat(adapter.digestOf(SEED_RUN)).isNotEqualTo(before);
    }

    /**
     * <b>내용이 같으면 묶음 번호가 달라도 같은 값이다.</b> {@code seed_run_id} 나 행 {@code id} 가
     * 재료에 섞이면 "같은 정답인가" 를 물을 수 없고, 지문이 아니라 그냥 일련번호가 된다.
     */
    @Test
    @DisplayName("지문은 내용에만 반응한다 — 묶음 번호도 삽입 순서도 안 탄다")
    void reactToContentOnly() {
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);
        expected(FindingType.DUP_PER_MEMBER, "COUPON:7|MEMBER:2", 7L, 2L);
        String first = adapter.digestOf(SEED_RUN);

        // 같은 두 건을 다른 묶음에 반대 순서로 심는다.
        for (Object[] row : new Object[][] {
                {FindingType.DUP_PER_MEMBER, "COUPON:7|MEMBER:2"},
                {FindingType.STOCK_MISMATCH, "COUPON:7"}}) {
            jdbcClient.sql("""
                            INSERT INTO expected_findings
                                (seed_run_id, corrupt_type, finding_type, target_key,
                                 note, created_at)
                            VALUES (2, 1, :findingType, :targetKey, '-', :createdAt)
                            """)
                    .param("findingType", ((FindingType) row[0]).name())
                    .param("targetKey", row[1])
                    .param("createdAt", AS_OF)
                    .update();
        }

        assertThat(adapter.digestOf(2L)).isEqualTo(first);
    }

    /** 없는 묶음도 값을 낸다 — 부재는 {@code exists} 가 따로 가른다. */
    @Test
    @DisplayName("빈 묶음과 내용이 있는 묶음은 지문이 다르다")
    void distinguishEmptyManifest() {
        String empty = adapter.digestOf(99L);
        expected(FindingType.STOCK_MISMATCH, "COUPON:7", 7L, null);

        assertThat(adapter.digestOf(SEED_RUN)).isNotEqualTo(empty);
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
