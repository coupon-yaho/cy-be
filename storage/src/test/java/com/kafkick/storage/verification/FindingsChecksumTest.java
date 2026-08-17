// 검출 checksum 이 계약 인코딩과 바이트 단위로 같은지 확인합니다.
package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;

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
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>재실행 결정론 판정의 근거가 이 값입니다.</b> 판정표가 읽는 것은
 * {@code dataset_fingerprint} 와 이 checksum 의 <b>조합</b>이고, 그중
 * <i>"지문은 같은데 checksum 이 다르다 → 검증기 버그"</i> 가 이 프로젝트가 진짜로 잡고 싶은 칸입니다.
 * 이 값이 계약과 어긋나면 그 칸이 통째로 무의미해집니다.
 *
 * <p>그래서 <b>"두 번 계산하면 같다" 로는 부족합니다.</b> 잘못된 인코딩도 자기 자신과는 항상 같습니다.
 * 계약({@code docs/contract.json} 의 {@code findings_checksum})이 정한 바이트열을
 * 테스트가 <b>독립적으로 만들어</b> 대조합니다.
 */
@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class FindingsChecksumTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private VerificationFindingJdbcAdapter findings;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private VerificationRunJdbcAdapter runs;

    private long runId;
    private int nextAttempt;

    @BeforeEach
    void setUp() {
        new VerificationSeed(jdbcClient);
        nextAttempt = 1;
        runId = newRun();
    }

    /** 검출은 {@code run_id} 를 FK 로 문다. 실행 행이 먼저 있어야 한다. */
    private long newRun() {
        return runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, nextAttempt++, AS_OF)).id();
    }

    /** 계약 인코딩을 손으로 만든다 — 구현과 같은 코드를 부르면 대조가 아니라 반복이다. */
    private static String contractChecksum(List<String[]> sortedRows) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String[] row : sortedRows) {
                digest.update(row[0].getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0x1f);
                digest.update(row[1].getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0x1e);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("계약이 정한 인코딩과 같다 — 두 번 계산해 같은 것만으로는 못 잡는다")
    void matchContractEncoding() {
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 7L, "a", "b"),
                VerificationFinding.forIssuance(FindingType.REPLAY_MISMATCH, 3L, "c", "d")));

        assertThat(findings.checksumOf(runId)).isEqualTo(contractChecksum(List.of(
                new String[]{"REPLAY_MISMATCH", "ISSUANCE:3"},
                new String[]{"STOCK_MISMATCH", "COUPON:7"})));
    }

    /**
     * <b>여섯 규칙 중 V2 만 구분자 {@code |} 를 갖는다 — 정렬이 갈리는 유일한 모양이다.</b>
     *
     * <p>MySQL 기본 콜레이션({@code utf8mb4_0900_ai_ci})은 UCA 순서라 {@code |}(U+007C)를
     * 숫자보다 앞에 두고, 참조 구현의 파이썬 {@code sorted()} 는 코드포인트 순서라 반대다.
     * 기대값을 <b>코드포인트 순서로 손으로 적는다</b> — DB 에서 뽑아 만들면 다시 콜레이션에 묶인다.
     *
     * <p>회차 id 자릿수가 섞여야 드러난다. 오염셋은 1~291 이라 실제로 섞인다.
     */
    @Test
    @DisplayName("V2 키의 정렬이 참조 구현과 같다 — 콜레이션이 아니라 코드포인트 순서다")
    void sortDuplicateKeysByCodePoint() {
        findings.appendAll(runId, List.of(
                VerificationFinding.forCouponMember(FindingType.DUP_PER_MEMBER, 1L, 2L, "a", "b"),
                VerificationFinding.forCouponMember(FindingType.DUP_PER_MEMBER, 11L, 2L, "a", "b"),
                VerificationFinding.forCouponMember(FindingType.DUP_PER_MEMBER, 2L, 9L, "a", "b")));

        assertThat(findings.checksumOf(runId))
                .as("콜레이션 순서면 COUPON:1 이 COUPON:11 보다 앞서 다른 값이 나온다")
                .isEqualTo(contractChecksum(List.of(
                        new String[]{"DUP_PER_MEMBER", "COUPON:11|MEMBER:2"},
                        new String[]{"DUP_PER_MEMBER", "COUPON:1|MEMBER:2"},
                        new String[]{"DUP_PER_MEMBER", "COUPON:2|MEMBER:9"})));
    }

    /**
     * <b>정렬이 계약의 일부다.</b> 같은 집합이라도 순서가 다르면 다른 값이 나오므로,
     * 넣은 순서와 무관하게 {@code (finding_type, target_key)} 오름차순이어야 한다.
     */
    @Test
    @DisplayName("넣은 순서가 달라도 같은 checksum 이 나온다")
    void ignoreInsertOrder() {
        findings.appendAll(runId, List.of(
                VerificationFinding.forIssuance(FindingType.REPLAY_MISMATCH, 9L, "a", "b"),
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1L, "c", "d")));
        String first = findings.checksumOf(runId);

        long other = newRun();
        findings.appendAll(other, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1L, "c", "d"),
                VerificationFinding.forIssuance(FindingType.REPLAY_MISMATCH, 9L, "a", "b")));

        assertThat(findings.checksumOf(other)).isEqualTo(first);
    }

    @Test
    @DisplayName("검출이 다르면 checksum 도 다르다")
    void detectDifferentFindings() {
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1L, "a", "b")));
        String first = findings.checksumOf(runId);

        long other = newRun();
        findings.appendAll(other, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2L, "a", "b")));

        assertThat(findings.checksumOf(other)).isNotEqualTo(first);
    }

    /**
     * <b>정상셋의 0건은 판정 대상이지 미실행이 아니다.</b> {@code null} 을 돌려주면
     * {@code NULL = NULL} 이 UNKNOWN 이라 두 실행이 같은지 물을 수 없게 된다.
     */
    @Test
    @DisplayName("검출이 없으면 빈 입력의 SHA-256 이다 — null 이 아니다")
    void useEmptyDigestForNoFindings() {
        assertThat(findings.checksumOf(runId))
                .isEqualTo(contractChecksum(List.of()))
                .isNotNull();
    }

    /** 증적(expected/actual)은 계약 입력이 아니다. 섞이면 문구만 바꿔도 판정이 갈린다. */
    @Test
    @DisplayName("증적 문구가 달라도 checksum 은 같다 — 입력은 두 컬럼뿐이다")
    void ignoreEvidenceText() {
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1L, "기대", "실제")));
        String first = findings.checksumOf(runId);

        long other = newRun();
        findings.appendAll(other, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1L, "다른 문구", "또 다른 문구")));

        assertThat(findings.checksumOf(other)).isEqualTo(first);
    }

    @Test
    @DisplayName("다른 실행의 검출은 섞이지 않는다")
    void isolateByRun() {
        findings.appendAll(runId, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1L, "a", "b")));
        long other = newRun();
        findings.appendAll(other, List.of(
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 2L, "a", "b")));

        assertThat(findings.countOf(runId)).isEqualTo(1);
        assertThat(findings.checksumOf(runId)).isNotEqualTo(findings.checksumOf(other));
    }

    /**
     * <b>{@code uk_run_finding} 이 접은 중복은 개수에도 안 잡혀야 한다.</b> 규칙 Step 이 센 것을
     * 더하면 checksum 과 어긋난 수가 기록되어, 둘을 함께 읽는 판정이 설명 안 되는 상태가 된다.
     */
    @Test
    @DisplayName("같은 검출을 두 번 써도 개수는 하나다")
    void countStoredRowsNotAppends() {
        VerificationFinding same =
                VerificationFinding.forCoupon(FindingType.STOCK_MISMATCH, 1L, "a", "b");
        findings.appendAll(runId, List.of(same));
        findings.appendAll(runId, List.of(same));

        assertThat(findings.countOf(runId)).isEqualTo(1);
    }
}
