// V2 1인 1매 위반 규칙을 CORRUPT 스키마(제약 셋이 없는) 위에서 검증합니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.storage.db.CorruptRepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>{@link CorruptRepositoryTest} 위에서 돕니다.</b> CLEAN 에서는 {@code uk_coupon_member} 와
 * {@code issuances.code} UNIQUE 가 두 케이스를 물리적으로 막아 <b>심을 수조차 없습니다.</b>
 * 제약이 살아 있는 채로 이 테스트를 쓰면 INSERT 가 튕기고, 그것을 잡지 않으면
 * "검출 0건" 이 정상으로 읽혀 <b>규칙이 틀려도 초록</b>이 됩니다.
 */
@CorruptRepositoryTest
@Import(VerificationRuleJdbcAdapter.class)
class DuplicateIssuanceRuleTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final int LIMIT = 100;

    @Autowired
    private VerificationRuleJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed data;

    @BeforeEach
    void setUp() {
        data = new VerificationSeed(jdbcClient);
    }

    private List<String> keysOf(List<VerificationFinding> findings) {
        return findings.stream().map(f -> f.targetKey()).toList();
    }

    // ─────────────────────────── 케이스 1 · 오염 유형 6 ───────────────────────────

    @Test
    @DisplayName("같은 회원이 같은 회차에서 두 번 받으면 검출한다")
    void detectSameMemberTwice() {
        long couponId = data.currentCouponIdOrCreate();
        long memberId = data.issuanceForNewMember();
        data.issuanceForMember(memberId);

        assertThat(keysOf(adapter.findDuplicateIssuances(AS_OF, LIMIT)))
                .containsExactly("COUPON:" + couponId + "|MEMBER:" + memberId);
    }

    @Test
    @DisplayName("두 번 받아도 검출은 한 행이다 — target_key 가 같아 uk_run_finding 이 막는다")
    void collapseToOneFindingPerCouponMember() {
        long memberId = data.issuanceForNewMember();
        data.issuanceForMember(memberId);
        data.issuanceForMember(memberId);

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT)).hasSize(1);
    }

    @Test
    @DisplayName("회원이 다르면 검출하지 않는다 — 한 회차에 여러 회원은 정상이다")
    void ignoreDifferentMembers() {
        data.issuanceForNewMember();
        data.issuanceForNewMember();

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT)).isEmpty();
    }

    // ─────────────────────────── 케이스 2 · 오염 유형 5 ───────────────────────────

    @Test
    @DisplayName("같은 code 가 다른 회원에게 복제되면 복제본만 검출한다")
    void detectDuplicatedCode() {
        long couponId = data.currentCouponIdOrCreate();
        long originalMember = data.issuanceForNewMember();
        String code = data.currentIssuanceCode();
        long copyMember = data.issuanceForNewMemberWithCode(code);

        assertThat(keysOf(adapter.findDuplicateIssuances(AS_OF, LIMIT)))
                .as("원본 회원까지 나오면 오탐이 오염 수만큼 늘어난다")
                .containsExactly("COUPON:" + couponId + "|MEMBER:" + copyMember)
                .doesNotContain("COUPON:" + couponId + "|MEMBER:" + originalMember);
    }

    /**
     * <b>{@code MIN(id)} 제외를 지우면 여기서 빨개진다.</b> 원본과 복제본이 둘 다 나와
     * 검출이 2행이 되고, 오염 100건이 200건으로 부풀어 집합 비교가 통째로 깨진다.
     */
    @Test
    @DisplayName("code 가 셋이면 원본 하나를 뺀 둘만 검출한다")
    void excludeOnlyTheOriginalAmongThree() {
        data.issuanceForNewMember();
        String code = data.currentIssuanceCode();
        data.issuanceForNewMemberWithCode(code);
        data.issuanceForNewMemberWithCode(code);

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT))
                .as("원본 1건을 뺀 복제본 2건")
                .hasSize(2);
    }

    /**
     * <b>{@code UNION} 을 {@code UNION ALL} 로 바꾸면 여기서 빨개진다.</b> 한 회원이 두 케이스에
     * 다 걸리면 같은 {@code target_key} 가 두 번 나오고, 잡에서는 {@code uk_run_finding}
     * 중복키로 <b>실행 전체가 죽는다.</b> 규칙 층에서 접어야 한다.
     */
    @Test
    @DisplayName("한 회원이 두 케이스에 다 걸려도 검출은 한 행이다")
    void collapseWhenBothCasesHitSameMember() {
        long couponId = data.currentCouponIdOrCreate();
        long memberId = data.issuanceForNewMember();
        String code = data.currentIssuanceCode();
        data.issuanceForMemberWithCode(memberId, code);

        assertThat(keysOf(adapter.findDuplicateIssuances(AS_OF, LIMIT)))
                .containsExactly("COUPON:" + couponId + "|MEMBER:" + memberId);
    }

    /**
     * <b>케이스 2 의 그룹 키에서 {@code coupon_id} 를 빼면 여기서 빨개진다.</b> 회차를 넘나드는
     * code 재사용이 전부 검출로 올라와 오탐이 된다 — 케이스 1 의
     * {@code ignoreSameMemberOnDifferentCoupons} 와 대칭인 규율이다.
     */
    @Test
    @DisplayName("회차가 다르면 같은 code 라도 검출하지 않는다")
    void ignoreSameCodeOnDifferentCoupons() {
        data.issuanceForNewMember();
        String code = data.currentIssuanceCode();
        data.newCoupon();
        data.issuanceForNewMemberWithCode(code);

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT))
                .as("code 중복도 회차 단위 규칙이다")
                .isEmpty();
    }

    // ─────────────────────────── 경계 ───────────────────────────

    @Test
    @DisplayName("정상 발급만 있으면 검출이 없다")
    void recordNothingForCleanData() {
        data.issuanceForNewMember();
        data.issuanceForNewMember();
        data.issuanceForNewMember();

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT)).isEmpty();
    }

    /**
     * <b>집계 안쪽의 {@code updated_at} 조건을 지우면 여기서 빨개진다.</b> asOf 이후 행이
     * {@code COUNT(*)} 를 올려 <b>그 시점에는 없던 중복이 보인다</b> — 재실행 결과가 갈린다.
     */
    @Test
    @DisplayName("asOf 이후에 생긴 두 번째 발급은 세지 않는다")
    void ignoreIssuanceUpdatedAfterAsOf() {
        long memberId = data.issuanceForNewMember();
        data.issuanceForMember(memberId, AS_OF.plusSeconds(1));

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT))
                .as("asOf 시점에는 1건뿐이었다")
                .isEmpty();
    }

    /**
     * <b>케이스 2 <em>바깥쪽</em>의 {@code updated_at} 조건을 지우면 여기서 빨개진다.</b>
     * asOf 이후에 복제된 행은 서브쿼리의 {@code MIN(id)} 계산에서는 빠지지만
     * 바깥 조인에서는 {@code i.id <> d.original_id} 를 만족해 검출로 올라온다 —
     * <b>그 시점에 없던 복제본이 finding 이 되어 재실행이 갈린다.</b>
     */
    @Test
    @DisplayName("asOf 이후에 복제된 code 는 검출하지 않는다")
    void ignoreCodeCopyUpdatedAfterAsOf() {
        data.issuanceForNewMember();
        String code = data.currentIssuanceCode();
        data.issuanceForNewMemberWithCode(code, AS_OF.plusSeconds(1));

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT))
                .as("asOf 시점에는 복제본이 없었다")
                .isEmpty();
    }

    @Test
    @DisplayName("상한을 넘겨 요청해도 그만큼만 돌려준다")
    void respectLimit() {
        List<Long> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            long memberId = data.issuanceForNewMember();
            data.issuanceForMember(memberId);
            members.add(memberId);
        }
        members.sort(null);
        long couponId = data.currentCouponId();

        assertThat(keysOf(adapter.findDuplicateIssuances(AS_OF, 2)))
                .as("몇 건인지만 보면 ORDER BY 를 지워도 초록이라, 어느 건인지까지 못 박는다")
                .containsExactly(
                        "COUPON:" + couponId + "|MEMBER:" + members.get(0),
                        "COUPON:" + couponId + "|MEMBER:" + members.get(1));
    }

    @Test
    @DisplayName("회차가 다르면 같은 회원이라도 검출하지 않는다")
    void ignoreSameMemberOnDifferentCoupons() {
        long memberId = data.issuanceForNewMember();
        data.newCoupon();
        data.issuanceForMember(memberId);

        assertThat(adapter.findDuplicateIssuances(AS_OF, LIMIT))
                .as("1인 1매는 회차 단위 규칙이다")
                .isEmpty();
    }
}
