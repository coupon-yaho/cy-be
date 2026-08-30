package com.kafkick.core.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class VerificationFindingTest {

    @Test
    @DisplayName("회차 단위 검출은 COUPON 키를 만들고 회차 컬럼만 채운다")
    void buildCouponGrain() {
        VerificationFinding finding = VerificationFinding.forCoupon(
                FindingType.STOCK_MISMATCH, 812, "active_count=9998", "집계=10001");

        assertThat(finding.targetKey()).isEqualTo("COUPON:812");
        assertThat(finding.couponId()).isEqualTo(812);
        assertThat(finding.memberId()).isNull();
        assertThat(finding.issuanceId()).isNull();
        assertThat(finding.historyId()).isNull();
    }

    @Test
    @DisplayName("회차·회원 단위 검출은 두 컬럼을 채운다")
    void buildCouponMemberGrain() {
        VerificationFinding finding = VerificationFinding.forCouponMember(
                FindingType.DUP_PER_MEMBER, 812, 9931, "1건", "2건");

        assertThat(finding.targetKey()).isEqualTo("COUPON:812|MEMBER:9931");
        assertThat(finding.couponId()).isEqualTo(812);
        assertThat(finding.memberId()).isEqualTo(9931);
    }

    @Test
    @DisplayName("발급건 단위 검출은 발급건 컬럼만 채운다 — 레거시 이름은 coupon_id 다")
    void buildIssuanceGrain() {
        VerificationFinding finding = VerificationFinding.forIssuance(
                FindingType.REPLAY_MISMATCH, 44210, "replay=USED", "status=ISSUED");

        assertThat(finding.targetKey()).isEqualTo("ISSUANCE:44210");
        assertThat(finding.issuanceId()).isEqualTo(44210);
        assertThat(finding.couponId()).isNull();
    }

    @Test
    @DisplayName("이력 단위 검출은 이력 컬럼만 채운다")
    void buildHistoryGrain() {
        VerificationFinding finding = VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 88131, "USED-EXPIRE->(없음)", "USED-EXPIRE->EXPIRED");

        assertThat(finding.targetKey()).isEqualTo("HISTORY:88131");
        assertThat(finding.historyId()).isEqualTo(88131);
        assertThat(finding.issuanceId()).isNull();
    }

    @Test
    @DisplayName("규칙의 검출 단위와 다른 키를 만들면 거부한다 — 개수는 맞고 키만 어긋나는 게 제일 찾기 어렵다")
    void rejectGrainMismatch() {
        assertThatThrownBy(() -> VerificationFinding.forHistory(
                FindingType.STOCK_MISMATCH, 1, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("검출 단위와 키 형식이 다릅니다");
    }

    @ParameterizedTest
    @EnumSource(FindingType.class)
    @DisplayName("여섯 규칙 모두 자기 단위의 팩토리로만 만들어진다")
    void buildEveryFindingTypeThroughItsGrain(FindingType type) {
        VerificationFinding finding = switch (type.grain()) {
            case COUPON -> VerificationFinding.forCoupon(type, 1, "a", "b");
            case COUPON_MEMBER -> VerificationFinding.forCouponMember(type, 1, 2, "a", "b");
            case ISSUANCE -> VerificationFinding.forIssuance(type, 1, "a", "b");
            case HISTORY -> VerificationFinding.forHistory(type, 1, "a", "b");
        };

        assertThat(finding.type()).isEqualTo(type);
        assertThat(finding.targetKey()).hasSizeLessThanOrEqualTo(TargetKey.MAX_LENGTH);
    }

    @Test
    @DisplayName("증적이 비면 거부한다 — 없으면 리포트가 무엇이 이상한지까지만 말한다")
    void rejectBlankEvidence() {
        assertThatThrownBy(() -> VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 1, " ", "b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("기대값이 필요합니다");
    }

    @Test
    @DisplayName("증적이 컬럼 길이를 넘으면 거부한다 — 잘라 넣으면 근거가 조용히 사라진다")
    void rejectTooLongEvidence() {
        assertThatThrownBy(() -> VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION, 1, "a",
                "b".repeat(VerificationFinding.EVIDENCE_MAX_LENGTH + 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("자를 넘을 수 없습니다");
    }

    @Test
    @DisplayName("키와 식별자 컬럼이 다른 대상을 가리키면 거부한다 — 형식만 맞아도 안 된다")
    void rejectKeyPointingElsewhere() {
        assertThatThrownBy(() -> new VerificationFinding(
                FindingType.REPLAY_MISMATCH, "ISSUANCE:5",
                null, null, 7L, null, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("다른 대상을 가리킵니다");
    }

    @Test
    @DisplayName("검출 단위가 쓰지 않는 컬럼이 채워지면 거부한다 — 인자를 한 칸 밀어 넣은 행이다")
    void rejectUnusedColumnFilled() {
        assertThatThrownBy(() -> new VerificationFinding(
                FindingType.REPLAY_MISMATCH, "ISSUANCE:5",
                99L, null, 5L, null, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("쓰지 않는 식별자 컬럼");
    }

    @Test
    @DisplayName("식별자 컬럼이 비면 거부한다")
    void rejectMissingIdColumn() {
        assertThatThrownBy(() -> new VerificationFinding(
                FindingType.REPLAY_MISMATCH, "ISSUANCE:5",
                null, null, null, null, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("발급건 ID가 필요합니다");
    }

    @Test
    @DisplayName("식별자가 0 이하면 거부한다")
    void rejectNonPositiveId() {
        assertThatThrownBy(() -> VerificationFinding.forIssuance(
                FindingType.REPLAY_MISMATCH, 0, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
