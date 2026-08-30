// target_key 형식을 검증합니다. 시드가 기록한 expected_findings 와 글자 단위로 맞아야 합니다.
package com.kafkick.core.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TargetKeyTest {

    @Test
    @DisplayName("V1 재고 정합은 회차 단위 키를 만든다")
    void buildCouponKey() {
        assertThat(TargetKey.coupon(812)).isEqualTo("COUPON:812");
    }

    @Test
    @DisplayName("V2 는 회차와 회원을 파이프로 이어 붙인다")
    void buildCouponMemberKey() {
        assertThat(TargetKey.couponMember(812, 9931))
                .isEqualTo("COUPON:812|MEMBER:9931");
    }

    @Test
    @DisplayName("V3·V5·V6 은 발급건 단위 키를 만든다")
    void buildIssuanceKey() {
        assertThat(TargetKey.issuance(44210)).isEqualTo("ISSUANCE:44210");
    }

    @Test
    @DisplayName("V4 는 이력 행 단위 키를 만든다")
    void buildHistoryKey() {
        assertThat(TargetKey.history(88131)).isEqualTo("HISTORY:88131");
    }

    @Test
    @DisplayName("구 어휘 CAMPAIGN 접두사를 쓰지 않는다 — 시드와 어긋나면 집합이 100% 불일치한다")
    void doNotUseLegacyCouponRoundPrefix() {
        assertThat(TargetKey.coupon(812)).doesNotStartWith("CAMPAIGN:");
        assertThat(TargetKey.couponMember(812, 9931)).doesNotContain("CAMPAIGN:");
    }

    @Test
    @DisplayName("발급건 키는 COUPON 이 아니라 ISSUANCE 로 시작한다 — 구 어휘에서 뜻이 뒤집혔다")
    void doNotUseCouponPrefixForIssuance() {
        assertThat(TargetKey.issuance(44210)).startsWith("ISSUANCE:");
    }

    @Test
    @DisplayName("가장 긴 키도 컬럼 길이 64를 넘지 않는다")
    void fitWithinColumnLength() {
        String longest = TargetKey.couponMember(Long.MAX_VALUE, Long.MAX_VALUE);

        assertThat(longest.length()).isLessThanOrEqualTo(TargetKey.MAX_LENGTH);
    }

    @Test
    @DisplayName("0 이하 식별자는 거부한다")
    void rejectNonPositiveId() {
        assertThatThrownBy(() -> TargetKey.coupon(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("회차 ID는 0보다 커야 합니다.");

        assertThatThrownBy(() -> TargetKey.issuance(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("발급건 ID는 0보다 커야 합니다.");

        assertThatThrownBy(() -> TargetKey.history(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("이력 ID는 0보다 커야 합니다.");

        assertThatThrownBy(() -> TargetKey.couponMember(812, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("회원 ID는 0보다 커야 합니다.");
    }
}
