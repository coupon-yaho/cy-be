// 발급 시점 스냅샷과 유효기간 계산 규칙을 검증합니다.
package com.kafkick.core.coupon.domain;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IssuanceTest {

    @Test
    @DisplayName("쿠폰 발급 시 ISSUED 상태와 만료 시각을 계산한다")
    void issueCoupon() {
        Instant issuedAt = Instant.parse("2026-08-18T05:00:00Z");

        Issuance issuance = Issuance.issue(
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                7,
                issuedAt
        );

        assertThat(issuance.id()).isNull();
        assertThat(issuance.status()).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(issuance.issuedGrade()).isEqualTo(MembershipGrade.GOLD);
        assertThat(issuance.expiresAt())
                .isEqualTo(Instant.parse("2026-08-25T05:00:00Z"));
        assertThat(issuance.updatedAt()).isEqualTo(issuedAt);
    }

    @Test
    @DisplayName("쿠폰 코드는 정확히 16자리여야 한다")
    void rejectInvalidCodeLength() {
        assertThatThrownBy(() -> Issuance.issue(
                10L,
                20L,
                "SHORT",
                MembershipGrade.GOLD,
                7,
                Instant.parse("2026-08-18T05:00:00Z")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("쿠폰 코드는 16자리여야 합니다.");
    }

    @Test
    @DisplayName("발급 시각이 없으면 유효기간 계산 전에 거부한다")
    void rejectMissingIssuedAtBeforeExpirationCalculation() {
        assertThatThrownBy(() -> Issuance.issue(
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                0,
                null
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("쿠폰 발급 시각은 필수입니다.");
    }
}
