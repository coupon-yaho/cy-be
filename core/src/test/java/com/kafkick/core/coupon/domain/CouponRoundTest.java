// 쿠폰 템플릿 스냅샷과 최초 재고 불변식을 검증합니다.
package com.kafkick.core.coupon.domain;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CouponRoundTest {

    @Test
    @DisplayName("활성 템플릿의 정책을 SCHEDULED 회차로 스냅샷한다")
    void scheduleCouponRoundFromTemplate() {
        CouponTemplate template = fixedAmountTemplate(true);
        Instant openAt = Instant.parse("2026-09-08T05:00:00Z");
        Instant generatedAt = Instant.parse("2026-08-18T00:00:00Z");

        CouponRound couponRound = CouponRound.schedule(
                template,
                openAt,
                generatedAt
        );

        assertThat(couponRound.id()).isNull();
        assertThat(couponRound.templateId()).isEqualTo(100L);
        assertThat(couponRound.brandId()).isEqualTo(1L);
        assertThat(couponRound.name()).isEqualTo("골드 VIP 5천원 할인");
        assertThat(couponRound.policyType())
                .isEqualTo(CouponPolicyType.FIXED_AMOUNT);
        assertThat(couponRound.discountAmount()).isEqualTo(5_000);
        assertThat(couponRound.validDays()).isEqualTo(7);
        assertThat(couponRound.eligibleGrades())
                .containsExactly(MembershipGrade.GOLD, MembershipGrade.VIP);
        assertThat(couponRound.openAt()).isEqualTo(openAt);
        assertThat(couponRound.closeAt())
                .isEqualTo(Instant.parse("2026-09-08T07:00:00Z"));
        assertThat(couponRound.status())
                .isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(couponRound.createdAt()).isEqualTo(generatedAt);
    }

    @Test
    @DisplayName("비활성 템플릿으로 회차를 생성할 수 없다")
    void rejectInactiveTemplate() {
        assertThatThrownBy(() -> CouponRound.schedule(
                fixedAmountTemplate(false),
                Instant.parse("2026-09-08T05:00:00Z"),
                Instant.parse("2026-08-18T00:00:00Z")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("비활성 쿠폰 템플릿으로 회차를 만들 수 없습니다.");
    }

    @Test
    @DisplayName("회차 시작 시각이 없으면 도메인 검증 예외를 반환한다")
    void rejectMissingOpenAt() {
        assertThatThrownBy(() -> CouponRound.schedule(
                fixedAmountTemplate(true),
                null,
                Instant.parse("2026-08-18T00:00:00Z")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("쿠폰 회차 시작 시각은 필수입니다.");
    }

    @Test
    @DisplayName("최초 재고는 active_count 0으로 초기화한다")
    void initializeCouponStock() {
        Instant updatedAt = Instant.parse("2026-08-18T00:00:00Z");

        CouponStock couponStock = CouponStock.initialize(10_000, updatedAt);

        assertThat(couponStock.couponRoundId()).isNull();
        assertThat(couponStock.totalQuantity()).isEqualTo(10_000);
        assertThat(couponStock.activeCount()).isZero();
        assertThat(couponStock.remainingQuantity()).isEqualTo(10_000);
        assertThat(couponStock.updatedAt()).isEqualTo(updatedAt);
    }

    private CouponTemplate fixedAmountTemplate(boolean active) {
        return CouponTemplate.restore(
                100L,
                1L,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                2,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                active
        );
    }
}
