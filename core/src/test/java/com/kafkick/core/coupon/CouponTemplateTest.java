// 쿠폰 템플릿 생성과 할인 정책별 도메인 검증을 테스트합니다.
package com.kafkick.core.coupon;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CouponTemplateTest {

    @Test
    @DisplayName("퍼센트 상한 할인 쿠폰을 생성한다")
    void createPercentCappedCoupon() {
        CouponTemplate couponTemplate = CouponTemplate.create(
                1L,
                "  모카빈 20% 할인  ",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                20_000,
                null,
                10_000,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(
                        MembershipGrade.WELCOME,
                        MembershipGrade.SILVER,
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                )
        );

        assertThat(couponTemplate.id()).isNull();
        assertThat(couponTemplate.name()).isEqualTo("모카빈 20% 할인");
        assertThat(couponTemplate.policyType())
                .isEqualTo(CouponPolicyType.PERCENT_CAPPED);
        assertThat(couponTemplate.discountRate()).isEqualTo(20);
        assertThat(couponTemplate.maxDiscountAmount()).isEqualTo(20_000);
        assertThat(couponTemplate.discountAmount()).isNull();
        assertThat(couponTemplate.eligibleGradesMask()).isEqualTo(15);
        assertThat(couponTemplate.active()).isTrue();
    }

    @Test
    @DisplayName("정액 할인 쿠폰을 생성한다")
    void createFixedAmountCoupon() {
        CouponTemplate couponTemplate = CouponTemplate.create(
                1L,
                "모카빈 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                10_000,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP)
        );

        assertThat(couponTemplate.policyType())
                .isEqualTo(CouponPolicyType.FIXED_AMOUNT);
        assertThat(couponTemplate.discountRate()).isNull();
        assertThat(couponTemplate.maxDiscountAmount()).isNull();
        assertThat(couponTemplate.discountAmount()).isEqualTo(5_000);
        assertThat(couponTemplate.eligibleGradesMask()).isEqualTo(12);
    }

    @Test
    @DisplayName("퍼센트 할인에 정액 할인 금액을 입력할 수 없다")
    void rejectFixedAmountInPercentPolicy() {
        assertThatThrownBy(() -> CouponTemplate.create(
                1L,
                "잘못된 퍼센트 할인",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                20_000,
                5_000,
                10_000,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(MembershipGrade.VIP)
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("퍼센트 할인에는 정액 할인 금액을 입력할 수 없습니다.");
    }

    @Test
    @DisplayName("정액 할인에 할인율을 입력할 수 없다")
    void rejectPercentRateInFixedPolicy() {
        assertThatThrownBy(() -> CouponTemplate.create(
                1L,
                "잘못된 정액 할인",
                CouponPolicyType.FIXED_AMOUNT,
                20,
                null,
                5_000,
                10_000,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(MembershipGrade.VIP)
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "정액 할인에는 할인율이나 최대 할인 금액을 입력할 수 없습니다."
                );
    }

    @Test
    @DisplayName("퍼센트 할인율은 1에서 100 사이여야 한다")
    void rejectInvalidDiscountRate() {
        assertThatThrownBy(() -> CouponTemplate.create(
                1L,
                "잘못된 할인율",
                CouponPolicyType.PERCENT_CAPPED,
                101,
                20_000,
                null,
                10_000,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(MembershipGrade.VIP)
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("퍼센트 할인율은 1에서 100 사이여야 합니다.");
    }

    @Test
    @DisplayName("참여 가능한 멤버십 등급이 하나 이상 필요하다")
    void rejectEmptyEligibleGrades() {
        assertThatThrownBy(() -> CouponTemplate.create(
                1L,
                "등급 없는 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                10_000,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of()
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("참여 가능한 멤버십 등급이 필요합니다.");
    }

    @Test
    @DisplayName("멤버십 등급을 비트마스크로 변환하고 다시 복원한다")
    void convertMembershipGradeMask() {
        Set<MembershipGrade> grades = Set.of(
                MembershipGrade.GOLD,
                MembershipGrade.VIP
        );

        int mask = MembershipGrade.toMask(grades);
        Set<MembershipGrade> restored =
                MembershipGrade.fromMask(mask);

        assertThat(mask).isEqualTo(12);
        assertThat(restored)
                .containsExactlyInAnyOrder(
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                );
    }
}