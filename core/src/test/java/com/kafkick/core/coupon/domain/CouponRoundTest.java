package com.kafkick.core.coupon.domain;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Set;

import com.kafkick.core.membership.domain.MembershipGrade;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// 쿠폰 템플릿 스냅샷, 예약 구간과 최초 재고 불변식을 검증합니다.
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
        assertThat(couponRound.generatedAt()).isEqualTo(generatedAt);
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
    @DisplayName("단발성 예약은 명시한 종료 시각으로 SCHEDULED 회차를 만든다")
    void scheduleOneTimeCouponRoundWithExplicitCloseAt() {
        Instant openAt = Instant.parse("2026-08-25T00:00:00Z");
        Instant closeAt = Instant.parse("2026-08-25T03:00:00Z");

        CouponRound couponRound = CouponRound.schedule(
                fixedAmountTemplate(true),
                openAt,
                closeAt,
                Instant.parse("2026-08-20T00:00:00Z")
        );

        assertThat(couponRound.openAt()).isEqualTo(openAt);
        assertThat(couponRound.closeAt()).isEqualTo(closeAt);
        assertThat(couponRound.status())
                .isEqualTo(CouponRoundStatus.SCHEDULED);
    }

    @Test
    @DisplayName("신규 예약 시작이 기존 예약 안에 들어오면 겹친다")
    void overlapWhenNewRoundStartsInsideExistingRound() {
        CouponRound couponRound = scheduledRound();

        boolean overlaps = couponRound.overlaps(
                Instant.parse("2026-08-25T02:00:00Z"),
                Instant.parse("2026-08-25T04:00:00Z")
        );

        assertThat(overlaps).isTrue();
    }

    @Test
    @DisplayName("신규 예약이 기존 예약 전체를 포함하면 겹친다")
    void overlapWhenNewRoundContainsExistingRound() {
        CouponRound couponRound = scheduledRound();

        boolean overlaps = couponRound.overlaps(
                Instant.parse("2026-08-24T23:00:00Z"),
                Instant.parse("2026-08-25T04:00:00Z")
        );

        assertThat(overlaps).isTrue();
    }

    @Test
    @DisplayName("신규 예약 시작이 기존 종료 시각과 같으면 겹치지 않는다")
    void allowRoundStartingAtExistingCloseAt() {
        CouponRound couponRound = scheduledRound();

        boolean overlaps = couponRound.overlaps(
                Instant.parse("2026-08-25T03:00:00Z"),
                Instant.parse("2026-08-25T04:00:00Z")
        );

        assertThat(overlaps).isFalse();
    }

    @Test
    @DisplayName("단발성 예약 종료 시각이 시작 시각과 같으면 거부한다")
    void rejectOneTimeRoundWithoutPositiveDuration() {
        Instant openAt = Instant.parse("2026-08-25T00:00:00Z");

        assertThatThrownBy(() -> CouponRound.schedule(
                fixedAmountTemplate(true),
                openAt,
                openAt,
                Instant.parse("2026-08-20T00:00:00Z")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("쿠폰 회차 종료 시각은 시작 시각보다 늦어야 합니다.");
    }

    @Test
    @DisplayName("단발성 예약 시간이 24시간을 초과하면 거부한다")
    void rejectOneTimeRoundLongerThanTwentyFourHours() {
        assertThatThrownBy(() -> CouponRound.schedule(
                fixedAmountTemplate(true),
                Instant.parse("2026-08-25T00:00:00Z"),
                Instant.parse("2026-08-26T00:00:01Z"),
                Instant.parse("2026-08-20T00:00:00Z")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("쿠폰 회차 진행 시간은 24시간 이하여야 합니다.");
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

    private CouponRound scheduledRound() {
        return CouponRound.schedule(
                fixedAmountTemplate(true),
                Instant.parse("2026-08-25T00:00:00Z"),
                Instant.parse("2026-08-25T03:00:00Z"),
                Instant.parse("2026-08-20T00:00:00Z")
        );
    }
}
