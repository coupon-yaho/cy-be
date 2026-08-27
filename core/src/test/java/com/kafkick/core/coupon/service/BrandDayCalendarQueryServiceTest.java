package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.port.BrandDayCalendarQueryPort;
import com.kafkick.core.coupon.query.BrandDayCalendarEntry;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BrandDayCalendarQueryServiceTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final Instant AS_OF =
            Instant.parse("2026-08-10T00:00:00Z");

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @Mock
    private BrandDayCalendarQueryPort calendarQueryPort;

    @Test
    @DisplayName("반복 일정에 실제 회차의 상태와 재고를 결합한다")
    void combineRecurringScheduleWithActualRound() {
        CouponTemplate template = template();
        Instant openAt = Instant.parse("2026-08-10T01:00:00Z");
        Instant closeAt = Instant.parse("2026-08-10T03:00:00Z");
        CouponRoundDetail actual = new CouponRoundDetail(
                100L,
                1L,
                2L,
                "골드 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                Set.of(MembershipGrade.GOLD),
                openAt,
                closeAt,
                CouponRoundStatus.OPEN,
                100,
                80
        );
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of(template));
        when(calendarQueryPort.findBetween(
                Instant.parse("2026-08-02T15:00:00Z"),
                Instant.parse("2026-08-30T15:00:00Z")
        )).thenReturn(List.of(actual));

        List<BrandDayCalendarEntry> result = service().findBetween(
                LocalDate.of(2026, 8, 3),
                LocalDate.of(2026, 8, 30),
                AS_OF
        );

        assertThat(result).singleElement().satisfies(entry -> {
            assertThat(entry.couponRoundId()).isEqualTo(100L);
            assertThat(entry.status()).isEqualTo(CouponRoundStatus.OPEN);
            assertThat(entry.totalQuantity()).isEqualTo(100);
            assertThat(entry.activeCount()).isEqualTo(20);
        });
    }

    @Test
    @DisplayName("실제 회차가 없는 미래 반복 일정은 예정 상태로 반환한다")
    void returnVirtualScheduledOccurrence() {
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of(template()));
        when(calendarQueryPort.findBetween(
                Instant.parse("2026-08-02T15:00:00Z"),
                Instant.parse("2026-08-30T15:00:00Z")
        )).thenReturn(List.of());

        BrandDayCalendarEntry result = service().findBetween(
                LocalDate.of(2026, 8, 3),
                LocalDate.of(2026, 8, 30),
                Instant.parse("2026-08-01T00:00:00Z")
        ).getFirst();

        assertThat(result.status()).isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(result.couponRoundId()).isNull();
        assertThat(result.totalQuantity()).isNull();
        assertThat(result.activeCount()).isNull();
    }

    @Test
    @DisplayName("프론트가 요청한 두 달 범위의 달력을 조회한다")
    void queryTwoMonthRange() {
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of());
        when(calendarQueryPort.findBetween(
                Instant.parse("2026-07-31T15:00:00Z"),
                Instant.parse("2026-09-30T15:00:00Z")
        )).thenReturn(List.of());

        assertThat(service().findBetween(
                LocalDate.of(2026, 8, 1),
                LocalDate.of(2026, 9, 30),
                AS_OF
        )).isEmpty();
    }

    @Test
    @DisplayName("종료일이 LocalDate 최댓값인 달력 조회를 입력 오류로 거부한다")
    void rejectMaximumLocalDate() {
        assertThatThrownBy(() -> service().findBetween(
                LocalDate.MAX,
                LocalDate.MAX,
                AS_OF
        )).isInstanceOfSatisfying(
                BusinessException.class,
                exception -> assertThat(exception.getErrorCode())
                        .isEqualTo(CommonErrorCode.INVALID_INPUT)
        );
    }

    @Test
    @DisplayName("달력 조회 기간은 양끝 포함 366일까지 허용한다")
    void allowMaximumCalendarRange() {
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of());
        when(calendarQueryPort.findBetween(
                Instant.parse("2023-12-31T15:00:00Z"),
                Instant.parse("2024-12-31T15:00:00Z")
        )).thenReturn(List.of());

        assertThat(service().findBetween(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 12, 31),
                AS_OF
        )).isEmpty();
    }

    @Test
    @DisplayName("달력 조회 기간이 양끝 포함 367일이면 입력 오류로 거부한다")
    void rejectCalendarRangeOverMaximum() {
        assertThatThrownBy(() -> service().findBetween(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2025, 1, 1),
                AS_OF
        )).isInstanceOfSatisfying(
                BusinessException.class,
                exception -> assertThat(exception.getErrorCode())
                        .isEqualTo(CommonErrorCode.INVALID_INPUT)
        );
    }

    private BrandDayCalendarQueryService service() {
        return new BrandDayCalendarQueryService(
                couponTemplateRepository,
                calendarQueryPort,
                SEOUL
        );
    }

    private static CouponTemplate template() {
        return CouponTemplate.restore(
                1L,
                2L,
                "골드 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                2,
                CouponDayOfWeek.MON,
                LocalTime.of(10, 0),
                2,
                100,
                Set.of(MembershipGrade.GOLD),
                true
        );
    }
}
