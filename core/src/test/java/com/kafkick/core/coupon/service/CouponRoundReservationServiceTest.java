package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupontemplate.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.exception.CouponRoundErrorCode;
import com.kafkick.core.coupon.service.command.CouponRoundReservationCommand;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundReservationServiceTest {

    private static final Instant OPEN_AT =
            Instant.parse("2026-08-25T00:00:00Z");
    private static final Instant CLOSE_AT =
            Instant.parse("2026-08-25T03:00:00Z");
    private static final Instant GENERATED_AT =
            Instant.parse("2026-08-20T00:00:00Z");

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @Mock
    private CouponRoundCreationService couponRoundCreationService;

    private CouponRoundReservationService service;

    @BeforeEach
    void setUp() {
        service = new CouponRoundReservationService(
                couponTemplateRepository,
                couponRoundCreationService
        );
    }

    @Test
    @DisplayName("이미 지난 시작 시각으로 단발성 회차를 예약하면 거부한다")
    void rejectPastOneTimeSchedule() {
        CouponRoundReservationCommand command =
                new CouponRoundReservationCommand(
                        10L,
                        GENERATED_AT.minusSeconds(1),
                        GENERATED_AT.plusSeconds(3_599),
                        GENERATED_AT
                );

        assertThatThrownBy(() -> service.reserve(command))
                .isInstanceOf(BusinessException.class)
                .extracting("errorCode")
                .isEqualTo(
                        CouponRoundErrorCode.INVALID_COUPON_ROUND_SCHEDULE
                );
        verify(couponTemplateRepository, never()).findById(10L);
    }

    @Test
    @DisplayName("저장된 템플릿 정책과 재고를 복사해 단발성 회차를 예약한다")
    void reserveOneTimeRoundFromTemplateSnapshot() {
        CouponTemplate template = template(true);
        CouponRound savedRound = savedRound(template);
        when(couponTemplateRepository.findById(10L))
                .thenReturn(Optional.of(template));
        ArgumentCaptor<CouponRound> roundCaptor =
                ArgumentCaptor.forClass(CouponRound.class);
        ArgumentCaptor<CouponStock> stockCaptor =
                ArgumentCaptor.forClass(CouponStock.class);
        when(couponRoundCreationService.create(
                roundCaptor.capture(),
                stockCaptor.capture()
        )).thenReturn(savedRound);

        CouponRound result = service.reserve(new CouponRoundReservationCommand(
                10L,
                OPEN_AT,
                CLOSE_AT,
                GENERATED_AT
        ));

        assertThat(result).isEqualTo(savedRound);
        CouponRound requestedRound = roundCaptor.getValue();
        assertThat(requestedRound.templateId()).isEqualTo(10L);
        assertThat(requestedRound.brandId()).isEqualTo(2L);
        assertThat(requestedRound.discountAmount()).isEqualTo(5_000);
        assertThat(requestedRound.eligibleGrades())
                .containsExactly(MembershipGrade.GOLD, MembershipGrade.VIP);
        assertThat(requestedRound.openAt()).isEqualTo(OPEN_AT);
        assertThat(requestedRound.closeAt()).isEqualTo(CLOSE_AT);
        assertThat(requestedRound.status())
                .isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(stockCaptor.getValue().totalQuantity()).isEqualTo(100);
        assertThat(stockCaptor.getValue().activeCount()).isZero();
        assertThat(stockCaptor.getValue().updatedAt()).isEqualTo(GENERATED_AT);
    }

    @Test
    @DisplayName("존재하지 않는 템플릿으로 단발성 회차를 예약할 수 없다")
    void rejectMissingTemplate() {
        when(couponTemplateRepository.findById(10L))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.reserve(
                new CouponRoundReservationCommand(
                        10L,
                        OPEN_AT,
                        CLOSE_AT,
                        GENERATED_AT
                )
        ))
                .isInstanceOf(BusinessException.class)
                .extracting("errorCode")
                .isEqualTo(CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND);
    }

    @Test
    @DisplayName("비활성 템플릿으로 단발성 회차를 예약할 수 없다")
    void rejectInactiveTemplate() {
        when(couponTemplateRepository.findById(10L))
                .thenReturn(Optional.of(template(false)));

        assertThatThrownBy(() -> service.reserve(
                new CouponRoundReservationCommand(
                        10L,
                        OPEN_AT,
                        CLOSE_AT,
                        GENERATED_AT
                )
        ))
                .isInstanceOf(BusinessException.class)
                .extracting("errorCode")
                .isEqualTo(CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE);

        verify(couponRoundCreationService, never()).create(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()
        );
    }

    @Test
    @DisplayName("단발성 예약 종료 시각이 시작 시각보다 늦지 않으면 400 오류로 거부한다")
    void rejectInvalidOneTimeScheduleAsBusinessError() {
        when(couponTemplateRepository.findById(10L))
                .thenReturn(Optional.of(template(true)));

        assertThatThrownBy(() -> service.reserve(
                new CouponRoundReservationCommand(
                        10L,
                        OPEN_AT,
                        OPEN_AT,
                        GENERATED_AT
                )
        ))
                .isInstanceOf(BusinessException.class)
                .extracting("errorCode")
                .isEqualTo(
                        CouponRoundErrorCode.INVALID_COUPON_ROUND_SCHEDULE
                );
    }

    private CouponTemplate template(boolean active) {
        return CouponTemplate.restore(
                10L,
                2L,
                "브랜드 2 단발성 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                2,
                CouponDayOfWeek.TUE,
                LocalTime.of(9, 0),
                3,
                100,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                active
        );
    }

    private CouponRound savedRound(CouponTemplate template) {
        return CouponRound.restore(
                20L,
                template.id(),
                template.brandId(),
                template.name(),
                template.policyType(),
                template.discountRate(),
                template.maxDiscountAmount(),
                template.discountAmount(),
                template.validDays(),
                template.eligibleGrades(),
                OPEN_AT,
                CLOSE_AT,
                CouponRoundStatus.SCHEDULED,
                GENERATED_AT
        );
    }
}
