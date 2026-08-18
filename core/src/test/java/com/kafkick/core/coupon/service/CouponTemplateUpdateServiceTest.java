// 쿠폰 템플릿 수정의 조회·검증·저장 흐름을 검증합니다.
package com.kafkick.core.coupon.service;

import java.time.LocalTime;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponTemplateUpdateServiceTest {

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @InjectMocks
    private CouponTemplateUpdateService couponTemplateUpdateService;

    @Test
    @DisplayName("기존 쿠폰 템플릿을 검증된 값으로 수정한다")
    void updateCouponTemplate() {
        CouponTemplate existingCouponTemplate = existingCouponTemplate();
        CouponTemplateUpdateCommand command = updateCommand(null);

        when(couponTemplateRepository.findById(100L))
                .thenReturn(Optional.of(existingCouponTemplate));
        when(couponTemplateRepository.save(any(CouponTemplate.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        CouponTemplate result =
                couponTemplateUpdateService.update(100L, command);

        ArgumentCaptor<CouponTemplate> couponTemplateCaptor =
                ArgumentCaptor.forClass(CouponTemplate.class);
        verify(couponTemplateRepository).save(couponTemplateCaptor.capture());

        CouponTemplate updatedCouponTemplate = couponTemplateCaptor.getValue();
        assertThat(updatedCouponTemplate.id()).isEqualTo(100L);
        assertThat(updatedCouponTemplate.active()).isFalse();
        assertThat(updatedCouponTemplate.name()).isEqualTo("수정된 20% 쿠폰");
        assertThat(updatedCouponTemplate.discountRate()).isEqualTo(20);
        assertThat(result).isSameAs(updatedCouponTemplate);
    }

    @Test
    @DisplayName("존재하지 않는 쿠폰 템플릿은 COUPON-102를 반환한다")
    void rejectMissingCouponTemplate() {
        when(couponTemplateRepository.findById(999L))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> couponTemplateUpdateService.update(
                999L,
                updateCommand(null)
        ))
                .isInstanceOf(BusinessException.class)
                .hasMessage("couponTemplateId=999")
                .satisfies(exception -> assertThat(
                        ((BusinessException) exception).getErrorCode()
                ).isEqualTo(
                        CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND
                ));

        verify(couponTemplateRepository, never()).save(any());
    }

    @Test
    @DisplayName("잘못된 할인 정책이면 COUPON-101을 반환하고 저장하지 않는다")
    void rejectInvalidDiscountPolicy() {
        when(couponTemplateRepository.findById(100L))
                .thenReturn(Optional.of(existingCouponTemplate()));

        assertThatThrownBy(() -> couponTemplateUpdateService.update(
                100L,
                updateCommand(5_000)
        ))
                .isInstanceOf(BusinessException.class)
                .hasMessage(
                        "퍼센트 할인에는 정액 할인 금액을 입력할 수 없습니다."
                )
                .satisfies(exception -> assertThat(
                        ((BusinessException) exception).getErrorCode()
                ).isEqualTo(
                        CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE
                ));

        verify(couponTemplateRepository, never()).save(any());
    }

    private CouponTemplate existingCouponTemplate() {
        return CouponTemplate.restore(
                100L,
                1L,
                "수정 전 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                2,
                CouponDayOfWeek.WED,
                LocalTime.of(10, 0),
                2,
                100,
                Set.of(MembershipGrade.GOLD),
                false
        );
    }

    private CouponTemplateUpdateCommand updateCommand(
            Integer discountAmount
    ) {
        return new CouponTemplateUpdateCommand(
                1L,
                "수정된 20% 쿠폰",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                10_000,
                discountAmount,
                14,
                3,
                CouponDayOfWeek.FRI,
                LocalTime.of(12, 0),
                3,
                50,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP)
        );
    }
}
