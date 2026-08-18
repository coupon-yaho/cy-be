// 쿠폰 템플릿 활성화 상태 변경의 조회·멱등성·저장 흐름을 검증합니다.
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
class CouponTemplateActivationServiceTest {

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @InjectMocks
    private CouponTemplateActivationService couponTemplateActivationService;

    @Test
    @DisplayName("활성 쿠폰 템플릿을 비활성화한다")
    void deactivateCouponTemplate() {
        CouponTemplate couponTemplate = couponTemplate(true);
        when(couponTemplateRepository.findById(100L))
                .thenReturn(Optional.of(couponTemplate));
        when(couponTemplateRepository.save(any(CouponTemplate.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        CouponTemplate result = couponTemplateActivationService
                .changeActivation(
                        100L,
                        new CouponTemplateActivationCommand(false)
                );

        ArgumentCaptor<CouponTemplate> captor =
                ArgumentCaptor.forClass(CouponTemplate.class);
        verify(couponTemplateRepository).save(captor.capture());
        assertThat(captor.getValue().id()).isEqualTo(100L);
        assertThat(captor.getValue().active()).isFalse();
        assertThat(result).isSameAs(captor.getValue());
    }

    @Test
    @DisplayName("비활성 쿠폰 템플릿을 다시 활성화한다")
    void activateCouponTemplate() {
        CouponTemplate couponTemplate = couponTemplate(false);
        when(couponTemplateRepository.findById(100L))
                .thenReturn(Optional.of(couponTemplate));
        when(couponTemplateRepository.save(any(CouponTemplate.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        CouponTemplate result = couponTemplateActivationService
                .changeActivation(
                        100L,
                        new CouponTemplateActivationCommand(true)
                );

        assertThat(result.active()).isTrue();
        verify(couponTemplateRepository).save(result);
    }

    @Test
    @DisplayName("이미 같은 활성 상태이면 저장하지 않고 현재 결과를 반환한다")
    void keepSameActivationWithoutSaving() {
        CouponTemplate couponTemplate = couponTemplate(false);
        when(couponTemplateRepository.findById(100L))
                .thenReturn(Optional.of(couponTemplate));

        CouponTemplate result = couponTemplateActivationService
                .changeActivation(
                        100L,
                        new CouponTemplateActivationCommand(false)
                );

        assertThat(result).isSameAs(couponTemplate);
        verify(couponTemplateRepository, never()).save(any());
    }

    @Test
    @DisplayName("존재하지 않는 쿠폰 템플릿 상태 변경은 COUPON-102를 반환한다")
    void rejectMissingCouponTemplate() {
        when(couponTemplateRepository.findById(999L))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> couponTemplateActivationService
                .changeActivation(
                        999L,
                        new CouponTemplateActivationCommand(false)
                ))
                .isInstanceOf(BusinessException.class)
                .satisfies(exception -> assertThat(
                        ((BusinessException) exception).getErrorCode()
                ).isEqualTo(
                        CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND
                ));

        verify(couponTemplateRepository, never()).save(any());
    }

    private CouponTemplate couponTemplate(boolean active) {
        return CouponTemplate.restore(
                100L,
                1L,
                "활성 상태 테스트 쿠폰",
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
                active
        );
    }
}
