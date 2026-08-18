// 트랜잭션 어댑터가 plain Java 활성화 상태 변경 유즈케이스를 호출하는지 검증합니다.
package com.kafkick.api.coupon.adapter;

import java.time.LocalTime;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.service.CouponTemplateActivationCommand;
import com.kafkick.core.coupon.service.CouponTemplateActivationService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponTemplateActivationTransactionalAdapterTest {

    @Mock
    private CouponTemplateActivationService couponTemplateActivationService;

    @InjectMocks
    private CouponTemplateActivationTransactionalAdapter adapter;

    @Test
    @DisplayName("트랜잭션 경계 안에서 활성화 상태 변경 유즈케이스를 호출한다")
    void delegateActivationChangeWithinTransactionBoundary() throws Exception {
        CouponTemplateActivationCommand command =
                new CouponTemplateActivationCommand(false);
        CouponTemplate deactivatedCouponTemplate = couponTemplate(false);
        when(couponTemplateActivationService.changeActivation(100L, command))
                .thenReturn(deactivatedCouponTemplate);

        CouponTemplate result = adapter.changeActivation(100L, command);

        assertThat(result).isSameAs(deactivatedCouponTemplate);
        verify(couponTemplateActivationService)
                .changeActivation(100L, command);
        assertThat(CouponTemplateActivationTransactionalAdapter.class
                .getMethod(
                        "changeActivation",
                        Long.class,
                        CouponTemplateActivationCommand.class
                )
                .isAnnotationPresent(Transactional.class))
                .isTrue();
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
