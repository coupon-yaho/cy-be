// 트랜잭션 어댑터가 plain Java 수정 유즈케이스를 호출하는지 검증합니다.
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
import com.kafkick.core.coupon.service.CouponTemplateUpdateCommand;
import com.kafkick.core.coupon.service.CouponTemplateUpdateService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponTemplateUpdateTransactionalAdapterTest {

    @Mock
    private CouponTemplateUpdateService couponTemplateUpdateService;

    @InjectMocks
    private CouponTemplateUpdateTransactionalAdapter adapter;

    @Test
    @DisplayName("트랜잭션 경계 안에서 쿠폰 템플릿 수정 유즈케이스를 호출한다")
    void delegateUpdateWithinTransactionBoundary() throws Exception {
        CouponTemplateUpdateCommand command = updateCommand();
        CouponTemplate updatedCouponTemplate = updatedCouponTemplate();
        when(couponTemplateUpdateService.update(100L, command))
                .thenReturn(updatedCouponTemplate);

        CouponTemplate result = adapter.update(100L, command);

        assertThat(result).isSameAs(updatedCouponTemplate);
        verify(couponTemplateUpdateService).update(100L, command);
        assertThat(CouponTemplateUpdateTransactionalAdapter.class
                .getMethod(
                        "update",
                        Long.class,
                        CouponTemplateUpdateCommand.class
                )
                .isAnnotationPresent(Transactional.class))
                .isTrue();
    }

    private CouponTemplateUpdateCommand updateCommand() {
        return new CouponTemplateUpdateCommand(
                2L,
                "수정된 정액 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                14,
                3,
                CouponDayOfWeek.FRI,
                LocalTime.of(12, 0),
                3,
                50,
                Set.of(MembershipGrade.WELCOME, MembershipGrade.SILVER)
        );
    }

    private CouponTemplate updatedCouponTemplate() {
        return CouponTemplate.restore(
                100L,
                2L,
                "수정된 정액 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                14,
                3,
                CouponDayOfWeek.FRI,
                LocalTime.of(12, 0),
                3,
                50,
                Set.of(MembershipGrade.WELCOME, MembershipGrade.SILVER),
                true
        );
    }
}
