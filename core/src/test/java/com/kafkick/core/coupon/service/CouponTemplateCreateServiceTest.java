// 쿠폰 템플릿 생성 유즈케이스의 저장과 도메인 오류 변환을 검증합니다.
package com.kafkick.core.coupon.service;

import java.time.LocalTime;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponTemplateCreateServiceTest {

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @InjectMocks
    private CouponTemplateCreateService couponTemplateCreateService;

    @Test
    @DisplayName("쿠폰 템플릿 생성 명령을 저장하고 도메인 모델을 반환한다")
    void createCouponTemplate() {
        CouponTemplateCreateCommand command = createCommand(null);
        CouponTemplate savedCouponTemplate = CouponTemplate.restore(
                100L,
                command.brandId(),
                command.name(),
                command.policyType(),
                command.discountRate(),
                command.maxDiscountAmount(),
                command.discountAmount(),
                command.validDays(),
                command.nthWeek(),
                command.dayOfWeek(),
                command.startTime(),
                command.durationHours(),
                command.stockPerOccurrence(),
                command.eligibleGrades(),
                true
        );

        when(couponTemplateRepository.save(any(CouponTemplate.class)))
                .thenReturn(savedCouponTemplate);

        CouponTemplate result = couponTemplateCreateService.create(command);

        ArgumentCaptor<CouponTemplate> couponTemplateCaptor =
                ArgumentCaptor.forClass(CouponTemplate.class);
        verify(couponTemplateRepository).save(couponTemplateCaptor.capture());

        CouponTemplate requestedCouponTemplate = couponTemplateCaptor.getValue();
        assertThat(requestedCouponTemplate.id()).isNull();
        assertThat(requestedCouponTemplate.brandId()).isEqualTo(1L);
        assertThat(requestedCouponTemplate.eligibleGradesMask()).isEqualTo(15);
        assertThat(result).isSameAs(savedCouponTemplate);
    }

    @Test
    @DisplayName("할인 정책이 잘못되면 저장소를 호출하지 않는다")
    void rejectInvalidDiscountPolicy() {
        CouponTemplateCreateCommand command = createCommand(5_000);

        assertThatThrownBy(() -> couponTemplateCreateService.create(command))
                .isInstanceOf(BusinessException.class)
                .hasMessage(
                        "퍼센트 할인에는 정액 할인 금액을 입력할 수 없습니다."
                )
                .satisfies(exception -> {
                    BusinessException businessException =
                            (BusinessException) exception;

                    assertThat(businessException.getErrorCode())
                            .isEqualTo(
                                    CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE
                            );
                });

        verifyNoInteractions(couponTemplateRepository);
    }

    private CouponTemplateCreateCommand createCommand(
            Integer discountAmount
    ) {
        return new CouponTemplateCreateCommand(
                1L,
                "모카빈 20% 할인",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                20_000,
                discountAmount,
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
    }
}
