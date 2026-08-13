// 쿠폰 생성 Service가 도메인을 저장하고 생성 결과를 반환하는지 테스트합니다.
package com.kafkick.api.coupon.service;

import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateResponse;
import com.kafkick.core.coupon.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalTime;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CouponTemplateCreateServiceTest {

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @InjectMocks
    private CouponTemplateCreateService couponTemplateCreateService;

    @Test
    @DisplayName("쿠폰 생성 요청을 저장하고 생성 결과를 반환한다")
    void createCoupon() {
        CouponTemplateCreateRequest request = new CouponTemplateCreateRequest(
                1L,
                "모카빈 20% 할인",
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

        CouponTemplate savedCouponTemplate = CouponTemplate.restore(
                100L,
                1L,
                "모카빈 20% 할인",
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
                ),
                true
        );

        when(couponTemplateRepository.save(any(CouponTemplate.class)))
                .thenReturn(savedCouponTemplate);

        CouponTemplateCreateResponse response =
                couponTemplateCreateService.create(request);

        ArgumentCaptor<CouponTemplate> couponCaptor =
                ArgumentCaptor.forClass(CouponTemplate.class);

        verify(couponTemplateRepository).save(couponCaptor.capture());

        CouponTemplate requestedCouponTemplate = couponCaptor.getValue();

        assertThat(requestedCouponTemplate.id()).isNull();
        assertThat(requestedCouponTemplate.brandId()).isEqualTo(1L);
        assertThat(requestedCouponTemplate.policyType())
                .isEqualTo(CouponPolicyType.PERCENT_CAPPED);
        assertThat(requestedCouponTemplate.eligibleGradesMask())
                .isEqualTo(15);

        assertThat(response.id()).isEqualTo(100L);
        assertThat(response.name()).isEqualTo("모카빈 20% 할인");
        assertThat(response.discountRate()).isEqualTo(20);
        assertThat(response.maxDiscountAmount()).isEqualTo(20_000);
        assertThat(response.discountAmount()).isNull();
        assertThat(response.active()).isTrue();
    }

    @Test
    @DisplayName("할인 정책이 잘못되면 Repository를 호출하지 않는다")
    void doNotSaveInvalidCoupon() {
        CouponTemplateCreateRequest request = new CouponTemplateCreateRequest(
                1L,
                "잘못된 쿠폰",
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
        );

        assertThatThrownBy(() -> couponTemplateCreateService.create(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "퍼센트 할인에는 정액 할인 금액을 입력할 수 없습니다."
                );

        verifyNoInteractions(couponTemplateRepository);
    }
}