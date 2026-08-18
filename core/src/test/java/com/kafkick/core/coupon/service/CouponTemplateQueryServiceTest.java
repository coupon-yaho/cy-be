// 쿠폰 템플릿 단건 및 목록 조회를 검증합니다.
package com.kafkick.core.coupon.service;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.port.CouponTemplatePage;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponTemplateQueryServiceTest {

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @InjectMocks
    private CouponTemplateQueryService couponTemplateQueryService;

    @Test
    @DisplayName("쿠폰 템플릿 ID로 단건 조회한다")
    void findCouponTemplateById() {
        CouponTemplate couponTemplate = createCouponTemplate(100L);

        when(couponTemplateRepository.findById(100L))
                .thenReturn(Optional.of(couponTemplate));

        CouponTemplate result = couponTemplateQueryService.findById(100L);

        assertThat(result).isSameAs(couponTemplate);
        verify(couponTemplateRepository).findById(100L);
    }

    @Test
    @DisplayName("쿠폰 템플릿이 존재하지 않으면 COUPON-102 오류를 반환한다")
    void rejectMissingCouponTemplate() {
        when(couponTemplateRepository.findById(999L))
                .thenReturn(Optional.empty());

        assertThatThrownBy(
                () -> couponTemplateQueryService.findById(999L)
        )
                .isInstanceOf(BusinessException.class)
                .hasMessage("couponTemplateId=999")
                .satisfies(exception -> {
                    BusinessException businessException =
                            (BusinessException) exception;

                    assertThat(businessException.getErrorCode())
                            .isEqualTo(
                                    CouponTemplateErrorCode
                                            .COUPON_TEMPLATE_NOT_FOUND
                            );
                });

        verify(couponTemplateRepository).findById(999L);
    }

    @Test
    @DisplayName("쿠폰 템플릿 페이지를 저장소 조회 결과로 반환한다")
    void findCouponTemplatePage() {
        CouponTemplate firstCouponTemplate = createCouponTemplate(1L);
        CouponTemplate secondCouponTemplate = createCouponTemplate(2L);
        CouponTemplatePage expectedPage = new CouponTemplatePage(
                List.of(firstCouponTemplate, secondCouponTemplate),
                0,
                20,
                2,
                1
        );

        when(couponTemplateRepository.findPageByIdAsc(0, 20))
                .thenReturn(expectedPage);

        CouponTemplatePage result =
                couponTemplateQueryService.findPage(0, 20);

        assertThat(result.content()).containsExactly(
                firstCouponTemplate,
                secondCouponTemplate
        );
        assertThat(result.page()).isZero();
        assertThat(result.size()).isEqualTo(20);
        assertThat(result.totalElements()).isEqualTo(2);
        assertThat(result.totalPages()).isEqualTo(1);
        verify(couponTemplateRepository).findPageByIdAsc(0, 20);
    }

    private CouponTemplate createCouponTemplate(Long id) {
        return CouponTemplate.restore(
                id,
                1L,
                "골드 VIP 20% 할인",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                10_000,
                null,
                7,
                2,
                CouponDayOfWeek.WED,
                LocalTime.of(10, 0),
                2,
                100,
                Set.of(
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                ),
                true
        );
    }
}
