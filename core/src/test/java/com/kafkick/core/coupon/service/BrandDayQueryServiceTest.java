package com.kafkick.core.coupon.service;

import java.time.LocalTime;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.query.BrandDaySchedule;
import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.membership.domain.MembershipGrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BrandDayQueryServiceTest {

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @Test
    @DisplayName("활성 브랜드 데이 규칙을 주차와 요일 순으로 반환한다")
    void findBrandDaySchedulesInCalendarOrder() {
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of(
                        template(2L, 2, CouponDayOfWeek.FRI),
                        template(1L, 1, CouponDayOfWeek.MON)
                ));

        List<BrandDaySchedule> result =
                new BrandDayQueryService(couponTemplateRepository).findAll();

        assertThat(result).extracting(BrandDaySchedule::templateId)
                .containsExactly(1L, 2L);
        assertThat(result.getFirst().eligibleGradesMask()).isEqualTo(12);
    }

    private static CouponTemplate template(
            Long id,
            int nthWeek,
            CouponDayOfWeek dayOfWeek
    ) {
        return CouponTemplate.restore(
                id,
                id,
                "브랜드 데이 " + id,
                CouponPolicyType.PERCENT_CAPPED,
                20,
                10_000,
                null,
                7,
                nthWeek,
                dayOfWeek,
                LocalTime.of(10, 0),
                2,
                100,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                true
        );
    }
}
