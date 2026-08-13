// 쿠폰 템플릿 저장 중 DB 제약 위반이 비즈니스 오류로 변환되는지 검증합니다.
package com.kafkick.storage.coupon.adapter;

import java.time.LocalTime;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import com.kafkick.core.coupon.CouponDayOfWeek;
import com.kafkick.core.coupon.CouponPolicyType;
import com.kafkick.core.coupon.CouponTemplate;
import com.kafkick.core.coupon.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.coupon.entity.CouponTemplateEntity;
import com.kafkick.storage.coupon.repository.CouponTemplateJpaRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponTemplateRepositoryAdapterTest {

    @Mock
    private CouponTemplateJpaRepository jpaRepository;

    @InjectMocks
    private CouponTemplateRepositoryAdapter repositoryAdapter;

    @Test
    @DisplayName("DB 제약을 위반하면 COUPON-001 오류로 변환한다")
    void convertDataIntegrityViolation() {
        CouponTemplate couponTemplate = CouponTemplate.create(
                999L,
                "존재하지 않는 브랜드 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
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

        when(jpaRepository.save(any(CouponTemplateEntity.class)))
                .thenThrow(new DataIntegrityViolationException(
                        "foreign key violation"
                ));

        assertThatThrownBy(() -> repositoryAdapter.save(couponTemplate))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("brandId=999")
                .satisfies(exception -> {
                    BusinessException businessException =
                            (BusinessException) exception;

                    assertThat(businessException.getErrorCode())
                            .isEqualTo(
                                    CouponErrorCode.INVALID_COUPON_TEMPLATE
                            );
                });

        verify(jpaRepository)
                .save(any(CouponTemplateEntity.class));
    }
}