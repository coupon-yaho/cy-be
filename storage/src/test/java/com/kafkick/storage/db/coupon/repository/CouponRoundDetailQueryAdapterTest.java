package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.membership.domain.MembershipGrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundDetailQueryAdapterTest {

    @Mock
    private CouponRoundJpaRepository couponRoundJpaRepository;

    @Mock
    private CouponRoundDetailProjection projection;

    @InjectMocks
    private CouponRoundDetailQueryAdapter queryAdapter;

    @Test
    @DisplayName("조회 결과를 쿠폰 회차 상세 정보로 변환한다")
    void mapCouponRoundDetail() {
        givenProjection();
        when(couponRoundJpaRepository.findCouponRoundDetailById(10L))
                .thenReturn(Optional.of(projection));

        CouponRoundDetail result = queryAdapter.findById(10L).orElseThrow();

        assertThat(result.couponRoundId()).isEqualTo(10L);
        assertThat(result.templateId()).isEqualTo(1L);
        assertThat(result.brandId()).isEqualTo(2L);
        assertThat(result.policyType())
                .isEqualTo(CouponPolicyType.PERCENT_CAPPED);
        assertThat(result.eligibleGrades())
                .containsExactly(MembershipGrade.GOLD, MembershipGrade.VIP);
        assertThat(result.status()).isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(result.totalQuantity()).isEqualTo(100);
        assertThat(result.remainingQuantity()).isEqualTo(80);
    }

    @Test
    @DisplayName("회차 상세 조회 DB 장애를 공통 영속성 예외로 변환한다")
    void convertDataAccessFailure() {
        when(couponRoundJpaRepository.findCouponRoundDetailById(10L))
                .thenThrow(new DataAccessResourceFailureException(
                        "DB unavailable"
                ));

        assertThatThrownBy(() -> queryAdapter.findById(10L))
                .isInstanceOf(CouponPersistenceException.class)
                .hasMessageContaining("쿠폰 회차 상세 조회");
    }

    private void givenProjection() {
        when(projection.getCouponRoundId()).thenReturn(10L);
        when(projection.getTemplateId()).thenReturn(1L);
        when(projection.getBrandId()).thenReturn(2L);
        when(projection.getName()).thenReturn("골드 VIP 20% 할인");
        when(projection.getPolicyType()).thenReturn("PERCENT_CAPPED");
        when(projection.getDiscountRate()).thenReturn(20);
        when(projection.getMaxDiscountAmount()).thenReturn(10_000);
        when(projection.getDiscountAmount()).thenReturn(null);
        when(projection.getValidDays()).thenReturn(7);
        when(projection.getEligibleGradesMask()).thenReturn(12);
        when(projection.getOpenAt())
                .thenReturn(Instant.parse("2026-08-24T01:00:00Z"));
        when(projection.getCloseAt())
                .thenReturn(Instant.parse("2026-08-24T03:00:00Z"));
        when(projection.getStatus()).thenReturn("SCHEDULED");
        when(projection.getTotalQuantity()).thenReturn(100);
        when(projection.getRemainingQuantity()).thenReturn(80);
    }
}
