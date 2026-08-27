package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;
import com.kafkick.core.membership.domain.MembershipGrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PublicCouponRoundQueryAdapterTest {

    @Mock
    private CouponRoundJpaRepository couponRoundJpaRepository;

    @Mock
    private CouponRoundDetailProjection projection;

    @InjectMocks
    private PublicCouponRoundQueryAdapter queryAdapter;

    @Test
    @DisplayName("공개 회차 조회 결과의 정책 등급 상태와 재고를 변환한다")
    void mapPublicCouponRoundPage() {
        givenProjection();
        PageRequest pageRequest = PageRequest.of(0, 20);
        when(couponRoundJpaRepository.findPublicCouponRounds(
                "OPEN",
                MembershipGrade.GOLD.getBitValue(),
                pageRequest
        )).thenReturn(new PageImpl<>(List.of(projection), pageRequest, 1));

        PublicCouponRoundPage result = queryAdapter.findPage(
                CouponRoundStatus.OPEN,
                MembershipGrade.GOLD,
                0,
                20
        );

        assertThat(result.content()).hasSize(1);
        assertThat(result.content().get(0).policyType())
                .isEqualTo(CouponPolicyType.PERCENT_CAPPED);
        assertThat(result.content().get(0).eligibleGrades())
                .containsExactly(MembershipGrade.GOLD, MembershipGrade.VIP);
        assertThat(result.content().get(0).status())
                .isEqualTo(CouponRoundStatus.OPEN);
        assertThat(result.content().get(0).totalQuantity()).isEqualTo(100);
        assertThat(result.content().get(0).remainingQuantity()).isEqualTo(80);
    }

    @Test
    @DisplayName("공개 회차 목록 조회 DB 장애를 공통 영속성 예외로 변환한다")
    void convertDataAccessFailure() {
        PageRequest pageRequest = PageRequest.of(0, 20);
        when(couponRoundJpaRepository.findPublicCouponRounds(
                null,
                null,
                pageRequest
        )).thenThrow(new DataAccessResourceFailureException("DB unavailable"));

        assertThatThrownBy(() -> queryAdapter.findPage(
                null,
                null,
                0,
                20
        ))
                .isInstanceOf(CouponPersistenceException.class)
                .hasMessageContaining("공개 쿠폰 회차 목록 조회");
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
        when(projection.getStatus()).thenReturn("OPEN");
        when(projection.getTotalQuantity()).thenReturn(100);
        when(projection.getRemainingQuantity()).thenReturn(80);
    }
}
