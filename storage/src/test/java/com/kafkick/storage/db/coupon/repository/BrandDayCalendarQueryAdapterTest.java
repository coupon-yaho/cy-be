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

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.membership.domain.MembershipGrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BrandDayCalendarQueryAdapterTest {

    private static final Instant FROM =
            Instant.parse("2026-08-01T00:00:00Z");
    private static final Instant TO =
            Instant.parse("2026-09-01T00:00:00Z");

    @Mock
    private CouponRoundJpaRepository couponRoundJpaRepository;

    @Mock
    private CouponRoundDetailProjection projection;

    @InjectMocks
    private BrandDayCalendarQueryAdapter adapter;

    @Test
    @DisplayName("달력 회차 조회 결과를 코어 모델로 변환한다")
    void mapCalendarRounds() {
        givenProjection();
        when(couponRoundJpaRepository.findCalendarRounds(FROM, TO))
                .thenReturn(List.of(projection));

        CouponRoundDetail result = adapter.findBetween(FROM, TO).getFirst();

        assertThat(result.couponRoundId()).isEqualTo(10L);
        assertThat(result.eligibleGrades())
                .containsExactly(MembershipGrade.GOLD, MembershipGrade.VIP);
        assertThat(result.status()).isEqualTo(CouponRoundStatus.OPEN);
        assertThat(result.remainingQuantity()).isEqualTo(80);
    }

    @Test
    @DisplayName("달력 조회 DB 장애를 공통 영속성 예외로 변환한다")
    void convertDataAccessFailure() {
        when(couponRoundJpaRepository.findCalendarRounds(FROM, TO))
                .thenThrow(new DataAccessResourceFailureException(
                        "DB unavailable"
                ));

        assertThatThrownBy(() -> adapter.findBetween(FROM, TO))
                .isInstanceOf(CouponPersistenceException.class)
                .hasMessageContaining("브랜드 데이 달력 조회");
    }

    private void givenProjection() {
        when(projection.getCouponRoundId()).thenReturn(10L);
        when(projection.getTemplateId()).thenReturn(1L);
        when(projection.getBrandId()).thenReturn(2L);
        when(projection.getName()).thenReturn("골드 할인");
        when(projection.getPolicyType()).thenReturn("FIXED_AMOUNT");
        when(projection.getDiscountAmount()).thenReturn(5_000);
        when(projection.getValidDays()).thenReturn(7);
        when(projection.getEligibleGradesMask()).thenReturn(12);
        when(projection.getOpenAt()).thenReturn(FROM.plusSeconds(3_600));
        when(projection.getCloseAt()).thenReturn(FROM.plusSeconds(10_800));
        when(projection.getStatus()).thenReturn("OPEN");
        when(projection.getTotalQuantity()).thenReturn(100);
        when(projection.getRemainingQuantity()).thenReturn(80);
    }
}
