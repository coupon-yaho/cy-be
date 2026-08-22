package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.PageRequest;

import com.kafkick.core.coupon.exception.CouponPersistenceException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IssuableCouponRoundQueryAdapterTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-22T09:00:00Z");

    @Mock
    private CouponRoundJpaRepository couponRoundJpaRepository;

    @InjectMocks
    private IssuableCouponRoundQueryAdapter queryAdapter;

    @Test
    @DisplayName("회차 목록 조회 DB 장애를 공통 영속성 예외로 변환한다")
    void convertDataAccessFailure() {
        when(couponRoundJpaRepository.findIssuableCouponRounds(
                20L,
                4,
                AS_OF,
                PageRequest.of(0, 20)
        )).thenThrow(new DataAccessResourceFailureException("DB unavailable"));

        assertThatThrownBy(() -> queryAdapter.findPage(
                20L,
                4,
                AS_OF,
                0,
                20
        )).isInstanceOf(CouponPersistenceException.class)
                .hasMessageContaining("발급 가능한 쿠폰 회차 목록 조회");
    }
}
