package com.kafkick.storage.db.coupon.repository;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.Pageable;

import com.kafkick.core.coupon.exception.CouponPersistenceErrorCode;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

// 사용자 쿠폰 조회의 저장소 오류가 표준 비즈니스 오류로 변환되는지 검증합니다.

@ExtendWith(MockitoExtension.class)
class MemberCouponQueryRepositoryImplTest {

    @Mock
    private IssuanceJpaRepository issuanceJpaRepository;

    @InjectMocks
    private MemberCouponQueryAdapter memberCouponQueryRepository;

    @Test
    @DisplayName("회원 쿠폰 조회 DB 오류를 공통 쿠폰 영속성 오류로 변환한다")
    void convertDataAccessException() {
        when(issuanceJpaRepository.findMemberCoupons(
                eq(20L),
                eq(null),
                any(Pageable.class)
        )).thenThrow(new DataAccessResourceFailureException("DB unavailable"));

        assertThatThrownBy(() -> memberCouponQueryRepository
                .findPageByMemberId(20L, null, 0, 20))
                .isInstanceOf(BusinessException.class)
                .satisfies(exception -> assertThat(
                        ((BusinessException) exception).getErrorCode()
                ).isEqualTo(
                        CouponPersistenceErrorCode.COUPON_PERSISTENCE_FAILED
                ));
    }
}
