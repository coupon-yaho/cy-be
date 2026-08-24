package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponRoundErrorCode;
import com.kafkick.core.coupon.port.CouponRoundDetailQueryPort;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundDetailQueryServiceTest {

    @Mock
    private CouponRoundDetailQueryPort queryPort;

    @InjectMocks
    private CouponRoundDetailQueryService queryService;

    @Test
    @DisplayName("쿠폰 회차 ID로 상세 정보를 조회한다")
    void findCouponRoundDetail() {
        CouponRoundDetail expected = detail();
        when(queryPort.findById(10L)).thenReturn(Optional.of(expected));

        CouponRoundDetail result = queryService.findById(10L);

        assertThat(result).isSameAs(expected);
        verify(queryPort).findById(10L);
    }

    @Test
    @DisplayName("존재하지 않는 쿠폰 회차를 조회하면 찾을 수 없음 오류를 반환한다")
    void rejectMissingCouponRound() {
        when(queryPort.findById(999L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> queryService.findById(999L))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(
                                        CouponRoundErrorCode
                                                .COUPON_ROUND_NOT_FOUND
                                )
                );
    }

    private static CouponRoundDetail detail() {
        return new CouponRoundDetail(
                10L,
                1L,
                2L,
                "골드 VIP 20% 할인",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                10_000,
                null,
                7,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                Instant.parse("2026-08-24T01:00:00Z"),
                Instant.parse("2026-08-24T03:00:00Z"),
                CouponRoundStatus.OPEN,
                100,
                80
        );
    }
}
