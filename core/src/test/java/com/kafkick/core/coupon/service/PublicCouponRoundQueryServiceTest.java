package com.kafkick.core.coupon.service;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.port.PublicCouponRoundQueryPort;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PublicCouponRoundQueryServiceTest {

    @Mock
    private PublicCouponRoundQueryPort queryPort;

    @InjectMocks
    private PublicCouponRoundQueryService queryService;

    @Test
    @DisplayName("상태와 페이지 조건으로 공개 쿠폰 회차를 조회한다")
    void findPublicCouponRoundPage() {
        PublicCouponRoundPage expected = new PublicCouponRoundPage(
                List.of(),
                1,
                10,
                0,
                0
        );
        when(queryPort.findPage(CouponRoundStatus.SCHEDULED, 1, 10))
                .thenReturn(expected);

        PublicCouponRoundPage result = queryService.findPage(
                CouponRoundStatus.SCHEDULED,
                1,
                10
        );

        assertThat(result).isSameAs(expected);
        verify(queryPort).findPage(CouponRoundStatus.SCHEDULED, 1, 10);
    }
}
