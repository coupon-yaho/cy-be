package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.port.IssuableCouponRoundQueryPort;
import com.kafkick.core.coupon.query.IssuableCouponRoundPage;
import com.kafkick.core.membership.domain.MembershipGrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IssuableCouponRoundQueryServiceTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-22T09:00:00Z");

    @Mock
    private IssuableCouponRoundQueryPort queryPort;

    @InjectMocks
    private IssuableCouponRoundQueryService queryService;

    @Test
    @DisplayName("회원 등급 비트와 조회 시각으로 발급 가능한 회차를 조회한다")
    void findIssuableCouponRounds() {
        IssuableCouponRoundPage expected = new IssuableCouponRoundPage(
                List.of(),
                1,
                10,
                0,
                0
        );
        when(queryPort.findPage(20L, 4, AS_OF, 1, 10))
                .thenReturn(expected);

        IssuableCouponRoundPage result = queryService.findPage(
                20L,
                MembershipGrade.GOLD,
                AS_OF,
                1,
                10
        );

        assertThat(result).isSameAs(expected);
        verify(queryPort).findPage(20L, 4, AS_OF, 1, 10);
    }
}
