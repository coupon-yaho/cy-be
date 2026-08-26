package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.port.MemberCouponQueryPort;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.core.coupon.query.MemberCouponSummary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 사용자 보유 쿠폰 목록 조회 계약을 검증합니다.

@ExtendWith(MockitoExtension.class)
class MemberCouponQueryServiceTest {

    @Mock
    private MemberCouponQueryPort memberCouponQueryPort;

    @InjectMocks
    private MemberCouponQueryService memberCouponQueryService;

    @Test
    @DisplayName("상태 조건 없이 회원의 보유 쿠폰 페이지를 조회한다")
    void findAllMemberCoupons() {
        MemberCouponPage expected = page();
        when(memberCouponQueryPort.findPageByMemberId(
                20L,
                null,
                0,
                20
        )).thenReturn(expected);

        MemberCouponPage result = memberCouponQueryService.findPage(
                20L,
                null,
                0,
                20
        );

        assertThat(result).isSameAs(expected);
        verify(memberCouponQueryPort).findPageByMemberId(
                20L,
                null,
                0,
                20
        );
    }

    @Test
    @DisplayName("상태 조건으로 회원의 보유 쿠폰 페이지를 조회한다")
    void findMemberCouponsByStatus() {
        MemberCouponPage expected = page();
        when(memberCouponQueryPort.findPageByMemberId(
                20L,
                IssuanceStatus.ISSUED,
                1,
                10
        )).thenReturn(expected);

        MemberCouponPage result = memberCouponQueryService.findPage(
                20L,
                IssuanceStatus.ISSUED,
                1,
                10
        );

        assertThat(result).isSameAs(expected);
        verify(memberCouponQueryPort).findPageByMemberId(
                20L,
                IssuanceStatus.ISSUED,
                1,
                10
        );
    }

    private static MemberCouponPage page() {
        return new MemberCouponPage(List.of(coupon()), 0, 20, 1, 1);
    }

    private static MemberCouponSummary coupon() {
        return new MemberCouponSummary(
                100L,
                10L,
                "ABCDEFGHJKLM2345",
                IssuanceStatus.ISSUED,
                "골드 VIP 20% 할인",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                10_000,
                null,
                Instant.parse("2026-08-18T05:30:00Z"),
                Instant.parse("2026-08-25T05:30:00Z"),
                null,
                null,
                null
        );
    }
}
