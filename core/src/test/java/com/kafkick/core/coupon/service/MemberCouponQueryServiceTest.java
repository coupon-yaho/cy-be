package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponQueryErrorCode;
import com.kafkick.core.coupon.port.MemberCouponQueryPort;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.core.coupon.query.MemberCouponSummary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 사용자 보유 쿠폰 목록과 단건 상세 조회 계약을 검증합니다.

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

    @Test
    @DisplayName("회원이 소유한 쿠폰 한 건을 조회한다")
    void findOwnedMemberCoupon() {
        MemberCouponSummary expected = coupon();
        when(memberCouponQueryPort.findByMemberIdAndIssuanceId(
                20L,
                100L
        )).thenReturn(Optional.of(expected));

        MemberCouponSummary result = memberCouponQueryService.findOne(
                20L,
                100L
        );

        assertThat(result).isSameAs(expected);
        verify(memberCouponQueryPort).findByMemberIdAndIssuanceId(
                20L,
                100L
        );
    }

    @Test
    @DisplayName("회원 소유 쿠폰이 없으면 동일한 404 오류를 반환한다")
    void rejectMissingOrUnownedMemberCoupon() {
        when(memberCouponQueryPort.findByMemberIdAndIssuanceId(
                20L,
                200L
        )).thenReturn(Optional.empty());

        assertThatThrownBy(() -> memberCouponQueryService.findOne(20L, 200L))
                .isInstanceOfSatisfying(
                        com.kafkick.core.support.exception.BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(CouponQueryErrorCode.MEMBER_COUPON_NOT_FOUND)
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
                Instant.parse("2026-08-25T05:30:00Z")
        );
    }
}
