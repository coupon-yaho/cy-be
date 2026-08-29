package com.kafkick.core.coupon.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.service.command.CouponCancelCommand;
import com.kafkick.core.coupon.service.command.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.membership.domain.MembershipGrade;

import static org.assertj.core.api.Assertions.assertThat;

class CouponIdempotencyRequestIdentityTest {

    @Test
    @DisplayName("쿠폰 발급 요청의 동일성은 회차·회원·발급 등급으로 결정한다")
    void canonicalizeCouponIssueRequest() {
        assertThat(CouponIssueCommand.canonicalRequest(
                10L,
                20L,
                MembershipGrade.GOLD
        )).isEqualTo(
                "ISSUE|couponRoundId=10|memberId=20|membershipGrade=GOLD"
        );
    }

    @Test
    @DisplayName("쿠폰 사용 요청의 동일성은 발급건·회원·주문·주문금액으로 결정한다")
    void canonicalizeCouponUseRequest() {
        assertThat(CouponUseCommand.canonicalRequest(
                100L,
                20L,
                30L,
                20_000
        )).isEqualTo(
                "USE|issuanceId=100|memberId=20|orderId=30|orderAmount=20000"
        );
    }

    @Test
    @DisplayName("쿠폰 사용 취소 요청의 동일성은 발급건과 회원으로 결정한다")
    void canonicalizeCouponCancelUseRequest() {
        assertThat(CouponCancelUseCommand.canonicalRequest(100L, 20L))
                .isEqualTo("CANCEL_USE|issuanceId=100|memberId=20");
    }

    @Test
    @DisplayName("쿠폰 발급 취소 요청의 동일성은 발급건과 회원으로 결정한다")
    void canonicalizeCouponCancelRequest() {
        assertThat(CouponCancelCommand.canonicalRequest(100L, 20L))
                .isEqualTo("CANCEL|issuanceId=100|memberId=20");
    }
}
