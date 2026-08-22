package com.kafkick.api.coupon.controller;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponQueryErrorCode;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.core.coupon.query.MemberCouponSummary;
import com.kafkick.core.coupon.service.MemberCouponQueryService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// 사용자 보유 쿠폰 목록과 단건 상세 조회의 HTTP 계약을 검증합니다.

@WebMvcTest(MemberCouponController.class)
class MemberCouponControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MemberCouponQueryService memberCouponQueryService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("회원 헤더로 모든 보유 쿠폰의 기본 페이지를 조회한다")
    void findAllMemberCoupons() throws Exception {
        when(memberCouponQueryService.findPage(20L, null, 0, 20))
                .thenReturn(page());

        mockMvc.perform(get("/api/v1/coupons")
                        .header(MemberRequestHeaders.MEMBER_ID, "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.content.length()").value(1))
                .andExpect(jsonPath("$.data.content[0].issuanceId")
                        .value(100))
                .andExpect(jsonPath("$.data.content[0].couponRoundId")
                        .value(10))
                .andExpect(jsonPath("$.data.content[0].code")
                        .value("ABCDEFGHJKLM2345"))
                .andExpect(jsonPath("$.data.content[0].status")
                        .value("ISSUED"))
                .andExpect(jsonPath("$.data.content[0].name")
                        .value("골드 VIP 20% 할인"))
                .andExpect(jsonPath("$.data.content[0].policyType")
                        .value("PERCENT_CAPPED"))
                .andExpect(jsonPath("$.data.content[0].discountRate")
                        .value(20))
                .andExpect(jsonPath("$.data.content[0].maxDiscountAmount")
                        .value(10_000))
                .andExpect(jsonPath("$.data.content[0].discountAmount")
                        .doesNotExist())
                .andExpect(jsonPath("$.data.content[0].issuedAt")
                        .value("2026-08-18T05:30:00Z"))
                .andExpect(jsonPath("$.data.content[0].expiresAt")
                        .value("2026-08-25T05:30:00Z"))
                .andExpect(jsonPath("$.data.page").value(0))
                .andExpect(jsonPath("$.data.size").value(20))
                .andExpect(jsonPath("$.data.totalElements").value(1))
                .andExpect(jsonPath("$.data.totalPages").value(1));

        verify(memberCouponQueryService).findPage(20L, null, 0, 20);
    }

    @Test
    @DisplayName("상태와 페이지 조건으로 회원 보유 쿠폰을 조회한다")
    void findMemberCouponsByStatus() throws Exception {
        when(memberCouponQueryService.findPage(
                20L,
                IssuanceStatus.USED,
                1,
                10
        )).thenReturn(new MemberCouponPage(List.of(), 1, 10, 0, 0));

        mockMvc.perform(get("/api/v1/coupons")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .param("status", "USED")
                        .param("page", "1")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.content").isEmpty())
                .andExpect(jsonPath("$.data.page").value(1))
                .andExpect(jsonPath("$.data.size").value(10));

        verify(memberCouponQueryService).findPage(
                20L,
                IssuanceStatus.USED,
                1,
                10
        );
    }

    @Test
    @DisplayName("회원이 소유한 쿠폰 한 건을 상세 조회한다")
    void findOwnedMemberCoupon() throws Exception {
        when(memberCouponQueryService.findOne(20L, 100L))
                .thenReturn(coupon());

        mockMvc.perform(get("/api/v1/coupons/100")
                        .header(MemberRequestHeaders.MEMBER_ID, "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.issuanceId").value(100))
                .andExpect(jsonPath("$.data.couponRoundId").value(10))
                .andExpect(jsonPath("$.data.code")
                        .value("ABCDEFGHJKLM2345"))
                .andExpect(jsonPath("$.data.status").value("ISSUED"))
                .andExpect(jsonPath("$.data.name")
                        .value("골드 VIP 20% 할인"))
                .andExpect(jsonPath("$.data.policyType")
                        .value("PERCENT_CAPPED"))
                .andExpect(jsonPath("$.data.discountRate").value(20))
                .andExpect(jsonPath("$.data.maxDiscountAmount")
                        .value(10_000))
                .andExpect(jsonPath("$.data.discountAmount")
                        .doesNotExist())
                .andExpect(jsonPath("$.data.issuedAt")
                        .value("2026-08-18T05:30:00Z"))
                .andExpect(jsonPath("$.data.expiresAt")
                        .value("2026-08-25T05:30:00Z"));

        verify(memberCouponQueryService).findOne(20L, 100L);
    }

    @Test
    @DisplayName("회원 소유 쿠폰이 없으면 404를 반환한다")
    void rejectMissingOrUnownedMemberCoupon() throws Exception {
        when(memberCouponQueryService.findOne(20L, 200L))
                .thenThrow(new BusinessException(
                        CouponQueryErrorCode.MEMBER_COUPON_NOT_FOUND
                ));

        mockMvc.perform(get("/api/v1/coupons/200")
                        .header(MemberRequestHeaders.MEMBER_ID, "20"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COUPON-415"))
                .andExpect(jsonPath("$.error.message")
                        .value("보유 쿠폰을 찾을 수 없습니다."));
    }

    @Test
    @DisplayName("쿠폰 발급 ID가 양수가 아니면 400을 반환한다")
    void rejectNonPositiveIssuanceId() throws Exception {
        mockMvc.perform(get("/api/v1/coupons/0")
                        .header(MemberRequestHeaders.MEMBER_ID, "20"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(memberCouponQueryService, never()).findOne(
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong()
        );
    }

    @Test
    @DisplayName("회원 헤더가 없으면 400을 반환한다")
    void rejectMissingMemberHeader() throws Exception {
        mockMvc.perform(get("/api/v1/coupons"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(memberCouponQueryService, never()).findPage(
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyInt()
        );
    }

    @Test
    @DisplayName("지원하지 않는 쿠폰 상태이면 400을 반환한다")
    void rejectUnknownStatus() throws Exception {
        mockMvc.perform(get("/api/v1/coupons")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .param("status", "AVAILABLE"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    @Test
    @DisplayName("페이지 크기가 100을 초과하면 400을 반환한다")
    void rejectOversizedPage() throws Exception {
        mockMvc.perform(get("/api/v1/coupons")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .param("size", "101"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("페이지 크기는 100 이하여야 합니다."));
    }

    private static MemberCouponPage page() {
        return new MemberCouponPage(
                List.of(coupon()),
                0,
                20,
                1,
                1
        );
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
