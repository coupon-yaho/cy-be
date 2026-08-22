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
import com.kafkick.core.coupon.query.IssuableCouponRoundPage;
import com.kafkick.core.coupon.query.IssuableCouponRoundSummary;
import com.kafkick.core.coupon.service.IssuableCouponRoundQueryService;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.TimeProvider;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CouponRoundController.class)
class CouponRoundControllerTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-22T09:00:00Z");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private IssuableCouponRoundQueryService queryService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("회원이 발급 가능한 쿠폰 회차 페이지를 조회한다")
    void findIssuableCouponRounds() throws Exception {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(queryService.findPage(
                20L,
                MembershipGrade.GOLD,
                AS_OF,
                0,
                20
        )).thenReturn(page());

        mockMvc.perform(get("/api/v1/coupon-rounds")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(MemberRequestHeaders.MEMBERSHIP_GRADE, "GOLD"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.content.length()").value(1))
                .andExpect(jsonPath("$.data.content[0].couponRoundId")
                        .value(10))
                .andExpect(jsonPath("$.data.content[0].brandId").value(1))
                .andExpect(jsonPath("$.data.content[0].name")
                        .value("골드 20% 할인"))
                .andExpect(jsonPath("$.data.content[0].policyType")
                        .value("PERCENT_CAPPED"))
                .andExpect(jsonPath("$.data.content[0].discountRate")
                        .value(20))
                .andExpect(jsonPath("$.data.content[0].maxDiscountAmount")
                        .value(10_000))
                .andExpect(jsonPath("$.data.content[0].discountAmount")
                        .doesNotExist())
                .andExpect(jsonPath("$.data.content[0].validDays").value(7))
                .andExpect(jsonPath("$.data.content[0].remainingQuantity")
                        .value(7))
                .andExpect(jsonPath("$.data.page").value(0))
                .andExpect(jsonPath("$.data.size").value(20))
                .andExpect(jsonPath("$.data.totalElements").value(1));

        verify(queryService).findPage(
                20L,
                MembershipGrade.GOLD,
                AS_OF,
                0,
                20
        );
    }

    @Test
    @DisplayName("지원하지 않는 멤버십 등급이면 400을 반환한다")
    void rejectUnknownMembershipGrade() throws Exception {
        mockMvc.perform(get("/api/v1/coupon-rounds")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(MemberRequestHeaders.MEMBERSHIP_GRADE, "BRONZE"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(queryService, never()).findPage(
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyInt()
        );
    }

    @Test
    @DisplayName("페이지 크기가 100을 초과하면 400을 반환한다")
    void rejectOversizedPage() throws Exception {
        mockMvc.perform(get("/api/v1/coupon-rounds")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(MemberRequestHeaders.MEMBERSHIP_GRADE, "GOLD")
                        .param("size", "101"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("페이지 크기는 100 이하여야 합니다."));
    }

    private static IssuableCouponRoundPage page() {
        return new IssuableCouponRoundPage(
                List.of(new IssuableCouponRoundSummary(
                        10L,
                        1L,
                        "골드 20% 할인",
                        CouponPolicyType.PERCENT_CAPPED,
                        20,
                        10_000,
                        null,
                        7,
                        Instant.parse("2026-08-22T08:00:00Z"),
                        Instant.parse("2026-08-22T10:00:00Z"),
                        7
                )),
                0,
                20,
                1,
                1
        );
    }
}
