package com.kafkick.api.coupon.controller;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.exception.CouponRoundErrorCode;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.coupon.query.IssuableCouponRoundPage;
import com.kafkick.core.coupon.query.IssuableCouponRoundSummary;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;
import com.kafkick.core.coupon.service.CouponRoundDetailQueryService;
import com.kafkick.core.coupon.service.IssuableCouponRoundQueryService;
import com.kafkick.core.coupon.service.PublicCouponRoundQueryService;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

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
    private CouponRoundDetailQueryService detailQueryService;

    @MockitoBean
    private PublicCouponRoundQueryService publicQueryService;

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
                        .header(MemberRequestHeaders.MEMBER_GRADE, "GOLD"))
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
                        .header(MemberRequestHeaders.MEMBER_GRADE, "BRONZE"))
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
                        .header(MemberRequestHeaders.MEMBER_GRADE, "GOLD")
                        .param("size", "101"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("페이지 크기는 100 이하여야 합니다."));
    }

    @Test
    @DisplayName("회원 헤더 없이 상태별 공개 쿠폰 회차를 조회한다")
    void findPublicCouponRounds() throws Exception {
        when(publicQueryService.findPage(
                CouponRoundStatus.SCHEDULED,
                MembershipGrade.GOLD,
                0,
                20
        )).thenReturn(publicPage());

        mockMvc.perform(get("/api/v1/coupon-rounds/public")
                        .param("status", "SCHEDULED")
                        .param("eligibleGrade", "GOLD"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.content.length()").value(1))
                .andExpect(jsonPath("$.data.content[0].couponRoundId")
                        .value(10))
                .andExpect(jsonPath("$.data.content[0].templateId").value(1))
                .andExpect(jsonPath("$.data.content[0].brandId").value(2))
                .andExpect(jsonPath("$.data.content[0].eligibleGrades[0]")
                        .value("GOLD"))
                .andExpect(jsonPath("$.data.content[0].eligibleGrades[1]")
                        .value("VIP"))
                .andExpect(jsonPath("$.data.content[0].status")
                        .value("SCHEDULED"))
                .andExpect(jsonPath("$.data.content[0].totalQuantity")
                        .value(100))
                .andExpect(jsonPath("$.data.content[0].remainingQuantity")
                        .value(80))
                .andExpect(jsonPath("$.data.page").value(0))
                .andExpect(jsonPath("$.data.size").value(20))
                .andExpect(jsonPath("$.data.totalElements").value(1));

        verify(publicQueryService).findPage(
                CouponRoundStatus.SCHEDULED,
                MembershipGrade.GOLD,
                0,
                20
        );
    }

    @Test
    @DisplayName("공개 회차 조회에서 지원하지 않는 상태는 400을 반환한다")
    void rejectUnknownCouponRoundStatus() throws Exception {
        mockMvc.perform(get("/api/v1/coupon-rounds/public")
                        .param("status", "WAITING"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(publicQueryService, never()).findPage(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyInt()
        );
    }

    @Test
    @DisplayName("공개 회차 조회에서 지원하지 않는 회원 등급은 400을 반환한다")
    void rejectUnknownEligibleGrade() throws Exception {
        mockMvc.perform(get("/api/v1/coupon-rounds/public")
                        .param("eligibleGrade", "PLATINUM"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(publicQueryService, never()).findPage(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyInt()
        );
    }

    @Test
    @DisplayName("공개 회차 페이지 크기가 100을 초과하면 400을 반환한다")
    void rejectOversizedPublicCouponRoundPage() throws Exception {
        mockMvc.perform(get("/api/v1/coupon-rounds/public")
                        .param("size", "101"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("페이지 크기는 100 이하여야 합니다."));

        verify(publicQueryService, never()).findPage(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyInt()
        );
    }

    @Test
    @DisplayName("회원 헤더 없이 쿠폰 회차 상세 정보를 조회한다")
    void findCouponRoundDetail() throws Exception {
        when(detailQueryService.findById(10L)).thenReturn(detail());

        mockMvc.perform(get("/api/v1/coupon-rounds/{couponRoundId}", 10L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.couponRoundId").value(10))
                .andExpect(jsonPath("$.data.templateId").value(1))
                .andExpect(jsonPath("$.data.brandId").value(2))
                .andExpect(jsonPath("$.data.name")
                        .value("골드 VIP 20% 할인"))
                .andExpect(jsonPath("$.data.policyType")
                        .value("PERCENT_CAPPED"))
                .andExpect(jsonPath("$.data.discountRate").value(20))
                .andExpect(jsonPath("$.data.maxDiscountAmount")
                        .value(10_000))
                .andExpect(jsonPath("$.data.discountAmount").doesNotExist())
                .andExpect(jsonPath("$.data.validDays").value(7))
                .andExpect(jsonPath("$.data.eligibleGrades[0]")
                        .value("GOLD"))
                .andExpect(jsonPath("$.data.eligibleGrades[1]")
                        .value("VIP"))
                .andExpect(jsonPath("$.data.status").value("OPEN"))
                .andExpect(jsonPath("$.data.totalQuantity").value(100))
                .andExpect(jsonPath("$.data.remainingQuantity").value(80));

        verify(detailQueryService).findById(10L);
    }

    @Test
    @DisplayName("존재하지 않는 쿠폰 회차 상세 조회는 404를 반환한다")
    void rejectMissingCouponRoundDetail() throws Exception {
        when(detailQueryService.findById(999L)).thenThrow(
                new BusinessException(
                        CouponRoundErrorCode.COUPON_ROUND_NOT_FOUND
                )
        );

        mockMvc.perform(get("/api/v1/coupon-rounds/{couponRoundId}", 999L))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code")
                        .value("COUPON_ROUND-204"));
    }

    @Test
    @DisplayName("0 이하의 쿠폰 회차 ID는 400을 반환한다")
    void rejectNonPositiveCouponRoundId() throws Exception {
        mockMvc.perform(get("/api/v1/coupon-rounds/{couponRoundId}", 0L))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(detailQueryService, never()).findById(0L);
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
                Set.of(MembershipGrade.VIP, MembershipGrade.GOLD),
                Instant.parse("2026-08-24T01:00:00Z"),
                Instant.parse("2026-08-24T03:00:00Z"),
                CouponRoundStatus.OPEN,
                100,
                80
        );
    }

    private static PublicCouponRoundPage publicPage() {
        return new PublicCouponRoundPage(
                List.of(new CouponRoundDetail(
                        10L,
                        1L,
                        2L,
                        "골드 VIP 20% 할인",
                        CouponPolicyType.PERCENT_CAPPED,
                        20,
                        10_000,
                        null,
                        7,
                        Set.of(MembershipGrade.VIP, MembershipGrade.GOLD),
                        Instant.parse("2026-08-25T01:00:00Z"),
                        Instant.parse("2026-08-25T03:00:00Z"),
                        CouponRoundStatus.SCHEDULED,
                        100,
                        80
                )),
                0,
                20,
                1,
                1
        );
    }
}
