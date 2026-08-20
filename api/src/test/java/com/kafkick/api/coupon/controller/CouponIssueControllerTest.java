package com.kafkick.api.coupon.controller;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.CouponIssueService;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// 사용자 발급 API의 헤더 입력과 201 공통 응답 계약을 검증합니다.

@WebMvcTest(CouponIssueController.class)
class CouponIssueControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponIssueService couponIssueService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("회원과 등급 헤더로 쿠폰을 발급하면 201을 반환한다")
    void issueCoupon() throws Exception {
        Issuance issuance = issuance();
        when(timeProvider.instant()).thenReturn(
                Instant.parse("2026-08-18T05:30:00Z")
        );
        when(couponIssueService.issue(any(CouponIssueCommand.class)))
                .thenReturn(issuance);

        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                MemberRequestHeaders.MEMBERSHIP_GRADE,
                                "GOLD"
                        ))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.issuanceId").value(100))
                .andExpect(jsonPath("$.data.couponRoundId").value(10))
                .andExpect(jsonPath("$.data.code")
                        .value("ABCDEFGHJKLM2345"))
                .andExpect(jsonPath("$.data.status").value("ISSUED"))
                .andExpect(jsonPath("$.error").doesNotExist());

        ArgumentCaptor<CouponIssueCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponIssueCommand.class);
        verify(couponIssueService).issue(commandCaptor.capture());
        CouponIssueCommand command = commandCaptor.getValue();
        assertThat(command.couponRoundId()).isEqualTo(10L);
        assertThat(command.memberId()).isEqualTo(20L);
        assertThat(command.membershipGrade()).isEqualTo(MembershipGrade.GOLD);
        assertThat(command.issuedAt())
                .isEqualTo(Instant.parse("2026-08-18T05:30:00Z"));
    }

    @Test
    @DisplayName("회원 헤더가 없으면 400을 반환한다")
    void rejectMissingMemberHeader() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(
                                MemberRequestHeaders.MEMBERSHIP_GRADE,
                                "GOLD"
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(couponIssueService, never()).issue(any());
    }

    @Test
    @DisplayName("지원하지 않는 등급 헤더는 400을 반환한다")
    void rejectInvalidMembershipGrade() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                MemberRequestHeaders.MEMBERSHIP_GRADE,
                                "PLATINUM"
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    private Issuance issuance() {
        return Issuance.restore(
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                Instant.parse("2026-08-18T05:30:00Z"),
                Instant.parse("2026-08-25T05:30:00Z"),
                Instant.parse("2026-08-18T05:30:00Z")
        );
    }
}
