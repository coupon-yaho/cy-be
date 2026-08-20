package com.kafkick.api.coupon.controller;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.coupon.CouponRequestHeaders;
import com.kafkick.api.coupon.MemberRequestHeaders;
import com.kafkick.api.coupon.adapter.CouponUseAdapter;
import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.support.TimeProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// 쿠폰 사용 API의 소유자·멱등키·주문 입력과 공통 응답 계약을 검증합니다.

@WebMvcTest(CouponUseController.class)
class CouponUseControllerTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponUseAdapter couponUseAdapter;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("회원 소유 쿠폰을 사용하면 실제 할인액을 반환한다")
    void useCoupon() throws Exception {
        when(couponUseAdapter.use(
                eq(100L),
                eq(20L),
                eq(IDEMPOTENCY_KEY),
                any(CouponUseRequest.class)
        )).thenReturn(new CouponUseResponse(
                100L,
                IssuanceStatus.USED,
                30L,
                5_000,
                Instant.parse("2026-08-20T05:00:00Z")
        ));

        mockMvc.perform(post("/api/v1/coupons/100/use")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        )
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "orderId": 30,
                                  "orderAmount": 20000
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.issuanceId").value(100))
                .andExpect(jsonPath("$.data.status").value("USED"))
                .andExpect(jsonPath("$.data.orderId").value(30))
                .andExpect(jsonPath("$.data.discountAmount").value(5000))
                .andExpect(jsonPath("$.error").doesNotExist());
    }

    @Test
    @DisplayName("멱등키 헤더가 없으면 400을 반환한다")
    void rejectMissingIdempotencyKey() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/100/use")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "orderId": 30,
                                  "orderAmount": 20000
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(couponUseAdapter, never()).use(
                any(), any(), any(), any()
        );
    }

    @Test
    @DisplayName("주문 금액이 0 이하면 400을 반환한다")
    void rejectNonPositiveOrderAmount() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/100/use")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        )
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "orderId": 30,
                                  "orderAmount": 0
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(couponUseAdapter, never()).use(
                any(), any(), any(), any()
        );
    }
}
