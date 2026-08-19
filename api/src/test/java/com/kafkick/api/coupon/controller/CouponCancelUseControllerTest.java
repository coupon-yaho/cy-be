// 쿠폰 사용 취소 API의 소유자 헤더·멱등키·응답 봉투 계약을 검증합니다.
package com.kafkick.api.coupon.controller;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.coupon.CouponRequestHeaders;
import com.kafkick.api.coupon.MemberRequestHeaders;
import com.kafkick.api.coupon.adapter.CouponCancelUseTransactionalAdapter;
import com.kafkick.api.coupon.dto.CouponCancelUseResponse;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.support.TimeProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CouponCancelUseController.class)
class CouponCancelUseControllerTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponCancelUseTransactionalAdapter cancelUseAdapter;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("회원 소유 쿠폰의 사용을 취소하면 변경 상태와 주문 실적을 반환한다")
    void cancelCouponUse() throws Exception {
        when(cancelUseAdapter.cancelUse(100L, 20L, IDEMPOTENCY_KEY))
                .thenReturn(new CouponCancelUseResponse(
                        100L,
                        IssuanceStatus.ISSUED,
                        30L,
                        5_000,
                        Instant.parse("2026-08-20T05:00:00Z")
                ));

        mockMvc.perform(post("/api/v1/coupons/100/cancel-use")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.issuanceId").value(100))
                .andExpect(jsonPath("$.data.status").value("ISSUED"))
                .andExpect(jsonPath("$.data.orderId").value(30))
                .andExpect(jsonPath("$.data.discountAmount").value(5000))
                .andExpect(jsonPath("$.error").doesNotExist());
    }

    @Test
    @DisplayName("멱등키 헤더가 없으면 400을 반환한다")
    void rejectMissingIdempotencyKey() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/100/cancel-use")
                        .header(MemberRequestHeaders.MEMBER_ID, "20"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(cancelUseAdapter, never()).cancelUse(any(), any(), any());
    }

    @Test
    @DisplayName("회원 ID가 0 이하면 400을 반환한다")
    void rejectNonPositiveMemberId() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/100/cancel-use")
                        .header(MemberRequestHeaders.MEMBER_ID, "0")
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(cancelUseAdapter, never()).cancelUse(any(), any(), any());
    }
}
