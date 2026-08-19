// 쿠폰 발급 취소 API의 소유자 헤더·멱등키·응답 봉투 계약을 검증합니다.
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
import com.kafkick.api.coupon.adapter.CouponCancelTransactionalAdapter;
import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.support.TimeProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CouponCancelController.class)
class CouponCancelControllerTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440002";

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponCancelTransactionalAdapter cancelAdapter;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("회원 소유 쿠폰의 발급을 취소하면 CANCELLED 상태를 반환한다")
    void cancelCouponIssuance() throws Exception {
        when(cancelAdapter.cancel(100L, 20L, IDEMPOTENCY_KEY))
                .thenReturn(new CouponCancelResponse(
                        100L,
                        IssuanceStatus.CANCELLED,
                        Instant.parse("2026-08-20T05:00:00Z")
                ));

        mockMvc.perform(post("/api/v1/coupons/100/cancel")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.issuanceId").value(100))
                .andExpect(jsonPath("$.data.status").value("CANCELLED"))
                .andExpect(jsonPath("$.data.canceledAt")
                        .value("2026-08-20T05:00:00Z"))
                .andExpect(jsonPath("$.error").doesNotExist());
    }

    @Test
    @DisplayName("멱등키 헤더가 없으면 발급 취소를 실행하지 않고 400을 반환한다")
    void rejectMissingIdempotencyKey() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/100/cancel")
                        .header(MemberRequestHeaders.MEMBER_ID, "20"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(cancelAdapter, never()).cancel(any(), any(), any());
    }
}
