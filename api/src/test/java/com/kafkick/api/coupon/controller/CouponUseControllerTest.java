package com.kafkick.api.coupon.controller;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import org.springframework.dao.CannotAcquireLockException;
import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.api.coupon.dto.request.CouponUseRequest;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.api.coupon.service.CouponOperationRetryingExecutor;
import com.kafkick.core.coupon.service.result.CouponUseResult;
import com.kafkick.core.support.TimeProvider;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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
    private CouponOperationRetryingExecutor executionService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("회원 소유 쿠폰을 사용하면 실제 할인액을 반환한다")
    void useCoupon() throws Exception {
        when(executionService.use(
                eq(100L),
                eq(20L),
                eq(20_000),
                eq(IDEMPOTENCY_KEY)
        )).thenReturn(new CouponUseResult(
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
                                  "orderAmount": 20000
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(executionService, never()).use(
                anyLong(), anyLong(), anyInt(), anyString()
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
                                  "orderAmount": 0
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(executionService, never()).use(
                anyLong(), anyLong(), anyInt(), anyString()
        );
    }

    /**
     * <b>재시도가 소진된 뒤의 응답을 고정한다.</b> {@code LockContentionRetry} 는 상한에
     * 닿으면 어댑터가 감싼 예외를 <b>그대로</b> 올린다 — 맥락을 벗기지 않으려는 것인데,
     * 그러면 무엇이 클라이언트에 나가는지는 전역 처리기가 정한다.
     *
     * <p>부하 집계와 Chaos 자동 판정의 근거가 응답 코드라, 그 값이 조용히 바뀌면 측정이
     * 바뀐다. 여기서 못 박아 둔다.
     */
    @Test
    @DisplayName("락 재시도가 소진되면 500 과 COUPON-407 로 끝난다")
    void exhaustedLockRetryEndsAsServerError() throws Exception {
        when(executionService.use(eq(100L), eq(20L), anyInt(), anyString()))
                .thenThrow(new IdempotencyPersistenceException(
                        "멱등 기록 저장에 실패했습니다.",
                        new CannotAcquireLockException("Deadlock found")));

        mockMvc.perform(post("/api/v1/coupons/100/use")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(CouponRequestHeaders.IDEMPOTENCY_KEY, "a".repeat(32))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "orderAmount": 20000
                                }
                                """))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COUPON-407"));
    }
}
