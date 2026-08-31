package com.kafkick.api.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponIssueV2ErrorCode;
import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class GlobalExceptionHandlerTest {

    private static final Instant FIXED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Test
    void mapsServiceBusinessExceptionWithoutExposingLogDetail()
            throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/business"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(409))
                .andExpect(jsonPath("$.error.code").value("COUPON-305"))
                .andExpect(jsonPath("$.error.message")
                        .value("이미 발급받은 쿠폰입니다."));
    }

    @Test
    void mapsUnexpectedServiceExceptionToCommonInternalError()
            throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/unexpected"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(500))
                .andExpect(jsonPath("$.error.code").value("COMMON-004"))
                .andExpect(jsonPath("$.error.message")
                        .value("일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."));
    }

    @Test
    void mapsIdempotencyCodecFailureToCouponErrorContract()
            throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/idempotency-codec"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(500))
                .andExpect(jsonPath("$.error.code").value("COUPON-407"));
    }

    @Test
    void addsRetryAfterHeaderForRetryableIssueFailures() throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/retry-after"))
                .andExpect(status().isConflict())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.error.code").value("COUPON-320"));
    }

    /** Retry-After 는 재시도로 풀리는 실패에만 붙는다. */
    @Test
    void omitsRetryAfterHeaderForOtherBusinessFailures() throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/business"))
                .andExpect(header().doesNotExist("Retry-After"));
    }

    private MockMvc mockMvc() {
        TimeProvider timeProvider = new TimeProvider(Clock.fixed(
                FIXED_AT,
                ZoneOffset.UTC
        ));
        return MockMvcBuilders
                .standaloneSetup(new FailingController())
                .setControllerAdvice(new GlobalExceptionHandler(timeProvider))
                .build();
    }

    @RestController
    static class FailingController {

        @GetMapping("/test/idempotency-codec")
        void fail() {
            throw new IdempotencyPersistenceException(
                    "테스트 코덱 실패",
                    new IllegalStateException("cause")
            );
        }

        @GetMapping("/test/retry-after")
        void retryable() {
            throw new RetryAfterException(CouponIssueV2ErrorCode.REPLAY_PENDING, 1);
        }

        @GetMapping("/test/business")
        void businessFailure() {
            throw new BusinessException(
                    CouponIssueErrorCode.ALREADY_ISSUED,
                    "memberId=20, couponRoundId=100"
            );
        }

        @GetMapping("/test/unexpected")
        void unexpectedFailure() {
            throw new IllegalStateException(
                    "raw failure must not be exposed"
            );
        }
    }
}
