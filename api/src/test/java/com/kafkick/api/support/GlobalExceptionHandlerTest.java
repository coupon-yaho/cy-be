package com.kafkick.api.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.exception.IdempotencyResponseCodecException;
import com.kafkick.core.support.TimeProvider;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class GlobalExceptionHandlerTest {

    @Test
    void mapsIdempotencyCodecFailureToCouponErrorContract()
            throws Exception {
        TimeProvider timeProvider = new TimeProvider(Clock.fixed(
                Instant.parse("2026-08-20T05:00:00Z"),
                ZoneOffset.UTC
        ));
        MockMvc mockMvc = MockMvcBuilders
                .standaloneSetup(new FailingController())
                .setControllerAdvice(new GlobalExceptionHandler(timeProvider))
                .build();

        mockMvc.perform(get("/test/idempotency-codec"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(500))
                .andExpect(jsonPath("$.error.code").value("COUPON-407"));
    }

    @RestController
    static class FailingController {

        @GetMapping("/test/idempotency-codec")
        void fail() {
            throw new IdempotencyResponseCodecException(
                    "테스트 코덱 실패",
                    new IllegalStateException("cause")
            );
        }
    }
}
