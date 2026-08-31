package com.kafkick.api.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.support.auth.RequestHeaderContractException;
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

    /**
     * <b>400 이 "잘못된 요청입니다" 만 말하면 호출자는 서버 결함과 구분하지 못한다.</b>
     * 대기열 게이트웨이와 발급의 등급 헤더 이름이 어긋났을 때 양쪽 담당자가 이 400 을
     * 각자 한참 들여다봤다 — 이름을 맞추는 것으로는 다음번 다른 헤더를 못 막는다.
     *
     * <p>값이 아니라 <b>이름만</b> 나가는 것도 함께 본다. 값은 회원 식별자나 등급이다.
     */
    @Test
    @DisplayName("헤더가 없으면 어느 헤더인지 응답이 말한다")
    void namesTheMissingRequestHeader() throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/needs-header"))
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("필수 요청 헤더가 없습니다: X-Member-Grade"));
    }

    /**
     * <b>등급 헤더는 위 갈래를 안 탄다.</b> 게이트웨이 전환 때 {@code required = false} 로
     * 받아 리졸버가 직접 거부하게 됐기 때문이다 — 스프링의 누락 예외가 안 나므로 헤더 이름을
     * 싣는 처리기가 안 걸린다. <b>하필 이번에 이름이 어긋나 문제가 된 그 헤더가 예외였다.</b>
     *
     * <p>그래서 헤더 계약 위반을 따로 잡아 문구를 그대로 싣는다. 값이 아니라 코드가 정한
     * 고정 문구다.
     */
    @Test
    @DisplayName("헤더 계약을 어기면 이유가 응답에 그대로 나온다")
    void surfacesHeaderContractMessage() throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/header-contract"))
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("회원 등급 헤더는 하나의 값만 허용합니다."));
    }

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

    @Test
    void mapsRedisFailoverToRetryableServiceUnavailable() throws Exception {
        MockMvc mockMvc = mockMvc();

        mockMvc.perform(get("/test/redis-unavailable"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.error.code").value("COUPON-325"));
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

        @GetMapping("/test/redis-unavailable")
        void redisUnavailable() {
            throw new RetryAfterException(CouponIssueV2ErrorCode.REDIS_UNAVAILABLE, 1);
        }

        /** 헤더 계약 위반. 리졸버가 던지는 것과 같은 예외다. */
        @GetMapping("/test/header-contract")
        void headerContract() {
            throw new RequestHeaderContractException(
                    RequestHeaderContractException.Reason.MULTIPLE_MEMBER_GRADE);
        }

        /** 헤더 하나를 필수로 받는다. 안 주면 MissingRequestHeaderException 이 난다. */
        @GetMapping("/test/needs-header")
        void needsHeader(@RequestHeader("X-Member-Grade") String grade) {
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
