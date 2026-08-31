package com.kafkick.api.coupon.controller;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.coupon.monitoring.CouponIssueMetrics;
import com.kafkick.api.observation.issuance.CouponIssueObservationCoordinator;
import com.kafkick.api.support.RequestIdFilter;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// 사용자 발급 API의 헤더 입력과 201 공통 응답 계약을 검증합니다.

@WebMvcTest(CouponIssueController.class)
class CouponIssueControllerTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String REQUEST_ID = "client-request-1";

    private final MockMvc mockMvc;

    @MockitoBean
    private CouponIssueObservationCoordinator observationCoordinator;

    @MockitoBean
    private CouponIssueMetrics couponIssueMetrics;

    @MockitoBean
    private TimeProvider timeProvider;

    @Autowired
    CouponIssueControllerTest(MockMvc mockMvc) {
        this.mockMvc = mockMvc;
    }

    @Test
    @DisplayName("게이트웨이 회원 등급 헤더로 쿠폰을 발급하면 201을 반환한다")
    void issueCoupon() throws Exception {
        CouponIssueResult result = issueResult();
        when(observationCoordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).thenReturn(result);

        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, REQUEST_ID)
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                "X-Member-Grade",
                                "GOLD"
                        )
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isCreated())
                .andExpect(header().string(
                        RequestIdFilter.REQUEST_ID_HEADER,
                        REQUEST_ID
                ))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.issuanceId").value(100))
                .andExpect(jsonPath("$.data.couponRoundId").value(10))
                .andExpect(jsonPath("$.data.code")
                        .value("ABCDEFGHJKLM2345"))
                .andExpect(jsonPath("$.data.status").value("ISSUED"))
                .andExpect(jsonPath("$.error").doesNotExist());

        verify(observationCoordinator).issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );
        verify(couponIssueMetrics).recordStarted(10L, 20L);
        verify(couponIssueMetrics).recordSuccess(
                org.mockito.ArgumentMatchers.eq(10L),
                org.mockito.ArgumentMatchers.eq(20L),
                org.mockito.ArgumentMatchers.longThat(duration -> duration >= 0)
        );
    }

    @Test
    @DisplayName("같은 게이트웨이 등급 헤더가 여러 값이면 요청을 거부한다")
    void rejectMultipleMemberGradeHeaderValues() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, REQUEST_ID)
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(MemberRequestHeaders.MEMBER_GRADE, "GOLD", "VIP")
                        .header(CouponRequestHeaders.IDEMPOTENCY_KEY, IDEMPOTENCY_KEY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(observationCoordinator, never())
                .issue(org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any());
    }

    /**
     * <b>헤더 계약 위반은 장애가 아니라 입력 거절이다.</b>
     *
     * <p>이 컨트롤러는 {@code BusinessException} 을 입력 거절로, 그 밖의
     * {@code RuntimeException} 을 예기치 못한 장애로 집계한다. 헤더 계약 위반이 뒤쪽으로
     * 새면 <b>클라이언트 잘못이 서버 에러율에 얹힌다</b> — 부하 측정의 판정 근거가 그만큼
     * 오염되는데, 응답은 400 이라 화면에서도 로그에서도 안 보인다.
     *
     * <p>실제로 한 번 그렇게 만들었다가 리뷰가 잡았다. 상속을 되돌리면 이 단언이 깨진다.
     */
    @Test
    @DisplayName("등급 헤더 계약 위반은 장애가 아니라 입력 거절로 집계된다")
    void countsHeaderContractFailureAsRejectionNotError() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, REQUEST_ID)
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(MemberRequestHeaders.MEMBER_GRADE, "GOLD", "VIP")
                        .header(CouponRequestHeaders.IDEMPOTENCY_KEY, IDEMPOTENCY_KEY))
                .andExpect(status().isBadRequest());

        verify(couponIssueMetrics, never()).recordUnexpectedFailure(
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    @DisplayName("요청 ID 헤더가 없으면 필터가 생성한 ID로 발급한다")
    void issueCouponWithGeneratedRequestId() throws Exception {
        when(observationCoordinator.issue(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.eq(10L),
                org.mockito.ArgumentMatchers.eq(20L),
                org.mockito.ArgumentMatchers.eq(MembershipGrade.GOLD),
                org.mockito.ArgumentMatchers.eq(IDEMPOTENCY_KEY)
        )).thenReturn(issueResult());

        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                MemberRequestHeaders.MEMBER_GRADE,
                                "GOLD"
                        )
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isCreated());

        verify(observationCoordinator).issue(
                org.mockito.ArgumentMatchers.argThat(requestId ->
                        requestId.matches("[0-9a-f]{32}")
                ),
                org.mockito.ArgumentMatchers.eq(10L),
                org.mockito.ArgumentMatchers.eq(20L),
                org.mockito.ArgumentMatchers.eq(MembershipGrade.GOLD),
                org.mockito.ArgumentMatchers.eq(IDEMPOTENCY_KEY)
        );
    }

    @Test
    @DisplayName("동일한 멱등키로 재시도하면 같은 발급 응답을 반환한다")
    void replayIssueWithSameIdempotencyKey() throws Exception {
        when(observationCoordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).thenReturn(issueResult());

        for (int attempt = 0; attempt < 2; attempt++) {
            mockMvc.perform(post("/api/v1/coupons/10/issue")
                            .header(RequestIdFilter.REQUEST_ID_HEADER, REQUEST_ID)
                            .header(MemberRequestHeaders.MEMBER_ID, "20")
                            .header(
                                    MemberRequestHeaders.MEMBER_GRADE,
                                    "GOLD"
                            )
                            .header(
                                    CouponRequestHeaders.IDEMPOTENCY_KEY,
                                    IDEMPOTENCY_KEY
                            ))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.data.issuanceId").value(100))
                    .andExpect(jsonPath("$.data.code")
                            .value("ABCDEFGHJKLM2345"));
        }

        verify(observationCoordinator, times(2)).issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        );
    }

    @Test
    @DisplayName("재고가 소진되면 품절 메트릭을 기록하고 409를 반환한다")
    void recordSoldOutMetric() throws Exception {
        BusinessException soldOut = new BusinessException(
                CouponIssueErrorCode.SOLD_OUT
        );
        when(observationCoordinator.issue(
                REQUEST_ID,
                10L,
                20L,
                MembershipGrade.GOLD,
                IDEMPOTENCY_KEY
        )).thenThrow(soldOut);
        when(timeProvider.instant()).thenReturn(
                Instant.parse("2026-08-24T00:00:00Z")
        );

        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, REQUEST_ID)
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                MemberRequestHeaders.MEMBER_GRADE,
                                "GOLD"
                        )
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.code").value("COUPON-306"));

        verify(couponIssueMetrics).recordBusinessFailure(
                org.mockito.ArgumentMatchers.eq(10L),
                org.mockito.ArgumentMatchers.eq(20L),
                org.mockito.ArgumentMatchers.eq(
                        CouponIssueErrorCode.SOLD_OUT
                ),
                org.mockito.ArgumentMatchers.longThat(duration -> duration >= 0)
        );
    }

    @Test
    @DisplayName("회원 헤더가 없으면 400을 반환한다")
    void rejectMissingMemberHeader() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, REQUEST_ID)
                        .header(
                                MemberRequestHeaders.MEMBER_GRADE,
                                "GOLD"
                        )
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(observationCoordinator, never())
                .issue(org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("새 등급 헤더와 기존 등급 헤더가 모두 없으면 요청을 거부한다")
    void rejectMissingMembershipGradeHeaders() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, REQUEST_ID)
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(CouponRequestHeaders.IDEMPOTENCY_KEY, IDEMPOTENCY_KEY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(observationCoordinator, never())
                .issue(org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("지원하지 않는 등급 헤더는 400을 반환한다")
    void rejectInvalidMembershipGrade() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                MemberRequestHeaders.MEMBER_GRADE,
                                "PLATINUM"
                        )
                        .header(
                                CouponRequestHeaders.IDEMPOTENCY_KEY,
                                IDEMPOTENCY_KEY
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    @Test
    @DisplayName("멱등키 헤더가 없으면 400을 반환한다")
    void rejectMissingIdempotencyKey() throws Exception {
        mockMvc.perform(post("/api/v1/coupons/10/issue")
                        .header(MemberRequestHeaders.MEMBER_ID, "20")
                        .header(
                                MemberRequestHeaders.MEMBER_GRADE,
                                "GOLD"
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verify(observationCoordinator, never())
                .issue(org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any());
    }

    private CouponIssueResult issueResult() {
        return new CouponIssueResult(
                100L,
                10L,
                "ABCDEFGHJKLM2345",
                IssuanceStatus.ISSUED,
                Instant.parse("2026-08-18T05:30:00Z"),
                Instant.parse("2026-08-25T05:30:00Z")
        );
    }
}
