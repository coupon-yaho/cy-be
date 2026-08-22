package com.kafkick.api.admin.dashboard;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** 관리자 개요 Service 연결과 나머지 선구축 조회의 요청 경계를 검증합니다. */
class AdminDashboardControllerTest {

    private static final Instant NOW = Instant.parse("2026-08-20T03:15:00Z");

    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminDashboardController(
                    AdminControllerContractTestSupport.overviewService(CLOCK),
                    AdminControllerContractTestSupport.couponMetricsService(CLOCK))
    );

    /** 개요 조회가 조립된 O1~O4·O3와 대표 조치를 성공 봉투에 보존하는지 검증합니다. */
    @Test
    @DisplayName("관리자 개요 조회는 Mock 계산 결과와 전체 관측값을 COMPLETE 응답으로 반환한다")
    void overviewReturnsCalculatedMockCampaignResponse() throws Exception {
        mockMvc.perform(get("/api/v1/admin/overview"))
                .andExpect(status().isOk())
                .andExpect(header().exists("X-Request-Id"))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.snapshotAt").value(NOW.toString()))
                .andExpect(jsonPath("$.data.overallStatus").value("COMPLETE"))
                .andExpect(jsonPath("$.data.actionRequired.state").value("VALID"))
                .andExpect(jsonPath("$.data.actionRequired.value.totalCount").value(4))
                .andExpect(jsonPath("$.data.openingSoon.value.totalCount").value(2))
                .andExpect(jsonPath("$.data.openingSoon.value.preparationIncompleteCount").value(1))
                .andExpect(jsonPath("$.data.campaignStatusSummary.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.openCount").value(3))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.scheduledCount").value(2))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.closedCount").value(1))
                .andExpect(jsonPath("$.data.actionItems.value.topItems[0].couponId").value(101))
                .andExpect(jsonPath("$.data.actionItems.value.topItems[0].recommendedAction.code")
                        .value("ISSUANCE_STOPPED"))
                .andExpect(jsonPath("$.data.campaigns.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value.length()").value(6))
                .andExpect(jsonPath("$.data.campaigns.value[0].couponId").value(101))
                .andExpect(jsonPath("$.data.campaigns.value[0].priority").value(1))
                .andExpect(jsonPath("$.data.campaigns.value[0].severity").value("CRITICAL"))
                .andExpect(jsonPath("$.data.campaigns.value[0].recommendedAction.code")
                        .value("ISSUANCE_STOPPED"))
                .andExpect(jsonPath("$.data.campaigns.value[0].campaignQueueStatus.value.assessment")
                        .value("ADMISSION_STOPPED"))
                .andExpect(jsonPath("$.data.campaigns.value[1].couponId").value(102))
                .andExpect(jsonPath("$.data.campaigns.value[1].issuanceFlow.value.currentPerMinute")
                        .value(44.0))
                .andExpect(jsonPath("$.data.campaigns.value[1].stockForecast.value.estimatedDepletion")
                        .value("PT7M58S"))
                .andExpect(jsonPath("$.data.queueRisk.state").value("VALID"))
                .andExpect(jsonPath("$.data.stockRisk.state").value("VALID"))
                .andExpect(jsonPath("$.data.aggregateIssuanceRate.state").value("VALID"))
                .andExpect(jsonPath("$.data.aggregateIssuanceRate.value.currentPerSecond").isNumber())
                .andExpect(jsonPath("$.data.aggregateIssuanceRate.observedAt").value(NOW.toString()))
                .andExpect(jsonPath("$.data.latencySummary.state").value("VALID"))
                .andExpect(jsonPath("$.data.latencySummary.value.successfulP99").isString())
                .andExpect(jsonPath("$.data.latencySummary.value.failedP99").isString())
                .andExpect(jsonPath("$.data.latencySummary.value.windowEnd").value(NOW.toString()))
                .andExpect(jsonPath("$.data.campaigns.value[2].couponId").value(103))
                .andExpect(jsonPath("$.data.campaigns.value[2].stockForecast.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value[2].issuanceFlow.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value[2].campaignQueueStatus.state").value("VALID"))
                .andExpect(jsonPath("$.data.customerOutcomes.state").value("VALID"))
                .andExpect(jsonPath("$.data.customerOutcomes.value.outcomes.length()").value(7))
                .andExpect(jsonPath("$.error").isEmpty());
    }

    /** 쿠폰 지표의 필수 집계 구간을 생략하면 400으로 거부되는지 검증합니다. */
    @Test
    @DisplayName("쿠폰 지표 조회는 window 없이 요청하면 400 실패 봉투를 반환한다")
    void couponMetricsRejectsMissingWindow() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupons/{couponId}/metrics", 1L))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400));
    }

    /** 허용된 집계 구간으로 상세 Mock 계산 결과를 성공 봉투에 반환하는지 검증합니다. */
    @Test
    @DisplayName("쿠폰 지표 조회는 요청 window로 계산한 상세 Mock 결과를 반환한다")
    void couponMetricsReturnsCalculatedMockResponse() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupons/{couponId}/metrics", 101L).param("window", "5m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.couponId").value(101))
                .andExpect(jsonPath("$.data.snapshotAt").value(NOW.toString()))
                .andExpect(jsonPath("$.data.window").value("FIVE_MINUTES"))
                .andExpect(jsonPath("$.data.stock.remainingCount.state").value("VALID"))
                .andExpect(jsonPath("$.data.stock.remainingCount.value").value(4_650))
                .andExpect(jsonPath("$.data.issuanceRate.value.currentPerSecond").isNumber())
                .andExpect(jsonPath("$.data.transitionRate.value.usePerSecond").isNumber())
                .andExpect(jsonPath("$.error").isEmpty());
    }

    /** Overview 모집단에 없는 couponId를 공통 404 봉투로 반환하는지 검증합니다. */
    @Test
    @DisplayName("쿠폰 지표 조회는 없는 캠페인에 COMMON-002를 반환한다")
    void couponMetricsReturns404ForUnknownCoupon() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupons/{couponId}/metrics", 999_999L)
                        .param("window", "1m"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-002"));
    }

    /** 유효한 기간·브랜드·쿠폰 필터가 분석 조회 계약에 바인딩되는지 검증합니다. */
    @Test
    @DisplayName("분석 조회는 유효한 기간과 선택 필터에서 ADMIN-001 선구축 오류를 반환한다")
    void analyticsReturnsNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/analytics")
                        .param("from", "2026-01-01")
                        .param("to", "2026-08-16")
                        .param("brandId", "2")
                        .param("couponId", "3"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** 분석 시작일이 종료일보다 늦은 요청을 400으로 거부하는지 검증합니다. */
    @Test
    @DisplayName("분석 조회는 from이 to보다 늦으면 400 실패 봉투를 반환한다")
    void analyticsRejectsReversedRange() throws Exception {
        mockMvc.perform(get("/api/v1/admin/analytics")
                        .param("from", "2026-08-16")
                        .param("to", "2026-01-01"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 분석 조회 허용 기간인 1년을 초과하면 400으로 거부하는지 검증합니다. */
    @Test
    @DisplayName("분석 조회는 1년을 초과한 기간을 400 실패 봉투로 거부한다")
    void analyticsRejectsRangeLongerThanOneYear() throws Exception {
        mockMvc.perform(get("/api/v1/admin/analytics")
                        .param("from", "2025-01-01")
                        .param("to", "2026-01-02"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }
}
