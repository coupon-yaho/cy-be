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

import com.kafkick.api.admin.dashboard.calculator.CampaignOverviewCalculator;
import com.kafkick.api.admin.dashboard.calculator.OperationActionCalculator;
import com.kafkick.api.admin.dashboard.calculator.OverviewStatusCalculator;
import com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataFactory;
import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.support.TimeProvider;

/** 관리자 개요 Service 연결과 나머지 선구축 조회의 요청 경계를 검증합니다. */
class AdminDashboardControllerTest {

    private static final Instant NOW = Instant.parse("2026-08-20T03:15:00Z");

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminDashboardController(new AdminOverviewService(
                    new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)),
                    new AdminOverviewMockDataFactory(),
                    new CampaignOverviewCalculator(),
                    new OperationActionCalculator(),
                    new OverviewStatusCalculator()))
    );

    /** 개요 조회가 계산된 캠페인 값과 미연결 관측 상태를 성공 봉투에 보존하는지 검증합니다. */
    @Test
    @DisplayName("관리자 개요 조회는 Mock 캠페인 계산 결과를 PARTIAL 응답으로 반환한다")
    void overviewReturnsCalculatedMockCampaignResponse() throws Exception {
        mockMvc.perform(get("/api/v1/admin/overview"))
                .andExpect(status().isOk())
                .andExpect(header().exists("X-Request-Id"))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.snapshotAt").value(NOW.toString()))
                .andExpect(jsonPath("$.data.overallStatus").value("PARTIAL"))
                .andExpect(jsonPath("$.data.actionRequired.state").value("VALID"))
                .andExpect(jsonPath("$.data.actionRequired.value.totalCount").value(1))
                .andExpect(jsonPath("$.data.openingSoon.value.totalCount").value(2))
                .andExpect(jsonPath("$.data.openingSoon.value.preparationIncompleteCount").value(1))
                .andExpect(jsonPath("$.data.campaignStatusSummary.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.openCount").value(1))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.scheduledCount").value(2))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.closedCount").value(1))
                .andExpect(jsonPath("$.data.actionItems.value.topItems[0].couponId").value(103))
                .andExpect(jsonPath("$.data.campaigns.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value.length()").value(4))
                .andExpect(jsonPath("$.data.campaigns.value[0].couponId").value(103))
                .andExpect(jsonPath("$.data.campaigns.value[0].priority").value(1))
                .andExpect(jsonPath("$.data.campaigns.value[0].severity").value("WARN"))
                .andExpect(jsonPath("$.data.campaigns.value[0].stockForecast.state")
                        .value("UNAVAILABLE"))
                .andExpect(jsonPath("$.data.campaigns.value[1].couponId").value(101))
                .andExpect(jsonPath("$.data.campaigns.value[1].stockForecast.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value[1].stockForecast.value.remainingQuantity")
                        .value(300))
                .andExpect(jsonPath("$.data.customerOutcomes.state").value("UNAVAILABLE"))
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

    /** 허용된 집계 구간은 정상 바인딩된 뒤 미연결 상태인 501로 도달하는지 검증합니다. */
    @Test
    @DisplayName("쿠폰 지표 조회는 허용 window 요청에 ADMIN-001 선구축 오류를 반환한다")
    void couponMetricsReturnsNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupons/{couponId}/metrics", 1L).param("window", "5m"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
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
