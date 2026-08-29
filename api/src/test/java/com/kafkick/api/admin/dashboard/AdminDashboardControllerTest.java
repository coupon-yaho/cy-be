package com.kafkick.api.admin.dashboard;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.api.admin.observability.PendingAdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;

/** 관리자 개요 Service 연결과 나머지 선구축 조회의 요청 경계를 검증합니다. */
class AdminDashboardControllerTest {

    private static final Instant NOW = Instant.parse("2026-08-20T03:15:00Z");

    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvcWithNonNullJson(
            new AdminDashboardController(
                    AdminControllerContractTestSupport.overviewService(CLOCK),
                    AdminControllerContractTestSupport.couponMetricsService(CLOCK),
                    AdminControllerContractTestSupport.analyticsService(CLOCK))
    );

    /** 개요 조회가 DB 캠페인과 연결된 관측, 미연결 PENDING을 같은 성공 봉투에 보존하는지 검증합니다. */
    @Test
    @DisplayName("관리자 개요 조회는 관측값과 aggregate PENDING을 PARTIAL 응답으로 반환한다")
    void overviewReturnsObservedAndPendingBoundaryResponse() throws Exception {
        mockMvc.perform(get("/api/v1/admin/overview"))
                .andExpect(status().isOk())
                .andExpect(header().exists("X-Request-Id"))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.snapshotAt").value(NOW.toString()))
                .andExpect(jsonPath("$.data.overallStatus").value("PARTIAL"))
                .andExpect(jsonPath("$.data.actionRequired.state").value("PENDING"))
                .andExpect(jsonPath("$.data.actionRequired.value").doesNotExist())
                .andExpect(jsonPath("$.data.openingSoon.state").value("PENDING"))
                .andExpect(jsonPath("$.data.openingSoon.value").doesNotExist())
                .andExpect(jsonPath("$.data.campaignStatusSummary.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.openCount").value(3))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.scheduledCount").value(2))
                .andExpect(jsonPath("$.data.campaignStatusSummary.value.closedCount").value(1))
                .andExpect(jsonPath("$.data.actionItems.state").value("PENDING"))
                .andExpect(jsonPath("$.data.actionItems.value").doesNotExist())
                .andExpect(jsonPath("$.data.campaigns.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value.length()").value(6))
                .andExpect(jsonPath("$.data.campaigns.value[0].couponId").value(101))
                .andExpect(jsonPath("$.data.campaigns.value[0].priority").value(1))
                .andExpect(jsonPath("$.data.campaigns.value[0].campaignQueueStatus.state")
                        .value("PENDING"))
                .andExpect(jsonPath("$.data.campaigns.value[1].couponId").value(102))
                .andExpect(jsonPath("$.data.campaigns.value[1].issuanceFlow.value.currentPerMinute")
                        .value(49.0))
                .andExpect(jsonPath("$.data.campaigns.value[1].stockForecast.value.estimatedDepletion")
                        .value("PT7M9S"))
                .andExpect(jsonPath("$.data.queueRisk.state").value("PENDING"))
                .andExpect(jsonPath("$.data.stockRisk.state").value("VALID"))
                .andExpect(jsonPath("$.data.aggregateIssuanceRate.state").value("PENDING"))
                .andExpect(jsonPath("$.data.aggregateIssuanceRate.value").doesNotExist())
                .andExpect(jsonPath("$.data.aggregateIssuanceRate.observedAt").doesNotExist())
                .andExpect(jsonPath("$.data.latencySummary.state").value("VALID"))
                .andExpect(jsonPath("$.data.latencySummary.value.successfulP99").isString())
                .andExpect(jsonPath("$.data.latencySummary.value.failedP99").doesNotExist())
                .andExpect(content().string(not(containsString("\"failedP99\""))))
                .andExpect(jsonPath("$.data.latencySummary.value.windowEnd").value(NOW.toString()))
                .andExpect(jsonPath("$.data.campaigns.value[2].couponId").value(103))
                .andExpect(jsonPath("$.data.campaigns.value[2].stockForecast.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value[2].issuanceFlow.state").value("VALID"))
                .andExpect(jsonPath("$.data.campaigns.value[2].campaignQueueStatus.state").value("PENDING"))
                .andExpect(jsonPath("$.data.customerOutcomes.state").value("VALID"))
                .andExpect(jsonPath("$.data.customerOutcomes.value.outcomes.length()").value(7))
                .andExpect(jsonPath("$.error").doesNotExist());
    }

    /** 쿠폰 지표의 필수 집계 구간을 생략하면 400으로 거부되는지 검증합니다. */
    @Test
    @DisplayName("쿠폰 지표 조회는 window 없이 요청하면 400 실패 봉투를 반환한다")
    void couponMetricsRejectsMissingWindow() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-metrics").param("couponId", "1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400));
    }

    /** couponId 가 path variable 에서 쿼리 파라미터로 옮겨진 뒤에도 필수인지 검증합니다. */
    @Test
    @DisplayName("쿠폰 지표 조회는 couponId 없이 요청하면 400 실패 봉투를 반환한다")
    void couponMetricsRejectsMissingCouponId() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-metrics").param("window", "5m"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400));
    }

    /**
     * 쿼리 파라미터로 옮긴 couponId 에도 {@code @Positive} 가 계속 걸리는지 검증합니다.
     *
     * <p>path variable 일 때와 달리 쿼리 파라미터는 값이 없어도 경로가 일치하므로, 양수 검증이
     * 빠지면 0·음수가 Service 까지 내려가 404 로 둔갑합니다. 400 과 404 를 함께 고정합니다.</p>
     */
    @ParameterizedTest
    @ValueSource(strings = {"0", "-1"})
    @DisplayName("쿠폰 지표 조회는 양수가 아닌 couponId를 400 실패 봉투로 거부한다")
    void couponMetricsRejectsNonPositiveCouponId(String couponId) throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-metrics")
                        .param("couponId", couponId)
                        .param("window", "5m"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    /** 허용된 모든 집계 구간에서 DB 상세값과 미연결 PENDING 응답 형식을 보존하는지 검증합니다. */
    @ParameterizedTest
    @CsvSource({"1m, ONE_MINUTE, 0.75", "5m, FIVE_MINUTES, 0.65", "15m, FIFTEEN_MINUTES, 0.4"})
    @DisplayName("쿠폰 지표 조회는 DB 상세값과 미연결 PENDING을 반환한다")
    void couponMetricsReturnsDatabaseAndPendingResponse(
            String window,
            String expectedWindow,
            double expectedUsePerSecond
    ) throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-metrics")
                        .param("couponId", "101")
                        .param("window", window))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.couponId").value(101))
                .andExpect(jsonPath("$.data.snapshotAt").value(NOW.toString()))
                .andExpect(jsonPath("$.data.window").value(expectedWindow))
                .andExpect(jsonPath("$.data.stock.remainingCount.state").value("VALID"))
                .andExpect(jsonPath("$.data.stock.remainingCount.value").value(4_650))
                .andExpect(jsonPath("$.data.issuanceRate.state").value("PENDING"))
                .andExpect(jsonPath("$.data.issuanceRate.value").doesNotExist())
                .andExpect(jsonPath("$.data.queue.waitingCount.state").value("PENDING"))
                .andExpect(jsonPath("$.data.transitionRate.value.usePerSecond").value(expectedUsePerSecond))
                .andExpect(jsonPath("$.error").doesNotExist());
    }

    /** Overview 모집단에 없는 couponId를 공통 404 봉투로 반환하는지 검증합니다. */
    @Test
    @DisplayName("쿠폰 지표 조회는 없는 캠페인에 COMMON-002를 반환한다")
    void couponMetricsReturns404ForUnknownCoupon() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-metrics")
                        .param("couponId", "999999")
                        .param("window", "1m"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-002"));
    }

    @Test
    @DisplayName("쿠폰 지표 DB 장애는 ADMIN-CAMPAIGN-001 503을 반환한다")
    void couponMetricsReturns503ForDatabaseFailure() throws Exception {
        MockMvc unavailableMvc = mockMvcWithReader(new UnavailableDetailReader());

        unavailableMvc.perform(get("/api/v1/admin/coupon-metrics")
                        .param("couponId", "101")
                        .param("window", "1m"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error.code").value("ADMIN-CAMPAIGN-001"));
    }

    @Test
    @DisplayName("관측 비활성 쿠폰 지표는 ADMIN-003 503을 반환한다")
    void couponMetricsReturns503WhenObservationIsDisabled() throws Exception {
        MockMvc disabledMvc = mockMvcWithReader(new PendingAdminCampaignDataReader());

        disabledMvc.perform(get("/api/v1/admin/coupon-metrics")
                        .param("couponId", "101")
                        .param("window", "1m"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error.code").value("ADMIN-003"));
    }

    @Test
    @DisplayName("관측 비활성 운영현황은 빈 PENDING 모집단을 200으로 반환한다")
    void overviewReturnsPendingWhenObservationIsDisabled() throws Exception {
        AdminCampaignDataReader reader = new PendingAdminCampaignDataReader();
        MockMvc disabledMvc = AdminControllerContractTestSupport.mockMvcWithNonNullJson(
                new AdminDashboardController(
                        AdminControllerContractTestSupport.overviewService(CLOCK, reader),
                        AdminControllerContractTestSupport.couponMetricsService(CLOCK, reader),
                        AdminControllerContractTestSupport.analyticsService(CLOCK)));

        disabledMvc.perform(get("/api/v1/admin/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.campaigns.state").value("PENDING"))
                .andExpect(jsonPath("$.data.campaigns.value").doesNotExist())
                .andExpect(jsonPath("$.data.campaignStatusSummary.state").value("PENDING"))
                .andExpect(jsonPath("$.data.openingSoon.state").value("PENDING"));
    }

    private static MockMvc mockMvcWithReader(AdminCampaignDataReader reader) {
        return AdminControllerContractTestSupport.mockMvcWithNonNullJson(
                new AdminDashboardController(
                        AdminControllerContractTestSupport.overviewService(CLOCK),
                        AdminControllerContractTestSupport.couponMetricsService(CLOCK, reader),
                        AdminControllerContractTestSupport.analyticsService(CLOCK)));
    }

    private static final class UnavailableDetailReader implements AdminCampaignDataReader {
        @Override
        public AdminCampaignCatalog loadCatalog(Instant snapshotAt) {
            throw new AssertionError("상세 HTTP 테스트에서 catalog를 읽으면 안 됩니다.");
        }

        @Override
        public AdminCampaignDetailData findDetail(
                long couponId,
                Instant fromInclusive,
                Instant toExclusive,
                Instant snapshotAt
        ) {
            return new AdminCampaignDetailData(DetailAvailability.UNAVAILABLE, null);
        }
    }

    /** 집계 Source가 없을 때 필터와 관계없이 가짜 값 대신 PENDING을 반환하는지 검증합니다. */
    @Test
    @DisplayName("분석 조회는 집계 Source 연결 전 PENDING 응답을 반환한다")
    void analyticsReturnsPendingResponseUntilAggregateSourceIsWired() throws Exception {
        mockMvc.perform(get("/api/v1/admin/analytics")
                        .param("from", "2026-01-01")
                        .param("to", "2026-03-31")
                        .param("brandId", "1")
                        .param("couponId", "101"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.sourceType").value("NONE"))
                .andExpect(jsonPath("$.data.range.from").value("2026-01-01"))
                .andExpect(jsonPath("$.data.filters.brandId").value(1))
                .andExpect(jsonPath("$.data.filters.couponId").value(101))
                .andExpect(jsonPath("$.data.brands.length()").value(0))
                .andExpect(jsonPath("$.data.brandTrends.state").value("PENDING"))
                .andExpect(jsonPath("$.data.brandTrends.value").doesNotExist())
                .andExpect(jsonPath("$.data.hourlyHeatmap.state").value("PENDING"))
                .andExpect(jsonPath("$.data.hourlyHeatmap.value").doesNotExist())
                .andExpect(jsonPath("$.data.issuanceStatusDistribution.state").value("PENDING"))
                .andExpect(jsonPath("$.data.issuanceStatusDistribution.value").doesNotExist())
                .andExpect(jsonPath("$.error").doesNotExist());
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
