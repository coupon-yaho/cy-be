package com.kafkick.api.admin.support;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.kafkick.api.admin.dashboard.AdminDashboardController;
import com.kafkick.api.admin.dashboard.AdminOverviewService;
import com.kafkick.api.admin.dashboard.calculator.CampaignOverviewCalculator;
import com.kafkick.api.admin.dashboard.calculator.OperationActionCalculator;
import com.kafkick.api.admin.dashboard.calculator.OverviewStatusCalculator;
import com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.support.TimeProvider;

/** 관리자 헤더 검사가 실제 HTTP 실패 봉투의 400·403 상태로 변환되는지 검증합니다. */
class AdminAuthorizationHttpContractTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport
            .mockMvcWithoutAdminHeaders(new AdminDashboardController(
                    new AdminOverviewService(
                            new TimeProvider(Clock.systemUTC()),
                            new AdminOverviewMockDataFactory(),
                            new CampaignOverviewCalculator(),
                            new OperationActionCalculator(),
                            new OverviewStatusCalculator())));

    /** 관리자 역할 누락 또는 정확하지 않은 대소문자 값을 ADMIN-002로 거부하는지 검증합니다. */
    @Test
    @DisplayName("관리자 API는 누락되거나 ADMIN이 아닌 역할을 403으로 거부한다")
    void rejectsMissingOrIncorrectAdminRole() throws Exception {
        mockMvc.perform(get("/api/v1/admin/overview").header("X-User-Id", "1"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("ADMIN-002"));

        mockMvc.perform(get("/api/v1/admin/overview")
                        .header("X-User-Id", "1")
                        .header("X-User-Role", "admin"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("ADMIN-002"));
    }

    /** 사용자 식별자 누락·비숫자·0 이하 값을 COMMON-001로 거부하는지 검증합니다. */
    @Test
    @DisplayName("관리자 API는 누락·문자열·0 이하 사용자 ID를 400으로 거부한다")
    void rejectsInvalidUserId() throws Exception {
        assertInvalidUserId(null);
        assertInvalidUserId("invalid");
        assertInvalidUserId("0");
        assertInvalidUserId("-1");
    }

    /** 두 헤더가 모두 없으면 Filter 통과 후 역할 interceptor가 먼저 403을 결정하는지 고정합니다. */
    @Test
    @DisplayName("두 관리자 헤더가 모두 없으면 현재 실행 순서상 역할 오류 403을 우선 반환한다")
    void missingBothHeadersPrioritizesRoleFailure() throws Exception {
        mockMvc.perform(get("/api/v1/admin/overview"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("ADMIN-002"));
    }

    private void assertInvalidUserId(String userId) throws Exception {
        MockHttpServletRequestBuilder request =
                get("/api/v1/admin/overview").header("X-User-Role", "ADMIN");
        if (userId != null) {
            request.header("X-User-Id", userId);
        }
        mockMvc.perform(request)
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }
}
