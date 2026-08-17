package com.kafkick.api.admin.dashboard;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** 관리자 개요·쿠폰 지표·분석 조회의 요청 경계와 선구축 501 응답을 검증합니다. */
class AdminDashboardControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(new AdminDashboardController());

    /** 데이터 연결 전 개요 조회가 가짜 성공 대신 ADMIN-001 실패 봉투를 반환하는지 검증합니다. */
    @Test
    @DisplayName("관리자 개요 조회는 현재 ADMIN-001 선구축 오류를 반환한다")
    void overviewReturnsNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/overview"))
                .andExpect(status().isNotImplemented())
                .andExpect(header().exists("X-Request-Id"))
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.data").isEmpty())
                .andExpect(jsonPath("$.error.status").value(501))
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
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
