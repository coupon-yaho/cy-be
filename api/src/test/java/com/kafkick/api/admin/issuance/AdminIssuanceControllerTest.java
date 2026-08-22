package com.kafkick.api.admin.issuance;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** 회원 발급 문의와 발급 이력 조회의 필터·기간·cursor Validation을 검증합니다. */
class AdminIssuanceControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminIssuanceController());

    /** 발급 문의의 필수 회원 식별자를 생략하면 400으로 거부되는지 검증합니다. */
    @Test
    @DisplayName("발급 문의 조회는 memberId가 없으면 400 실패 봉투를 반환한다")
    void issuanceInquiriesRejectMissingMemberId() throws Exception {
        mockMvc.perform(get("/api/v1/admin/members/issuance-inquiries"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 유효한 문의 필터가 바인딩된 뒤 미연결 상태를 ADMIN-001로 반환하는지 검증합니다. */
    @Test
    @DisplayName("발급 문의 조회는 유효 요청에 ADMIN-001 선구축 오류를 반환한다")
    void issuanceInquiriesReturnNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/members/issuance-inquiries")
                        .param("memberId", "1")
                        .param("limit", "50"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** 발급 이력의 상태·이벤트·과거 cursor 조건이 정상 바인딩되는지 검증합니다. */
    @Test
    @DisplayName("발급 이력 조회는 eventType과 cursor를 바인딩하고 ADMIN-001 선구축 오류를 반환한다")
    void issuanceHistoriesReturnNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("couponId", "2")
                        .param("eventType", "ISSUE")
                        .param("beforeCursor", "cursor")
                        .param("limit", "200"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** HTTP 상태 필터가 표준 범위 100~599 밖이면 400으로 거부되는지 검증합니다. */
    @Test
    @DisplayName("발급 문의 조회는 HTTP 상태 코드 범위 밖 값을 400 실패 봉투로 거부한다")
    void issuanceInquiriesRejectInvalidHttpStatus() throws Exception {
        mockMvc.perform(get("/api/v1/admin/members/issuance-inquiries")
                        .param("memberId", "1")
                        .param("httpStatus", "99"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 발급 이력의 시작일이 종료일보다 늦은 기간 조건을 400으로 거부하는지 검증합니다. */
    @Test
    @DisplayName("발급 이력 조회는 역전된 기간을 400 실패 봉투로 거부한다")
    void issuanceHistoriesRejectReversedRange() throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("from", "2026-08-16")
                        .param("to", "2026-01-01"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }
}
