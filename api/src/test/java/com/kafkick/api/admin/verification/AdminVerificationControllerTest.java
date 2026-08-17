package com.kafkick.api.admin.verification;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** 전수 검증 실행 명령과 실행 목록·상세 조회의 분리된 계약을 검증합니다. */
class AdminVerificationControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(new AdminVerificationController());

    /** 검증 명령, 과거 방향 실행 목록, 실행 상세가 각각 독립 endpoint인지 검증합니다. */
    @Test
    void exposesVerificationCommandAndReadContracts() throws Exception {
        assertNotImplemented(post("/api/v1/admin/verify")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"asOf\":\"2026-08-16T00:00:00Z\",\"scope\":\"FULL\",\"dataset\":\"CLEAN\"}"));
        assertNotImplemented(get("/api/v1/admin/verification-runs"));
        assertNotImplemented(get("/api/v1/admin/verification-runs/1"));
    }

    /** 재현 가능한 검증을 위해 기준 시각·범위·데이터셋이 모두 필수인지 검증합니다. */
    @Test
    void verificationRequiresAsOfScopeAndDataset() throws Exception {
        mockMvc.perform(post("/api/v1/admin/verify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"scope\":\"FULL\",\"dataset\":\"CLEAN\"}"))
                .andExpect(status().isBadRequest());
    }

    private void assertNotImplemented(org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request)
            throws Exception {
        mockMvc.perform(request)
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }
}
