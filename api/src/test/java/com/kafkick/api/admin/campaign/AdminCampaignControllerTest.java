package com.kafkick.api.admin.campaign;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** 일반 CUD를 제외한 캠페인·브랜드·템플릿 조회와 상태 전환 명령 계약을 검증합니다. */
class AdminCampaignControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(new AdminCampaignController());

    /** 세 카탈로그 조회가 각각 독립 경로로 등록되고 미연결 상태를 501로 표현하는지 검증합니다. */
    @Test
    void exposesThreeReadContracts() throws Exception {
        assertNotImplemented(get("/api/v1/admin/campaigns"));
        assertNotImplemented(get("/api/v1/admin/brands"));
        assertNotImplemented(get("/api/v1/admin/templates"));
    }

    /** 캠페인 상태 전환이 일반 수정이 아닌 전용 운영 명령 경로로 유지되는지 검증합니다. */
    @Test
    void exposesStatusTransitionAsDedicatedCommand() throws Exception {
        assertNotImplemented(post("/api/v1/admin/campaigns/1/status-transitions")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"targetStatus\":\"CLOSED\",\"reason\":\"운영자 강제 마감\"}"));
    }

    /** 목표 상태는 확정 enum이어야 하고 감사 사유는 비어 있을 수 없음을 검증합니다. */
    @Test
    void statusTransitionRequiresKnownTargetAndAuditReason() throws Exception {
        mockMvc.perform(post("/api/v1/admin/campaigns/1/status-transitions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"targetStatus\":\"UNKNOWN\",\"reason\":\"\"}"))
                .andExpect(status().isBadRequest());
    }

    private void assertNotImplemented(org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request)
            throws Exception {
        mockMvc.perform(request)
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }
}
