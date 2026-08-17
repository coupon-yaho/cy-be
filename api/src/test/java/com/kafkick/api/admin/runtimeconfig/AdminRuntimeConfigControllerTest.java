package com.kafkick.api.admin.runtimeconfig;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** RuntimeConfig 조회와 revision 기반 전체 PUT 계약을 검증합니다. */
class AdminRuntimeConfigControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(new AdminRuntimeConfigController());

    /** 조회와 전체 교체가 같은 리소스의 GET·PUT으로 등록되고 유효 요청은 501을 반환하는지 검증합니다. */
    @Test
    void exposesGetAndFullPutContracts() throws Exception {
        mockMvc.perform(get("/api/v1/admin/runtime-config"))
                .andExpect(status().isNotImplemented());

        mockMvc.perform(put("/api/v1/admin/runtime-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedRevision\":1,\"engineVersion\":\"V3\","
                                + "\"releaseStage\":\"V3\",\"queueMode\":\"ADAPTIVE\"}"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** PUT이 PATCH처럼 누락 필드를 허용하지 않고 revision과 모든 설정을 요구하는지 검증합니다. */
    @Test
    void fullPutRejectsMissingRevisionOrAnyConfigurationField() throws Exception {
        mockMvc.perform(put("/api/v1/admin/runtime-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"engineVersion\":\"V3\",\"releaseStage\":\"V3\",\"queueMode\":\"ADAPTIVE\"}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(put("/api/v1/admin/runtime-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedRevision\":1,\"releaseStage\":\"V3\",\"queueMode\":\"ADAPTIVE\"}"))
                .andExpect(status().isBadRequest());
    }
}
