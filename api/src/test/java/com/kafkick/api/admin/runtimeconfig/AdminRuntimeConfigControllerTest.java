package com.kafkick.api.admin.runtimeconfig;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.ReadOnlyRuntimeConfigStore;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;

/** RuntimeConfig 조회와 revision 기반 전체 PUT 계약을 검증합니다. */
class AdminRuntimeConfigControllerTest {

    /** 조회와 전체 교체가 같은 리소스의 GET·PUT으로 등록되는지 검증합니다. */
    @Test
    void exposesGetAndFullPutContracts() throws Exception {
        MockMvc mockMvc = mockMvc(snapshot(SourceStatus.VALID));

        mockMvc.perform(get("/api/v1/admin/runtime-config"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.revision").value(3))
                .andExpect(jsonPath("$.data.engineVersion").value("V3"))
                .andExpect(jsonPath("$.data.releaseStage").value("V2_1"))
                .andExpect(jsonPath("$.data.queueMode").value("ADAPTIVE"))
                .andExpect(jsonPath("$.data.updatedAt").value("2026-08-26T00:00:00Z"))
                .andExpect(jsonPath("$.data.updatedBy").value("812934"))
                .andExpect(jsonPath("$.data.sourceStatus").value("VALID"));

        mockMvc.perform(put("/api/v1/admin/runtime-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedRevision\":1,\"engineVersion\":\"V3\","
                                + "\"releaseStage\":\"V3\",\"queueMode\":\"ADAPTIVE\"}"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** Store가 보유한 마지막 유효 설정의 STALE 상태를 HTTP 응답에도 보존하는지 검증합니다. */
    @Test
    void getPreservesStaleSnapshotStatus() throws Exception {
        MockMvc mockMvc = mockMvc(snapshot(SourceStatus.STALE));

        mockMvc.perform(get("/api/v1/admin/runtime-config"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.revision").value(3))
                .andExpect(jsonPath("$.data.sourceStatus").value("STALE"));
    }

    /** PUT이 PATCH처럼 누락 필드를 허용하지 않고 revision과 모든 설정을 요구하는지 검증합니다. */
    @Test
    void fullPutRejectsMissingRevisionOrAnyConfigurationField() throws Exception {
        MockMvc mockMvc = mockMvc(snapshot(SourceStatus.VALID));

        mockMvc.perform(put("/api/v1/admin/runtime-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"engineVersion\":\"V3\",\"releaseStage\":\"V3\",\"queueMode\":\"ADAPTIVE\"}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(put("/api/v1/admin/runtime-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedRevision\":1,\"releaseStage\":\"V3\",\"queueMode\":\"ADAPTIVE\"}"))
                .andExpect(status().isBadRequest());
    }

    private static MockMvc mockMvc(RuntimeConfigSnapshot snapshot) {
        return AdminControllerContractTestSupport.mockMvc(
                new AdminRuntimeConfigController(new ReadOnlyRuntimeConfigStore(snapshot)));
    }

    private static RuntimeConfigSnapshot snapshot(SourceStatus sourceStatus) {
        return new RuntimeConfigSnapshot(
                EngineVersion.V3,
                ReleaseStage.V2_1,
                QueueMode.ADAPTIVE,
                3L,
                Instant.parse("2026-08-26T00:00:00Z"),
                "812934",
                sourceStatus);
    }
}
