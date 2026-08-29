package com.kafkick.api.admin.measurement;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** Benchmark 수명주기와 분리된 계측 시작·중지 명령의 HTTP 계약을 검증합니다. */
class AdminMeasurementControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(new AdminMeasurementController());

    /** 계측 시작과 중지가 하나의 상태 변경 API로 합쳐지지 않고 별도 경로인지 검증합니다. */
    @Test
    void keepsStartAndStopAsSeparateCommands() throws Exception {
        assertNotImplemented("/api/v1/admin/measurements/start");
        assertNotImplemented("/api/v1/admin/measurements/stop");
    }

    /** 계측 대상 Benchmark 실행 식별자는 반드시 양수여야 함을 검증합니다. */
    @Test
    void rejectsNonPositiveBenchmarkRunId() throws Exception {
        mockMvc.perform(post("/api/v1/admin/measurements/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"benchmarkRunId\":0}"))
                .andExpect(status().isBadRequest());
    }

    private void assertNotImplemented(String path) throws Exception {
        mockMvc.perform(post(path).contentType(MediaType.APPLICATION_JSON).content("{\"benchmarkRunId\":1}"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }
}
