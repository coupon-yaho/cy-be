package com.kafkick.api.admin.batch;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.batch.BatchExecution;
import com.kafkick.core.batch.BatchExecutionRepository;

/**
 * {@code limit} 경계가 <b>실제로 발동하는지</b>를 본다.
 *
 * <p>{@code @Min}·{@code @Max} 를 파라미터에 적어 두는 것만으로는 부족하다 — 메서드 검증이
 * 켜져 있지 않으면 애너테이션은 <b>아무 일도 하지 않고</b> 그 상태에서도 컴파일과 정상 요청은
 * 전부 통과한다. 그러면 {@code limit=100000} 이 그대로 SQL 에 실려 관측 풀의
 * {@code max_execution_time} 에 잘린다.
 */
class BatchHistoryControllerTest {

    /**
     * <b>두 조회를 구분되는 값으로 돌려준다.</b> 둘 다 빈 목록을 주면
     * {@code jobName} 을 통째로 무시하는 컨트롤러도 이 테스트를 통과한다 —
     * 실제로 그 변이를 만들어 확인했고, 빈 스텁이던 시절에는 <b>안 잡혔다.</b>
     * 그 방향은 예외가 안 난다: 필터가 조용히 죽고 전체 목록이 돌아올 뿐이다.
     */
    private static final class RecordingRepository implements BatchExecutionRepository {

        @Override
        public List<BatchExecution> findRecent(int limit) {
            return List.of(execution(1L, "allJobs"));
        }

        @Override
        public List<BatchExecution> findRecentByJobName(String jobName, int limit) {
            return List.of(execution(2L, jobName));
        }

        private static BatchExecution execution(long id, String jobName) {
            return new BatchExecution(id, jobName, "COMPLETED", "COMPLETED",
                    Instant.parse("2026-08-23T00:00:00Z"), null, null);
        }
    }

    /**
     * 컨트롤러가 {@code ObjectProvider} 로 받으므로 여기서도 그 형태로 준다.
     * 관측이 꺼진 경우(빈이 없는 경우)는 실제 컨텍스트가 필요해
     * {@code BatchHistoryDisabledHttpContractTest} 가 따로 본다.
     */
    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new BatchHistoryController(provider(new RecordingRepository())));

    private static ObjectProvider<BatchExecutionRepository> provider(
            BatchExecutionRepository repository) {
        return new ObjectProvider<>() {
            @Override
            public BatchExecutionRepository getObject() {
                return repository;
            }

            @Override
            public BatchExecutionRepository getIfAvailable() {
                return repository;
            }
        };
    }

    @Test
    @DisplayName("limit 0 을 400 으로 거부한다")
    void rejectsLimitBelowMinimum() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    @Test
    @DisplayName("limit 201 을 400 으로 거부한다")
    void rejectsLimitAboveMaximum() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions").param("limit", "201"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    @Test
    @DisplayName("jobName 을 주면 잡별 조회로 간다")
    void routesToJobNameQuery() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions").param("jobName", "expireJob"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.executions[0].jobName").value("expireJob"));
    }

    @Test
    @DisplayName("jobName 이 없거나 공백이면 전체 조회로 간다")
    void routesToFullQueryWhenJobNameAbsentOrBlank() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.executions[0].jobName").value("allJobs"));

        // 공백을 잡별 조회로 넘기면 이름이 "" 인 잡을 찾아 항상 빈 목록이 된다.
        mockMvc.perform(get("/api/v1/admin/batch-executions").param("jobName", "   "))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.executions[0].jobName").value("allJobs"));
    }

    @Test
    @DisplayName("경계 안의 요청은 통과한다 — 거부가 항상 참이면 위 둘은 아무것도 검증하지 않는다")
    void acceptsLimitInRange() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions").param("limit", "200"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.source").value("BATCH_JOB_EXECUTION"));
    }
}
