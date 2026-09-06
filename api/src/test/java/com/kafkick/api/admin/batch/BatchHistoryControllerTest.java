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
import com.kafkick.core.batch.BatchJobParameter;
import com.kafkick.core.batch.BatchStepExecution;
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

        /**
         * <b>요청받은 실행 id 를 그대로 되돌려 준다.</b> 고정값을 주면 컨트롤러가 경로
         * 변수를 통째로 무시해도 통과한다 — 위 두 조회를 구분되는 값으로 만든 것과 같은
         * 이유다.
         */
        @Override
        public List<BatchStepExecution> findSteps(long jobExecutionId) {
            return List.of(new BatchStepExecution(11L, jobExecutionId, "step-one",
                    "COMPLETED", "COMPLETED", "원인이 기록되지 않았습니다",
                    Instant.parse("2026-08-23T00:00:00Z"), null, null,
                    7, 3, 1, 2, 4, 1, 2, 3));
        }

        @Override
        public List<BatchJobParameter> findParameters(long jobExecutionId) {
            return List.of(
                    new BatchJobParameter("asOf", "java.time.LocalDateTime",
                            "2026-08-22T00:00", true),
                    new BatchJobParameter("attempt", "java.lang.Long", "2", false));
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

    /**
     * <b>경로 변수가 실제로 어댑터까지 간다.</b> 스텁이 받은 id 를 되돌려 주므로,
     * 컨트롤러가 그것을 무시하면 여기서 값이 안 맞는다.
     */
    @Test
    @DisplayName("스텝 조회가 경로의 실행 id 를 그대로 넘긴다")
    void passesTheExecutionIdThrough() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions/4242/steps"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.jobExecutionId").value(4242))
                .andExpect(jsonPath("$.data.steps[0].stepName").value("step-one"));
    }

    /**
     * <b>카운터 여덟 개가 응답에서도 자리를 안 바꾼다.</b> 전부 숫자라 뒤바뀌어도 예외가
     * 안 나고 <b>그럴듯한 값</b>이 나간다 — 그래서 여덟을 전부 다른 값으로 둔다.
     */
    @Test
    @DisplayName("응답의 카운터 8종이 자리를 바꾸지 않는다")
    void serializesAllEightCountersToTheirOwnFields() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions/1/steps"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.steps[0].readCount").value(7))
                .andExpect(jsonPath("$.data.steps[0].writeCount").value(3))
                .andExpect(jsonPath("$.data.steps[0].filterCount").value(1))
                .andExpect(jsonPath("$.data.steps[0].commitCount").value(2))
                .andExpect(jsonPath("$.data.steps[0].rollbackCount").value(4))
                .andExpect(jsonPath("$.data.steps[0].readSkipCount").value(1))
                .andExpect(jsonPath("$.data.steps[0].processSkipCount").value(2))
                .andExpect(jsonPath("$.data.steps[0].writeSkipCount").value(3));
    }

    /**
     * <b>{@code identifying} 이 JSON 에서도 boolean 이다.</b> 문자열로 나가면 화면이
     * {@code "Y"} 와 {@code "true"} 중 무엇을 볼지 몰라 <b>둘 다 대응하는 코드</b>가 생기고,
     * 그 코드는 한쪽이 바뀌는 날 조용히 틀린다.
     */
    @Test
    @DisplayName("파라미터 응답의 identifying 이 boolean 이다")
    void serializesIdentifyingAsBoolean() throws Exception {
        mockMvc.perform(get("/api/v1/admin/batch-executions/7/parameters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.jobExecutionId").value(7))
                .andExpect(jsonPath("$.data.parameters[0].name").value("asOf"))
                .andExpect(jsonPath("$.data.parameters[0].identifying").value(true))
                .andExpect(jsonPath("$.data.parameters[1].identifying").value(false));
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
