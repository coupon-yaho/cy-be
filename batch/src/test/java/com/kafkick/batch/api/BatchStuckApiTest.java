// 전 잡 시체 조회가 배포된 잡을 빠짐없이 보고, 시체를 제 잡에만 놓는지 잽니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.storage.db.MySqlContainerConfig;

import tools.jackson.databind.JsonNode;

/**
 * <b>{@code POST /runs/{id}/abandon} 은 범용 관제에 있는데 그 {@code id} 를 찾을 조회가
 * 없었다</b>(CY-938). 시체 목록은 {@code /admin/expire}·{@code /admin/cleanup} 처럼 잡별
 * 경로에만 있어서, 잡을 새로 붙이면 조치는 되고 대상은 못 찾았다.
 *
 * <p><b>여기서 재는 것은 판정이 아니라 범위다.</b> 시체 판정 자체는
 * {@code RunningJobProbe} 것이고 {@code CleanupRecoveryTest}·{@code ExpireRecoveryTest} 가
 * 이미 잰다. 이 클래스가 답하는 질문은 <b>"어느 잡까지 봤나"</b> 하나다.
 *
 * <p><b>시체를 안 심고도 재는 시험을 하나 둔다.</b> 잡 이름 열거가 깨지는 방식이
 * <i>오탐이 아니라 누락</i>이라서다 — 빈 그룹을 응답에서 빼면 "그 잡을 봤는데 없었다" 와
 * "그 잡을 아예 안 봤다" 가 같은 모양이 되고, 그 상태로 잡이 하나 사라져도 전부 초록이다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 배치 메타를 훑는다. 손으로 심은 행과 섞이지 않게 창을 닫는다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class BatchStuckApiTest {

    /** {@code batch.stuck-job-after-ms} 기본이 30분이다. 넉넉히 넘긴다. */
    private static final Duration DEAD = Duration.ofHours(2);

    private static final String PATH = "/api/v1/admin/batch/runs/stuck";

    @LocalServerPort
    private int port;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private List<Job> jobs;

    @Autowired
    private org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping
            mappings;

    private VerifyApiProbe api;

    // 정리는 RunningJobFixture.close() 가 자기 행만 지우는 것으로 끝난다.
    // JobRepositoryTestUtils.removeJobExecutions() 를 얹지 않는다 — 그것은 getJobNames()
    // 로 배치 메타를 **통째로** 비우는데, 이 클래스가 문서로 남긴 실측이 바로 그 메서드가
    // 인터페이스 기본 구현에서 빈 목록을 돌려준다는 것이다(BatchHistoryController 참조).
    // 못 믿겠다고 적어 둔 메서드에 정리를 맡기지 않는다.

    private VerifyApiProbe api() {
        if (api == null) {
            api = new VerifyApiProbe(port);
        }
        return api;
    }

    /**
     * <b>배포된 잡 이름을 컨텍스트에서 받아 맞댄다.</b> 기대값을 리터럴로 적으면 이 테스트가
     * 잡이 늘어난 것을 못 보고, 정작 <b>이 API 가 놓치는 그 상황</b>을 재는 힘을 잃는다.
     */
    @Test
    @DisplayName("배포된 잡이 시체 유무와 무관하게 전부 그룹으로 나온다")
    void coversEveryDeployedJob() throws Exception {
        HttpResponse<String> response = api().get(PATH);
        assertThat(response.statusCode()).isEqualTo(200);

        List<String> listed = VerifyApiProbe.data(response).valueStream()
                .map(group -> group.path("jobName").asString())
                .sorted().toList();

        assertThat(listed)
                .as("빈 그룹을 빼면 '봤는데 없었다' 와 '안 봤다' 가 응답에서 같아진다")
                .containsExactlyElementsOf(jobs.stream().map(Job::getName).sorted().toList());
    }

    /**
     * <b>심어 놓고 잰다.</b> 빈 DB 에 "시체가 없다" 를 물으면 <b>판정을 통째로 건너뛰어도</b>
     * 통과한다 — {@code BatchHistoryApiTest} 가 필터를 그렇게 재는 것과 같은 이유다.
     *
     * <p>다른 그룹이 <b>비어 있는지</b>까지 본다. 잡 이름을 안 걸고 한 번만 조회해서 모든
     * 그룹에 같은 목록을 붙여도, 그 확인이 없으면 통과한다.
     */
    @Test
    @DisplayName("심은 시체는 제 잡 그룹에만 나온다")
    void placesAStuckRunUnderItsOwnJob() throws Exception {
        // 이 클래스 전용 키다 — JOB_INST_UN 이 (잡 이름, 파라미터 키)에 걸려 있어
        // 같은 컨테이너를 쓰는 형제와 겹치면 심는 쪽이 죽는다. CleanupRecoveryTest 는
        // 2026-06-01 대를, CleanupJobTest 는 2026-04 를 쓴다.
        LocalDateTime key = LocalDateTime.of(2026, 6, 2, 0, 0);
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key, DEAD, DEAD)) {

            HttpResponse<String> response = api().get(PATH);
            assertThat(response.statusCode()).isEqualTo(200);
            JsonNode data = VerifyApiProbe.data(response);

            // **여기서 세지 않으면 이 시험이 공허하게 통과한다.** 단언이 전부 루프 안에
            // 있어서, 500 이 나면 data 가 MissingNode 라 루프가 0회 돌고 **단언이 하나도
            // 안 돈 채 초록**이다. 앞 시험이 그물이 되기는 하지만, 그 의존을 안 적으면
            // 앞 시험이 바뀌는 날 이쪽이 조용히 무효가 된다.
            assertThat(data).isNotEmpty();
            assertThat(data.valueStream()
                    .map(group -> group.path("jobName").asString())
                    .toList())
                    .as("심은 잡의 그룹이 아예 빠지면 아래 루프는 contains 를 한 번도 "
                            + "안 돌고 나머지 doesNotContain 으로 통과한다")
                    .contains(CleanupJobConfig.JOB_NAME);

            for (JsonNode group : data) {
                String jobName = group.path("jobName").asString();
                List<Long> ids = group.path("runs").valueStream()
                        .map(run -> run.path("executionId").asLong()).toList();

                if (CleanupJobConfig.JOB_NAME.equals(jobName)) {
                    assertThat(ids).contains(dead.executionId());
                } else {
                    assertThat(ids)
                            .as("%s 그룹에 남의 잡 실행이 들어왔다 — 잡 이름을 안 걸고 "
                                    + "조회하면 이 모양이 된다", jobName)
                            .doesNotContain(dead.executionId());
                }
            }
        }
    }

    /**
     * <b>범용 관제 표면을 못 박는다.</b> 형제 잡별 컨트롤러는 이 자물쇠를 갖고 있는데
     * ({@code ExpireRecoveryTest#exposesExactlyTheRecoveryEndpoints} ·
     * {@code CleanupRecoveryTest}) <b>범용 쪽에는 없었다</b> — 그래서 CY-927 이 조치를
     * 올리고 조회를 안 올린 것이 아무 데도 안 걸렸다. 이 티켓이 그 짝을 맞추면서
     * 자물쇠도 같이 건다.
     *
     * <p><b>읽기와 쓰기를 한 목록에서 본다.</b> 둘이 다른 클래스에 있어도 경로는 같은
     * {@code /api/v1/admin/batch} 한 표면이고, 운영자가 보는 것도 그 표면이다 —
     * 나눠서 못 박으면 "조치는 있는데 조회가 없다" 가 다시 두 목록 사이로 숨는다.
     */
    @Test
    @DisplayName("범용 관제는 조회 둘과 조치 셋만 연다")
    void exposesExactlyTheGenericConsole() {
        Set<String> exposed = mappings.getHandlerMethods().entrySet().stream()
                .filter(entry -> entry.getValue().getBeanType() == BatchHistoryController.class
                        || entry.getValue().getBeanType() == BatchControlController.class)
                .flatMap(entry -> entry.getKey().getPathPatternsCondition().getPatternValues()
                        .stream()
                        .map(path -> entry.getKey().getMethodsCondition().getMethods() + " "
                                + path))
                .collect(Collectors.toSet());

        assertThat(exposed).containsExactlyInAnyOrder(
                "[GET] /api/v1/admin/batch/runs",
                "[GET] /api/v1/admin/batch/runs/stuck",
                "[POST] /api/v1/admin/batch/runs/{executionId}/restart",
                "[POST] /api/v1/admin/batch/runs/{executionId}/stop",
                "[POST] /api/v1/admin/batch/runs/{executionId}/abandon");
    }
}
