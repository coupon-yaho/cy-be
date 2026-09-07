// 도메인과 무관한 잡 하나를 붙여, 범용 관제만으로 그 잡을 끝까지 다루는지 봅니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.storage.db.MySqlContainerConfig;

import tools.jackson.databind.JsonNode;

/**
 * <b>"범용 관제는 도메인과 무관하다" 를 태워서 확인한다.</b>
 *
 * <p>그 주장이 융합프로젝트가 이 배치 서버를 재사용하는 근거인데, 지금까지 받치는 것은
 * <b>정적 논거뿐</b>이었다 — {@code BatchHistoryController}·{@code BatchControlController}·
 * {@code BatchRunAbandonService} 의 import 가 {@code batch.config}·{@code core.batch}·
 * {@code core.support} 셋뿐이라는 것. <b>그것으로는 새 잡에서 돈다를 증명하지 못한다.</b>
 * 세 도메인 잡만 있는 컨텍스트에서만 돌아 봤기 때문이다 — 이번 세션이 반복해서 만난
 * 실패 모드가 정확히 그 모양이다(CY-939 관문, CY-941 비밀번호: <b>기본 구성에서만 검증</b>).
 *
 * <p><b>잡별 경로를 한 번도 안 쓴다.</b> {@code /admin/expire}·{@code /admin/cleanup}·
 * {@code /admin/verify} 는 새 도메인에 존재하지 않는다. 이 시험이 지키는 계약이 그것이다.
 *
 * <h2>실측해 둔 것</h2>
 *
 * <p><b>컨텍스트 비용 — 2026-09-07 에 잰 값이고, 이 파일이 지키는 계약은 아니다.</b>
 * 아래 중첩 {@code @TestConfiguration} 이 이 클래스의 설정 집합에 들어가
 * {@code MergedContextConfiguration} 이 형제들과 달라진다(그 중첩 클래스는
 * {@code @Import} 없이도 잡힌다 — {@code @Import} 에 적은 것은 형제
 * {@code BatchMetadataPersistenceTest} 와 모양을 맞춘 것이고 중복이다). 셋을 함께 돌렸을
 * 때 기동이 <b>두 번</b>이었다 — {@code BatchStuckApiTest} 하나 + 이 클래스 하나,
 * {@code JobCalibrationContractTest} 는 앞의 것을 재사용.
 *
 * <p><b>그 수를 재는 코드는 없다.</b> 형제의 {@code properties} 가 한 글자만 달라져도
 * 조용히 늘고 이 시험들은 그대로 통과한다. 그래서 <b>보장이 아니라 관측</b>으로 적는다 —
 * 비용이 궁금해지는 날 {@code -i} 로 {@code "Started ... in"} 을 세면 된다.
 *
 * <h2>⚠️ 이 시험이 통과해도 "잡 빈만 더하면 된다" 는 아니다</h2>
 *
 * <p>{@code JobCalibrationContractTest} 는 <b>앱 컨텍스트</b>의 {@code Job} 빈을 보므로
 * 여기서 더한 테스트 전용 잡이 안 샌다 — 함께 돌려 확인했다. 그래서 이 잡은
 * {@code RunningJobProbe} 생성자를 안 건드리고도 돈다.
 *
 * <p><b>그것은 이 시험의 면제일 뿐, 융합프로젝트의 면제가 아니다.</b> 진짜 잡을 앱에
 * 붙이면 {@code JobCalibrationContractTest} 가 <b>빌드를 깬다</b> — 그 잡의
 * {@code batch.<잡>.step-timeout-ms} 를 {@code RunningJobProbe} 생성자에 팔로 더하고
 * 임계 비교식에도 넣어야 초록이 된다.
 *
 * <p>그러니 재사용 계약을 정확히 적으면 이렇다.
 *
 * <ul>
 * <li><b>관제 표면은 잡 이름을 안 가린다</b> — 이 시험이 재는 것이 그것이다.</li>
 * <li><b>시체 임계 보정은 가린다</b> — 새 잡마다 {@code batch/config/RunningJobProbe.java}
 *     를 고쳐야 한다. 그것이 <b>일부러 만든 관문</b>이라는 것은 그 클래스에 적혀 있다:
 *     보정 안 된 잡은 살아 있는 실행이 시체로 판정될 수 있다.</li>
 * </ul>
 *
 * <p>즉 이 파일은 <i>"잡 빈만 더하면 관제가 따라온다"</i> 의 근거가 아니라
 * <i>"관제 코드를 안 고쳐도 된다"</i> 의 근거다. 그 차이를 흐리면 융합프로젝트가
 * 붙이는 날 <b>그 관문을 처음 만난다.</b>
 *
 * <p><b>프로브 잡은 동기로 돈다.</b> 이 컨텍스트에 {@code taskExecutor} 이름의 빈이 없어
 * {@code start()} 가 돌아올 때 이미 {@code COMPLETED} 다 — 그 실행이
 * {@code findRunningJobExecutions} 에 섞일 창이 없다. 아래 시체 시험이 심은 행만 본다는
 * 근거가 임계 차이 하나가 아니라 이것이다.
 *
 * <p><b>시체 판정은 이 잡에 대해 보정되지 않았다</b> — {@code RunningJobProbe} 생성자가
 * 잡 셋의 Step 데드라인만 받는다({@code JobCalibrationContractTest} 가 <b>앱 컨텍스트</b>를
 * 보므로 테스트 전용 잡에는 안 걸린다). 그래서 여기서는 <b>기본 임계(30분)보다 훨씬 오래된</b>
 * 행을 심어 그 차이가 판정에 영향을 못 주게 한다 — 이 시험이 재는 것은 임계값이 아니라
 * <b>범용 경로가 잡 이름을 안 가린다</b>는 것이다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import({ MySqlContainerConfig.class, GenericConsoleOnANewJobTest.NewDomainJob.class })
class GenericConsoleOnANewJobTest {

    /**
     * <b>도메인이 없는 잡.</b> 쿠폰도 검증도 안 건드리고 태스클릿 하나가 바로 끝난다 —
     * 융합프로젝트가 붙일 잡이 이 관제에 무엇을 요구하는지 재려면, 요구하는 쪽에
     * 아무것도 없어야 한다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class NewDomainJob {

        static final String NAME = "consoleProbeJob";

        @Bean
        Job consoleProbeJob(JobRepository repository,
                @Qualifier("transactionManager") PlatformTransactionManager tx) {
            Step step = new StepBuilder("consoleProbeStep", repository)
                    .tasklet((contribution, context) -> RepeatStatus.FINISHED, tx)
                    .build();
            return new JobBuilder(NAME, repository).start(step).build();
        }
    }

    /** 기본 임계가 30분이다. 넉넉히 넘겨 보정 여부가 판정에 안 섞이게 한다. */
    private static final Duration DEAD = Duration.ofHours(4);

    private static final String RUNS = "/api/v1/admin/batch/runs";

    @LocalServerPort
    private int port;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job consoleProbeJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    private VerifyApiProbe api;

    @AfterEach
    void tearDown() {
        // 형제 셋(BatchControlApiTest·CleanupRecoveryTest·ExpireRecoveryTest)과 같은
        // 정리다. 이 클래스는 **진짜 잡을 돌리므로** 지울 것이 실제로 생긴다 —
        // 안 지우면 그 인스턴스가 JVM 내내 남아 같은 파라미터의 두 번째 실행이
        // JobInstanceAlreadyCompleteException 이 된다(오늘은 재시도 설정이 없어 안 문다).
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
    }

    private VerifyApiProbe api() {
        if (api == null) {
            api = new VerifyApiProbe(port);
        }
        return api;
    }

    /** 배치 메타의 실제 상태. 응답 문자열이 아니라 이것이 증거다. */
    private String statusOf(long executionId) {
        return jdbcClient.sql("SELECT STATUS FROM BATCH_JOB_EXECUTION WHERE JOB_EXECUTION_ID = :id")
                .param("id", executionId)
                .query(String.class)
                .single();
    }

    /**
     * <b>범용 경로만 쓴다는 계약을 코드로 만든다.</b> 그냥 안 쓰는 것으로 두면, 이 시험이
     * 빨개진 날 다음 사람이 잡별 경로로 갈아 끼워 초록을 만든다 — 계약은 죽고 클래스는
     * 초록이다. 새 도메인에 {@code /admin/expire}·{@code /admin/cleanup} 은 없다.
     */
    private String generic(String path) {
        assertThat(path)
                .as("이 시험은 잡별 경로를 쓰면 안 된다 — 새 도메인에 그 경로는 없다")
                .startsWith("/api/v1/admin/batch/");
        return path;
    }

    /**
     * <b>진짜로 돈 실행을 뷰가 온전히 매핑하는지 본다.</b> 형제
     * {@code BatchHistoryApiTest} 는 배치 메타에 <b>손 INSERT</b> 한 행으로 필터를 재는데,
     * 그 행은 사람이 채운 값이라 <i>"프레임워크가 실제로 채우는 컬럼을 뷰가 다 읽는가"</i>
     * 를 못 잰다. 여기서는 잡을 돌려 프레임워크가 채우게 하고, <b>id 만이 아니라 상태와
     * 시각까지</b> 단언한다 — 그러지 않으면 이 시험의 증분이 없다.
     */
    @Test
    @DisplayName("실제로 돈 새 잡의 실행이 범용 이력에 온전히 나온다")
    void historyShowsTheNewJob() throws Exception {
        JobExecution done = jobOperator.start(consoleProbeJob,
                new JobParametersBuilder().addString("probe", "history").toJobParameters());
        assertThat(done.getStatus().name())
                .as("이 컨텍스트에는 taskExecutor 빈이 없어 start() 가 끝난 뒤 돌아온다")
                .isEqualTo("COMPLETED");

        HttpResponse<String> response =
                api().get(generic(RUNS + "?jobName=" + NewDomainJob.NAME));
        assertThat(response.statusCode()).isEqualTo(200);

        JsonNode mine = VerifyApiProbe.data(response).path("items").valueStream()
                .filter(run -> run.path("executionId").asLong() == done.getId())
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "이력이 잡 이름을 안 가린다면 방금 돌린 실행이 여기 있어야 한다"));

        assertThat(mine.path("jobName").asString()).isEqualTo(NewDomainJob.NAME);
        assertThat(mine.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(mine.path("startedAt").asString())
                .as("프레임워크가 채운 시각을 뷰가 못 읽으면 여기가 빈다")
                .isNotBlank();
        assertThat(mine.path("finishedAt").asString()).isNotBlank();
    }

    /**
     * <b>시체가 없어도 그룹으로 나온다</b>(CY-938 계약). 빼면 "봤는데 없었다" 와
     * "안 봤다" 가 응답에서 같아지고, 새 도메인의 운영자는 그 차이를 알 수 없다.
     */
    @Test
    @DisplayName("새 잡이 시체 조회에 그룹으로 나온다")
    void stuckListingCoversTheNewJob() throws Exception {
        HttpResponse<String> response = api().get(generic(RUNS + "/stuck"));
        assertThat(response.statusCode()).isEqualTo(200);

        assertThat(VerifyApiProbe.data(response).valueStream()
                .map(group -> group.path("jobName").asString()).toList())
                .contains(NewDomainJob.NAME);
    }

    /**
     * <b>잡별 경로 없이 시체를 닫는다.</b> 만료·정리가 쓰는 {@code recover} 는 새 도메인에
     * 없으므로, 범용 {@code stop} → {@code abandon} 두 단계가 그 자리를 대신한다.
     *
     * <p>{@code stop} 이 죽은 {@code STARTED} 에도 받아들여지는 것은 계약이다 —
     * {@code SimpleJobOperator.stop} 의 관문이 <b>{@code STARTED}·{@code STARTING} 두
     * 상태</b>다(6.0.4 바이트코드). 그래서 {@code abandon} 이 받는 {@code STOPPING} 으로
     * 넘어간다. ⚠️ 관문은 {@code isRunning()} 이 <b>아니다</b> — 그것은 {@code STOPPING}
     * 에도 참인데 {@code stop} 은 그것을 거절한다.
     */
    @Test
    @DisplayName("새 잡의 시체를 범용 stop → abandon 으로 닫는다")
    void closesAStuckRunWithoutAnyPerJobPath() throws Exception {
        LocalDateTime key = LocalDateTime.of(2026, 6, 3, 0, 0);
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, NewDomainJob.NAME, key, DEAD, DEAD)) {

            List<Long> listed = VerifyApiProbe.data(api().get(generic(RUNS + "/stuck"))).valueStream()
                    .filter(group -> NewDomainJob.NAME.equals(group.path("jobName").asString()))
                    .flatMap(group -> group.path("runs").valueStream())
                    .map(run -> run.path("executionId").asLong())
                    .toList();
            assertThat(listed)
                    .as("심은 시체가 제 잡 그룹에 나와야 그다음 조치를 할 수 있다")
                    .contains(dead.executionId());

            HttpResponse<String> stopped =
                    api().post(generic(RUNS + "/" + dead.executionId() + "/stop"));
            assertThat(stopped.statusCode()).isEqualTo(200);
            // **누른 시점의 상태**다. 그 앞뒤를 뒤집은 사고가 실제로 있었고
            // (BatchControlController 가 그 자리를 적어 뒀다) 여기서 그것을 못 박는다.
            assertThat(VerifyApiProbe.data(stopped).path("status").asString())
                    .as("심은 것은 죽은 STARTED 다 — stop 이 그것을 받아야 다음 단계가 있다")
                    .isEqualTo("STARTED");

            HttpResponse<String> abandoned =
                    api().post(generic(RUNS + "/" + dead.executionId() + "/abandon"));
            assertThat(abandoned.statusCode()).isEqualTo(200);
            // **DB 를 되읽는다.** 응답의 status 는 서버가 박는 상수(BatchRunAbandonService)라
            // abandon 을 통째로 건너뛰어도 200 + "ABANDONED" 가 나온다 — 그 돌연변이가
            // 살아남는다. 형제 BatchControlApiTest 가 거절 경로에서 같은 되읽기를 한다.
            assertThat(statusOf(dead.executionId()))
                    .as("행이 실제로 닫혀야 한다. 응답 문자열은 그 증거가 못 된다")
                    .isEqualTo("ABANDONED");
        }
    }
}
