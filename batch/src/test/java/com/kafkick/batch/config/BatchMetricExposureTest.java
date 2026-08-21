// 배치 실행이 관리 포트에 메트릭으로 나오는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>"배치가 돌았다" 를 관제에 넘기는 통로가 이것이다.</b> 우리가 만든 것이 아니라
 * Spring Batch 가 Micrometer 로 자동 등록한다 — 그래서 <b>지워지는 것도 우리 모르게</b>
 * 일어날 수 있다. 의존성 버전이 오르거나 노출 목록이 좁아지면 관제 쪽에서 먼저 비는데,
 * 그때는 이미 며칠 지난 뒤다.
 *
 * <p><b>두 축을 본다.</b>
 *
 * <table border="1">
 *   <caption>관제가 읽는 메트릭</caption>
 *   <tr><td>{@code spring_batch_job_seconds_count}</td>
 *       <td>잡이 몇 번 끝났나. {@code spring_batch_job_status} 로 성패가 갈린다</td></tr>
 *   <tr><td>{@code spring_batch_job_active_seconds_max}</td>
 *       <td>지금 도는 중인가. 주기보다 오래 걸리는지 여기서 보인다</td></tr>
 * </table>
 *
 * <p><b>스텝 단위 메트릭({@code spring_batch_step_seconds_count})은 여기서 안 본다.</b>
 * 규칙 파일이 그것을 안 쓰기 때문이고, 안 쓰는 이름을 단언에 넣으면 <b>규칙과 무관한 것을
 * 지키는</b> 테스트가 된다. 규칙이 그 축을 쓰기 시작하면 아래 대조가 자동으로 딸려 온다.
 *
 * <p><b>이름이 계약이다.</b> 관제 대시보드와 알림 규칙이 이 문자열을 그대로 쓴다.
 * 태그 이름까지 함께 보는 이유가 그것이다 — {@code spring_batch_job_name} 이 빠지면
 * 잡별로 가를 수 없어 규칙이 통째로 무의미해진다.
 *
 * <p><b>실제 설정 파일을 읽는다.</b> 노출 목록을 이 클래스에 복붙하면
 * {@code application.yml} 에서 {@code prometheus} 가 빠져도 여기는 초록으로 지나간다 —
 * 정작 막아야 할 사고가 그것이다. 같은 이유로 {@code ActuatorExposureTest} 도 파일을 읽는다.
 *
 * <p><b>알림 규칙 파일을 직접 읽는다.</b> {@code infra/prometheus/rules/batch-alerts.yml} 의 규칙이 쓰는
 * 메트릭 이름을 뽑아 실제 노출과 대조한다. 이름 목록을 이 클래스에 복붙하면 규칙이 새 메트릭을
 * 쓰기 시작해도 여기는 초록으로 지나가고, <b>관제에서 규칙 하나가 영원히 안 뜨는 상태</b>가 된다 —
 * 알림은 안 울리는 것이 정상처럼 보여서 아무도 눈치채지 못한다.
 *
 * <p><b>이 저장소에서 같은 형태의 거짓말을 세 번 했다.</b> <i>"어긋나면 저기서 잡힌다"</i> 고
 * 적었는데 실제로는 두 쪽을 잇는 것이 아무것도 없던 경우다. 그래서 문장으로 잇지 않고
 * 파일을 읽어서 잇는다.
 *
 * <p><b>여기서 안 보이는 것도 적어 둔다.</b> 처리 건수({@code writeCount})는 메트릭에 없다.
 * 그래서 <i>"스케줄러는 도는데 아무것도 안 줄어든다"</i> 를 이 통로만으로는 못 잡는다.
 * 그 지표를 붙이는 것은 별도 티켓이다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 실제 설정 파일을 읽는다. 노출 목록을 여기 복붙하면 파일에서 prometheus 가 빠져도
        // 이 테스트는 초록으로 지나간다 — 관제가 비는 것을 못 잡는다는 뜻이다.
        //
        // 테스트 설정(classpath:/application.yml)을 뒤에 둬서 스키마를 만드는 부분만 덮는다.
        // 실제 파일은 flyway.enabled:false 다(마이그레이션 소유자가 api 라서). 그것만 읽으면
        // issuances 테이블이 없어 잡이 bad SQL grammar 로 죽고, 정작 재려던 메트릭은
        // FAILED 로 찍힌다. 노출 목록은 테스트 설정에 없으므로 실제 파일 것이 그대로 산다.
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 포트만 덮는다. 파일의 9090·9092 는 이미 뜬 컨테이너와 부딪힌다.
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class BatchMetricExposureTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /**
     * 관제가 실제로 읽는 규칙 파일. <b>클래스패스가 아니라 배포 산출물이다</b> —
     * {@code src/main/resources} 에 두면 jar 안에만 들어가고 Prometheus 는 jar 를 안 읽는다.
     *
     * <p>gradle 은 모듈 디렉터리에서 테스트를 돌리므로 저장소 루트가 한 단계 위다.
     */
    private static final Path ALERT_RULES =
            Path.of("..", "infra", "prometheus", "rules", "batch-alerts.yml").normalize();

    /** 규칙 파일에 쓰인 메트릭 이름. 뒤따르는 {@code {태그}} 는 빼고 이름만 잡는다. */
    private static final Pattern METRIC_NAME = Pattern.compile("spring_batch_[a-z_]+");

    @LocalManagementPort
    private int managementPort;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JobRepository jobRepository;

    /**
     * <b>배선 전에는 이 정리가 필요 없었다.</b> {@code JobRepository} 가
     * {@code ResourcelessJobRepository} 라 아무것도 안 남았기 때문이다. 이제 인스턴스가 실제로
     * 남으므로, 같은 {@code asOf} 로 도는 두 번째 테스트를 넣는 순간
     * {@code JobInstanceAlreadyCompleteException} 이 난다 — 실패 메시지가 메트릭이 아니라
     * 배치 메타 얘기를 해서 원인이 안 보인다. 잡을 띄우는 다른 테스트 열다섯이 이미 이렇게 한다.
     */
    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
    }

    @Test
    @DisplayName("잡이 한 번 돌면 실행 횟수·상태·잡 이름이 메트릭으로 나온다")
    void exposeJobRunAsMetric() throws Exception {
        JobExecution execution = jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
        assertThat(execution.getStatus())
                .as("메트릭에 COMPLETED 가 찍히려면 잡이 먼저 성공해야 한다: %s",
                        execution.getAllFailureExceptions())
                .isEqualTo(BatchStatus.COMPLETED);

        String body = ActuatorProbe.get(managementPort, "/actuator/prometheus").body();

        Set<String> used = metricsUsedByAlertRules();
        assertThat(used)
                .as("규칙 파일에서 메트릭 이름을 하나도 못 뽑았다 — 경로나 정규식부터 확인해라")
                .isNotEmpty();
        assertThat(body)
                .as("알림 규칙이 쓰는 이름 %s 이 실제로 나와야 한다. "
                        + "안 나오면 그 규칙은 관제에서 영원히 안 뜬다", used)
                .contains(used);

        assertThat(metricLine(body, "spring_batch_job_seconds_count"))
                .as("잡 이름과 상태가 태그로 갈려야 알림 규칙을 잡별로 쓸 수 있다")
                .contains("spring_batch_job_name=\"expireJob\"")
                .contains("spring_batch_job_status=\"COMPLETED\"");
    }

    /**
     * 알림 규칙 파일에서 {@code spring_batch_*} 이름을 전부 뽑는다.
     *
     * <p>태그가 붙은 형태({@code ..._count{job="x"}})로 쓰이므로 이름 부분만 취한다.
     * 규칙에 새 메트릭이 등장하면 여기 자동으로 딸려 와, 그 이름도 함께 검사받는다.
     */
    private Set<String> metricsUsedByAlertRules() throws IOException {
        Path rulesFile = ALERT_RULES;
        assertThat(rulesFile)
                .as("규칙 파일이 %s 에 없다. 옮겼다면 이 경로도 함께 고쳐야 한다", rulesFile)
                .exists();
        {
            String rules = Files.readString(rulesFile, StandardCharsets.UTF_8);
            Set<String> names = new TreeSet<>();
            Matcher matcher = METRIC_NAME.matcher(rules);
            while (matcher.find()) {
                names.add(matcher.group());
            }
            return names;
        }
    }

    /** 이름이 같은 줄 중 첫 데이터 줄. {@code # HELP}/{@code # TYPE} 은 건너뛴다. */
    private String metricLine(String body, String metric) {
        return body.lines()
                .filter(line -> line.startsWith(metric))
                .findFirst()
                .orElseThrow(() -> new AssertionError(metric + " 데이터 줄이 없다"));
    }
}
