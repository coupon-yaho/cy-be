package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>스프링 배치의 내장 지표가 우리 레지스트리로 오는가 — 그것부터 재야 한다.</b>
 *
 * <p>배치는 지표를 {@code Metrics.globalRegistry} 에 등록하고, actuator 는 자기
 * {@code MeterRegistry} 빈을 노출한다. <b>둘이 이어져 있다는 보장은 코드 어디에도 없다</b> —
 * 이어져 있지 않으면 노출 목록에 무엇을 적든 {@code spring.batch.*} 는 안 나온다.
 *
 * <p>그래서 잡을 실제로 돌린다. 배치 지표는 <b>실행이 있어야 생기므로</b> 빈 컨텍스트에서는
 * 있는지 없는지를 가릴 수 없다.
 *
 * <h2>실측 결과 — 이미 이어져 있다</h2>
 *
 * <pre>
 * [내장 배치 지표] spring.batch.job · spring.batch.job.active
 *                  spring.batch.step · spring.batch.step.active
 * </pre>
 *
 * <p><b>그래서 이 테스트가 하는 일은 "고치는 것" 이 아니라 "붙잡는 것" 이다.</b>
 * {@code management.metrics.use-global-registry} 를 끄거나 레지스트리 배선이 바뀌면
 * 이 지표들이 <b>조용히</b> 사라진다 — 노출 목록에는 그대로 있으니 설정만 봐서는 모른다.
 *
 * <h2>커스텀 지표와 겹치지 않는다</h2>
 *
 * <p>{@code cy_*} 33종을 훑어 봤는데 내장과 겹치는 것이 <b>없다.</b>
 *
 * <ul>
 *   <li>{@code cy_batch_last_success_seconds} 는 <b>마지막 성공 시각</b>이고 내장은
 *       실행 <b>횟수·누적 시간</b>(TIMER)이다 — 후자로 전자를 못 만든다</li>
 *   <li>{@code cy_expire_*} 는 도메인 축(넘긴 행 수)이지 잡 축이 아니다</li>
 *   <li>{@code cy_coupon_round_*} 는 {@code @Scheduled} 라 애초에 배치 잡이 아니다</li>
 *   <li>{@code cy_batch_stuck_executions} 는 내장에 대응물이 없다</li>
 * </ul>
 *
 * <p>그래서 <b>줄일 것이 없다.</b> 티켓이 세운 <i>"겹치는 것을 정리한다"</i> 는 전제가
 * 실측으로 무너졌다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false"
})
@Import(MySqlContainerConfig.class)
class SpringBatchMeterBridgeTest {

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    @Qualifier("cleanupJob")
    private Job cleanupJob;

    @Autowired
    private MeterRegistry registry;

    @Test
    void batchMetersReachTheActuatorRegistry() throws Exception {
        JobExecution execution = jobOperator.start(cleanupJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", LocalDateTime.of(2026, 4, 1, 9, 0))
                .addLong("attempt", 1L)
                .toJobParameters());
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<String> names = registry.getMeters().stream()
                .map(Meter::getId)
                .map(Meter.Id::getName)
                .filter(name -> name.startsWith("spring.batch"))
                .distinct()
                .sorted()
                .toList();

        System.out.println("[내장 배치 지표] " + names);

        assertThat(names)
                .as("배치가 globalRegistry 에 등록한 지표가 actuator 레지스트리로 안 옵니다. "
                        + "노출 목록에 무엇을 적든 /actuator/prometheus 에 안 나옵니다.")
                .contains("spring.batch.job", "spring.batch.step");
    }
}
