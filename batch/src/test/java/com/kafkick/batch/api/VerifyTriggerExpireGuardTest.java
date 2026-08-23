// 만료 스케줄러가 도는 동안에는 검증 트리거가 거절되는지 확인합니다.
package com.kafkick.batch.api;

import static com.kafkick.batch.api.VerifyApiProbe.error;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code VerifyTriggerApiTest} 로는 이 축을 못 잰다.</b> 그 클래스는
 * {@code batch.scheduling.enabled=false} 로 고정돼 있어 — 그것이 {@code verifyJob} 이
 * 도는 유일한 조건이라 — 스케줄러가 켜진 경로를 구조적으로 밟을 수 없다.
 *
 * <p><b>왜 거절해야 하나.</b> 만료는 재고를 쓰는 유일한 배치이고, 판정 근거인
 * {@code dataset_fingerprint} 재료에 {@code sum(active_count)} 와 {@code max(updated_at)} 이
 * 들어 있다. 검증 중에 만료가 지나가면 <b>판정의 입력이 판정 도중에 바뀐다.</b>
 *
 * <p>여기서 두 가지를 함께 본다 — 컨트롤러가 접수 단계에서 409 로 답하는 것, 그리고
 * <b>그 검사를 우회해도 잡 안의 가드가 여전히 막는 것.</b> 문서가 <i>"컨트롤러 검사는
 * 편의이고 진실은 잡 안에 있다"</i> 고 적었으므로, 그 문장이 사실인지도 확인한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        // 이 한 줄이 이 클래스의 전부다. 만료 크론은 먼 미래로 밀어 실제로 안 돌게 한다.
        "batch.scheduling.enabled=true",
        "batch.schedule.expire-cron=0 0 0 1 1 *",
        "server.port=0",
        "management.server.port=0",
        "batch.verify.metrics-refresh-ms=120000"
})
@Import(MySqlContainerConfig.class)
class VerifyTriggerSchedulerGuardTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 3, 1, 9, 0);

    @LocalServerPort
    private int port;

    @Autowired
    @Qualifier("verifyJob")
    private Job verifyJob;

    @Autowired
    @Qualifier("jobOperator")
    private JobOperator sharedJobOperator;

    @Test
    @DisplayName("스케줄러가 켜져 있으면 접수 단계에서 409")
    void refusesToAcceptWhileTheSchedulerIsOn() throws Exception {
        var response = new VerifyApiProbe(port).post("/api/v1/admin/verify?asOf=" + AS_OF);

        assertThat(response.statusCode())
                .as("만료가 도는 동안의 판정은 근거가 검증 중에 바뀐다")
                .isEqualTo(409);
        assertThat(error(response).path("code").asText()).isEqualTo("VERIFICATION-012");
    }

    /**
     * <b>컨트롤러 검사를 지워도 막혀야 한다.</b> 그것이 문서가 말한 <i>"진실은 잡 안에 있다"</i>
     * 이고, 그 문장이 사실이 아니면 컨트롤러 한 줄이 유일한 방어가 된다.
     */
    @Test
    @DisplayName("컨트롤러를 우회해도 잡 안의 가드가 막는다")
    void theJobGuardStillRefusesWhenTheControllerIsBypassed() throws Exception {
        // JobOperator.start 는 잡이 FAILED 로 끝나도 **예외를 안 던진다** — 실행 결과를
        // 돌려줄 뿐이다(ExpireScheduler 가 같은 사실 위에 상태 검사를 세워 뒀다).
        // 그래서 반환값을 본다. 공용 operator 는 동기라 여기서 이미 끝나 있고,
        // 이 객체는 살아 있는 인스턴스라 실패 예외가 채워져 있다(DB 조회 객체와 다르다).
        JobExecution execution = sharedJobOperator.start(verifyJob,
                new JobParametersBuilder()
                        .addLocalDateTime("asOf", AS_OF)
                        .addString("dataset", "CLEAN")
                        .addString("scope", "FULL")
                        .addLong("attempt", 1L)
                        .toJobParameters());

        assertThat(execution.getStatus())
                .as("가드가 죽으면 여기가 COMPLETED 로 바뀐다 — 그때 컨트롤러 한 줄이 "
                        + "유일한 방어가 된다")
                .isEqualTo(BatchStatus.FAILED);
        assertThat(execution.getAllFailureExceptions())
                .anySatisfy(failure -> assertThat(failure)
                        .isInstanceOf(BusinessException.class)
                        .hasMessageContaining("스케줄러"));
    }
}
