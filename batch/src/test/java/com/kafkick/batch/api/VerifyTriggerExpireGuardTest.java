// 만료가 도는 동안에는 검증 트리거가 거절되는지, 안 돌면 접수되는지 확인합니다.
package com.kafkick.batch.api;

import static com.kafkick.batch.api.VerifyApiProbe.awaitUntil;
import static com.kafkick.batch.api.VerifyApiProbe.error;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.job.ExpireJobConfig;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>{@code VerifyTriggerApiTest} 로는 이 축을 못 잰다.</b> 그 클래스에는 실행 중인 만료가
 * 없어 이 가드를 구조적으로 밟을 수 없다.
 *
 * <p><b>왜 거절해야 하나.</b> 만료는 재고를 쓰는 유일한 배치이고, 판정 근거인
 * {@code dataset_fingerprint} 재료에 {@code sum(active_count)} 와 {@code max(updated_at)} 이
 * 들어 있다. 검증 중에 만료가 지나가면 <b>판정의 입력이 판정 도중에 바뀐다.</b>
 *
 * <p>여기서 세 가지를 본다 — 컨트롤러가 접수 단계에서 409 로 답하는 것, <b>그 검사를 우회해도
 * 잡 안의 가드가 여전히 막는 것</b>, 그리고 <b>만료가 안 돌면 스케줄러가 켜져 있어도
 * 접수되는 것</b>. 문서가 <i>"컨트롤러 검사는 편의이고 진실은 잡 안에 있다"</i> 고 적었으므로,
 * 그 문장이 사실인지도 확인한다.
 *
 * <p><b>스케줄러를 켜 둔 채 돈다.</b> 예전에는 그 플래그 하나가 이 API 를 항상 409 로 만들었다 —
 * 셋째 테스트가 그것이 끝났음을 잰다. 크론은 먼 미래로 밀어 진짜 만료가 끼어들지 않게 한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=true",
        "batch.schedule.expire-cron=0 0 0 1 1 *",
        // 이 클래스는 플래그가 켜져도 접수되는 것을 잰다 — 그래서 플래그를 켜 둬야 한다.
        // 발화는 크론을 1월 1일로 밀어 막는다. **그 시각에 도는 CI 는 이 잡을 한 번
        // 실행한다** — 연 1회 1초짜리 창이고, 스케줄러를 끄면 이 클래스가 재려는
        // 축이 사라지므로 그 창을 남긴다. 없애려면 크론이 아니라 트리거를 갈아야 한다.
        //
        // 연 단위 크론으로는 어떤 SLA 도 못 맞춰 기동 가드가 거절하므로,
        // 그 검사가 여기서 뜻이 없다는 것을 값으로 명시한다.
        // 정리 크론도 함께 민다. 그러지 않으면 04:30 UTC(13:30 KST)를 지나며 도는 CI 에서
        // 진짜 정리가 발화해 asof_state · verification_findings · 통계 세 테이블을 지운다 —
        // MySqlContainerConfig 는 컨테이너를 공유하므로 무관한 검증 테스트가 그날만 빨개진다.
        // 연 1회 크론은 CleanupScheduler 의 SLA 가드에 걸리므로 SLA 도 함께 올린다.
        "batch.schedule.cleanup-cron=0 0 0 1 1 *",
        "batch.metrics.cleanup-sla-seconds=999999999",
        "batch.metrics.expire-sla-seconds=999999999",
        // 검증 크론도 함께 민다(CY-470). 기본값 05:00 UTC 를 그대로 두면
        // 그 시각을 지나며 도는 CI 에서 진짜 검증이 발화해, 공유 컨테이너의
        // asof_state 를 300만 행까지 채우고 다른 테스트의 전제를 바꾼다 —
        // 위 정리 크론을 민 것과 같은 이유다. 연 1회는 SLA 가드에 걸려 SLA 도 올린다.
        "batch.schedule.verify-cron=0 0 0 1 1 *",
        "batch.metrics.verify-sla-seconds=999999999",
        "server.port=0",
        "management.server.port=0",
        "batch.verify.metrics-refresh-ms=120000"
})
@Import(MySqlContainerConfig.class)
class VerifyTriggerExpireGuardTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 3, 1, 9, 0);

    @LocalServerPort
    private int port;

    @Autowired
    @Qualifier("verifyJob")
    private Job verifyJob;

    @Autowired
    @Qualifier("jobOperator")
    private JobOperator sharedJobOperator;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    /**
     * <b>셋째 테스트가 검증 잡을 실제로 끝까지 돌린다.</b> 그래서 치울 것이 셋이다.
     *
     * <p>⑴ 실행기가 스레드 하나라, 도는 것을 두고 나가면 다음 클래스가 그 스레드를 못 잡는다.
     *
     * <p>⑵ {@code statsAggregateStep} 이 {@code hourly_stats} 등에 행을 심고 그것이
     * {@code verification_runs} 를 FK 로 문다. 남기면 다음 <b>다른 클래스</b>가
     * {@code DELETE FROM verification_runs} 에서 FK 위반으로 죽는다 — 원인 테스트가 아니라
     * 남의 테스트가 빨개지는 모양이라 찾기가 어렵다.
     *
     * <p>⑶ 배치 메타도 함께 비운다. 안 비우면 같은 파라미터 조합이 {@code COMPLETED} 로 남아
     * 다음 실행이 <b>{@code JobInstanceAlreadyCompleteException}</b> 으로 죽는다 —
     * 이 클래스 안에서 실제로 그렇게 깨졌다.
     */
    @AfterEach
    void tearDown() throws Exception {
        awaitUntil(Duration.ofSeconds(90), "검증 잡이 안 끝났습니다",
                () -> jobRepository.findRunningJobExecutions(
                        VerifyJobConfig.JOB_NAME).isEmpty());
        new VerificationSeed(jdbcClient).clear();
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
    }

    @Test
    @DisplayName("만료가 도는 중이면 접수 단계에서 409")
    void refusesToAcceptWhileExpireIsRunning() throws Exception {
        try (RunningJobFixture expire = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireJobConfig.JOB_NAME, AS_OF.minusDays(1))) {

            var response = new VerifyApiProbe(port).post("/api/v1/admin/verify?asOf=" + AS_OF);

            assertThat(response.statusCode())
                    .as("만료가 도는 동안의 판정은 근거가 검증 중에 바뀐다")
                    .isEqualTo(409);
            assertThat(error(response).path("code").asText()).isEqualTo("VERIFICATION-012");
            assertThat(error(response).path("message").asText())
                    .as("detail 은 로그용이고 클라이언트에 나가는 문구는 errorCode 의 것이다")
                    .doesNotContain(String.valueOf(expire.executionId()));
        }
    }

    /**
     * <b>컨트롤러 검사를 지워도 막혀야 한다.</b> 그것이 문서가 말한 <i>"진실은 잡 안에 있다"</i>
     * 이고, 그 문장이 사실이 아니면 컨트롤러 한 줄이 유일한 방어가 된다.
     */
    @Test
    @DisplayName("컨트롤러를 우회해도 잡 안의 가드가 막는다")
    void theJobGuardStillRefusesWhenTheControllerIsBypassed() throws Exception {
        try (RunningJobFixture expire = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireJobConfig.JOB_NAME, AS_OF.minusDays(2))) {

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
                            .hasMessageContaining("만료 배치가 실행 중"))
                    .as("막고 있는 실행 id 가 detail 에 남아야 로그로 원인을 짚는다")
                    .anySatisfy(failure -> assertThat(failure)
                            .hasMessageContaining(String.valueOf(expire.executionId())));
        }
    }

    /**
     * <b>이 티켓이 여는 문이다.</b> 예전 가드는 {@code batch.scheduling.enabled} 만 봤으므로
     * 이 조합 — 스케줄러가 켜져 있고 만료는 안 도는 상태 — 에서 <b>항상 409</b> 였다.
     * 운영은 늘 이 조합이라, 그것이 검증을 온디맨드로 밀어낸 원인이었다.
     */
    @Test
    @DisplayName("만료가 안 돌면 스케줄러가 켜져 있어도 접수된다")
    void acceptsWhenNoExpireIsRunning() throws Exception {
        var response = new VerifyApiProbe(port).post("/api/v1/admin/verify?asOf=" + AS_OF);

        assertThat(response.statusCode())
                .as("이 조합이 409 로 돌아오면 가드가 다시 플래그를 보고 있는 것이다")
                .isEqualTo(202);
    }

    /**
     * <b>이 티켓이 새로 닫는 창이다(CY-470).</b> 위 셋은 전부 <i>이미 도는</i> 만료를 본다.
     * <b>곧 뜰</b> 만료는 아무도 안 봤다 — 그리고 {@code max-expire-skips} 가 0 이 되면서
     * 그 창이 위험해졌다: 만료는 검증을 <b>건너뛰지 않고 지나가고</b>, 그때 찍히는
     * {@code updated_at} 때문에 그 {@code asOf} 는 영구히 못 쓴다(재시딩 말고 복구가 없다).
     *
     * <p>하필 만료 04:10 UTC 가 <b>13:10 KST</b> 라 시연 준비 시간대와 겹친다 — 규약을
     * 문서에만 두면 그 시각에 누르는 사람을 아무것도 막지 못한다.
     *
     * <p><b>이 컨텍스트의 만료 크론은 연 1회(1월 1일)</b>라 평소에는 이 갈래가 안 열린다.
     * 그래서 크론을 <b>지금 곧 뜨는 값</b>으로 바꾼 별도 컨텍스트에서 잰다.
     */
    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
            "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
            "spring.batch.job.enabled=false",
            // **스케줄러를 켜야 한다.** 꺼져 있으면 만료가 아예 안 떠서 이 가드가 통째로
            // 비활성이다 — 그 상태로 재면 항상 202 라 아무것도 안 지킨다.
            "batch.scheduling.enabled=true",
            // ⚠️ **진짜로 뜨는 크론을 쓰면 안 된다.** 1분 크론을 줬다가 CodeRabbit 이 잡았다 —
            //    분 경계에 만료가 실제로 발화하면 접수가 VERIFICATION-017 이 아니라
            //    **먼저 오는 검사**인 VERIFICATION-012(이미 도는 만료)에 걸려 간헐 실패한다.
            //    안 뜨는 크론(1월 1일)을 주고, "다음 발화까지 얼마" 는 아래 고정 시계로 만든다 —
            //    .coderabbit.yaml 이 시각 의존 테스트에 Clock.fixed 를 규약으로 못 박는다.
            "batch.schedule.expire-cron=0 0 0 1 1 *",
            "batch.metrics.expire-sla-seconds=999999999",
            "batch.schedule.cleanup-cron=0 0 0 1 1 *",
            "batch.metrics.cleanup-sla-seconds=999999999",
            "batch.schedule.verify-cron=0 0 0 1 1 *",
            "batch.metrics.verify-sla-seconds=999999999",
            "batch.metrics.expire-sla-seconds=999999999",
            "server.port=0",
            "management.server.port=0",
            "batch.verify.metrics-refresh-ms=120000"
    })
    @Import({MySqlContainerConfig.class, WhenExpireIsAboutToFire.FixedClockConfig.class})
    @DisplayName("만료가 곧 뜰 때")
    class WhenExpireIsAboutToFire {

        /**
         * <b>만료 크론(1월 1일 00:00) 직전으로 시계를 세운다.</b> 다음 발화까지 10분이라
         * 검증 최악 소요(1,200초 = 20분)보다 짧아 반드시 거절 갈래로 간다.
         *
         * <p>실제 발화는 안 한다 — 스케줄러가 보는 것은 <b>진짜 시각</b>이고 그쪽 기준으로는
         * 1월 1일이 멀기 때문이다. 고정 시계는 컨트롤러의 {@code TimeProvider} 만 움직인다.
         */
        @TestConfiguration
        static class FixedClockConfig {
            @Bean
            @Primary
            Clock fixedClock() {
                return Clock.fixed(
                        LocalDateTime.of(2026, 12, 31, 23, 50).toInstant(ZoneOffset.UTC),
                        ZoneOffset.UTC);
            }
        }

        @LocalServerPort
        private int port;

        @Test
        @DisplayName("접수 단계에서 409 로 거절하고 언제 다시 부를지 말해 준다")
        void refusesAndSaysWhenToRetry() throws Exception {
            var response = new VerifyApiProbe(port).post("/api/v1/admin/verify?asOf=" + AS_OF);

            assertThat(response.statusCode())
                    .as("이 검사가 없으면 접수는 통과하고, 곧 뜬 만료가 그 asOf 를 영구히 "
                            + "못 쓰게 만든다 — 실패가 시연 직전에 드러난다")
                    .isEqualTo(409);
            assertThat(response.body())
                    .as("**전용 코드여야 한다.** VERIFICATION-012(이미 도는 만료)와 상태 코드는 "
                            + "같지만 처방이 다르다 — 그쪽은 '끝나길 기다려라', 이쪽은 "
                            + "'배치 창을 지난 뒤에 불러라' 다")
                    .contains("VERIFICATION-017");
            assertThat(response.body())
                    .as("응답에 나가는 것은 고정 문구뿐이다(핸들러가 detail 을 로그에만 남긴다). "
                            + "그러니 그 문구 자체가 처방을 말해야 한다 — 안 그러면 "
                            + "운영자가 같은 시각에 또 누른다")
                    .contains("배치 창")
                    .contains("영구히 못 쓰게");
        }
    }
}
