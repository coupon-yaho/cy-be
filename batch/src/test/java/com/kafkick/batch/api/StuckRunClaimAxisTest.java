// 선점문의 시각 파라미터가 컬럼과 같은 좌표계에 있는지 세 서비스 모두 잽니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>선점문의 {@code :stuckBefore} 는 컬럼과 같은 축에 있어야 한다.</b> 배치 메타의
 * {@code LAST_UPDATED}·{@code START_TIME}·{@code CREATE_TIME} 은 스프링 배치가
 * {@code Timestamp.valueOf} 로 써서 드라이버가 세션 존(UTC)으로 렌더링한 값인데,
 * {@code LocalDateTime} 을 그대로 바인딩하면 그 정규화를 <b>안 탄다</b>.
 *
 * <p><b>실측은 {@link com.kafkick.batch.config.DefaultZoneGuard} 가 정본으로 든다.</b>
 * 요지만 옮기면, KST JVM 에서 두 축이 아홉 시간 어긋나 진도 조건이 <b>항상 참</b>이 되고
 * 방금 뜬 실행이 시체 판정을 통과한다 — 복구·중단 API 가 도는 잡을 닫는다.
 *
 * <p><b>세 문장을 함께 잰다.</b> 그리고 <b>변환을 테스트가 직접 부르지 않는다</b> —
 * {@link StuckRunClaim#claim} 이 바인딩까지 감싸서, 콜사이트에서 축을 빠뜨리는 것이
 * 구조적으로 불가능하다. 한때 테스트가 변환을 직접 불렀는데 그러면 <b>SQL 과 헬퍼만</b>
 * 재고 서비스가 그것을 쓰는지는 안 재는 상태였다.
 *
 * <p>자바 쪽 벽시계로 만든 <b>원시 값</b>을 심는 것이 요점이다. 기존 테스트들은
 * {@code stuckExecutions()} 가 <b>DB 에서 읽어 이미 정규화된</b> 값을 써서 그 축을 못 잰다.
 */
@SpringBootTest(properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class StuckRunClaimAxisTest {

    /** {@code batch.stuck-job-after-ms} 기본이 30분이다. 넉넉히 넘긴다. */
    private static final Duration DEAD = Duration.ofHours(2);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @AfterEach
    void tearDown() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
    }

    static Stream<Arguments> claimSites() {
        return Stream.of(
                Arguments.of(ExpireStepContext.JOB_NAME, StuckRunClaim.CLAIM, 1),
                Arguments.of(CleanupJobConfig.JOB_NAME, StuckRunClaim.CLAIM, 2),
                Arguments.of(VerifyJobConfig.JOB_NAME, VerifyStopService.CLAIM, 3));
    }

    /**
     * 잡 이름과 문장을 함께 넘긴다 — 검증 쪽은 {@code STOPPING} 을 뺀 자기 문장을 쓴다.
     * {@code slot} 은 {@code JOB_INST_UN} 때문에 케이스마다 달라야 한다.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("claimSites")
    void freshRunNeverPassesTheClaim(String jobName, String claim, int slot) throws Exception {
        try (RunningJobFixture fresh = RunningJobFixture.plant(
                jobRepository, jdbcClient, jobName,
                LocalDateTime.of(2026, 7, 1, 0, 0).plusHours(slot),
                Duration.ZERO, Duration.ZERO)) {

            // 자바 쪽 벽시계로 만든 원시 값. 축을 안 맞추면 KST 에서 항상 참이 된다.
            LocalDateTime stuckBefore = LocalDateTime.now().minus(DEAD);

            assertThat(StuckRunClaim.claim(jdbcClient, claim, fresh.executionId(), stuckBefore))
                    .as("%s: 바인딩이 컬럼과 다른 축이면 KST 에서 방금 뜬 실행도 선점을 "
                            + "지난다 — 살아 있는 잡을 FAILED 로 닫는다", jobName)
                    .isZero();

            assertThat(jobRepository.getJobExecution(fresh.executionId()).getStatus())
                    .isEqualTo(BatchStatus.STARTED);
        }
    }
}
