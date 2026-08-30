// 검증 실행 행 안에서 배치 메타 시각과 도메인 시각이 같은 축에 서는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.AfterEach;
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
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>한 행 안에 축이 둘 있다.</b> {@code as_of}·{@code from_ts} 는 {@code TimeProvider}(UTC)
 * 에서 오고, {@code started_at}·{@code finished_at} 은 스프링 배치가 <b>인자 없는
 * {@code LocalDateTime.now()}</b> 로 찍은 <b>JVM 기본 존</b> 벽시계다
 * ({@code .coderabbit.yaml} 이 그 출처를 계약으로 못 박았다).
 *
 * <p>옮기지 않으면 UTC 가 아닌 JVM 에서 두 축이 그 존의 오프셋만큼 벌어진다. 예외도 로그도
 * 안 나고 판정 결과도 안 바뀌어서, <b>나중에 그 시각으로 사건을 맞춰 볼 때</b> 드러난다.
 *
 * <p><b>테스트 JVM 이 일부러 {@code Asia/Seoul} 이라</b>({@code batch/build.gradle}) 이 검사가
 * 성립한다 — UTC 였으면 변환이 항등이라 아무것도 못 잰다. 그래서 전제를 {@code assumeThat}
 * 으로 먼저 밝힌다.
 *
 * <p><b>DB 가 저장한 벽시계를 직접 읽는다.</b> 자바로 되읽으면 쓰기·읽기가 같은 변환을
 * 대칭으로 지나 <b>어느 축이든 항등</b>이라 안 갈린다 — CY-718 에서 같은 함정을 밟았다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2"
})
@Import(MySqlContainerConfig.class)
class VerifyRunAxisTest {

    /**
     * <b>이 클래스만의 {@code asOf} 다.</b> 잡 인스턴스는 식별 파라미터로 정해지는데
     * ({@code asOf}·{@code scope}·{@code dataset}·{@code attempt}) 다른 검증 테스트가 전부
     * {@code 2026-01-15 09:00} · {@code attempt=1} 을 쓴다 — 같은 값을 쓰면 전체 실행에서
     * 먼저 돈 쪽이 인스턴스를 완료시켜 이쪽이 {@code JobInstanceAlreadyCompleteException} 으로
     * 죽는다(단독 실행에서는 안 드러난다). 분 단위로 갈라 둔다.
     */
    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 43);

    /** DB 의 {@code DATE_FORMAT(...,'%Y-%m-%d %H:%i:%s')} 과 같은 모양. */
    private static final DateTimeFormatter SQL_WALL_CLOCK =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job verifyJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    /**
     * <b>비우고 시작한다.</b> 컨테이너를 JVM 이 공유하므로({@code MySqlContainerConfig})
     * 앞 클래스가 남긴 행이 그대로 보인다 — 형제들은 {@code @BeforeEach} 에서 비우고
     * <b>자기 행은 남기고</b> 끝나므로, 뒤에서 도는 이 클래스가 그것을 읽는다.
     * 남은 {@code issuance_histories} 는 {@code rejectAsOfBeforeLatestHistory} 에,
     * 남은 {@code verification_runs} 는 {@code rejectExistingRun} 에 걸려 잡이 죽는다.
     * <b>단독 실행에서는 안 드러나고 전체 빌드에서만 빨갛다.</b>
     */
    @BeforeEach
    void setUp() {
        clearAll();
    }

    @AfterEach
    void tearDown() {
        // **형제 관용 그대로 쓴다.** 손으로 테이블 목록을 적으면 FK 순서와 대상이 두 벌이 되고,
        // VerificationSeed.clear() 는 배치 메타(BATCH_*)까지 함께 비운다 — 그 파일이 적어 뒀듯
        // removeJobExecutions() 만으로는 **실행이 없는 인스턴스가 남아** 다음 클래스의 같은
        // asOf 와 충돌한다. 아래 AS_OF 를 갈라 둔 것도 같은 실패를 겪고 한 조치다.
        clearAll();
    }

    private void clearAll() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        new VerificationSeed(jdbcClient).clear();
    }

    @Test
    @DisplayName("started_at·finished_at 이 as_of 와 같은 좌표계에 저장된다")
    void batchMetaTimesLandOnTheDomainAxis() throws Exception {
        assumeThat(ZoneId.systemDefault().getRules().getOffset(Instant.now()))
                .as("UTC JVM 에서는 변환이 항등이라 이 축을 못 잰다")
                .isNotEqualTo(ZoneOffset.UTC);

        JobExecution execution = jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", 1L)
                .toJobParameters());

        // ⚠️ **여기는 assumeThat 이면 안 된다.** 축을 한쪽만 옮기면 finished_at 이
        //    started_at 보다 앞서서 VerificationRun.finish 가 던지고 잡이 FAILED 한다 —
        //    가정으로 두면 그 실패를 삼켜 테스트가 건너뛰어지고 빌드는 초록이다.
        //    실제로 그렇게 만들었다가 돌연변이 둘이 다 살아남았다.
        assertThat(execution.getStatus())
                .as("축을 한쪽만 옮기면 finish 의 순서 검사에 걸려 여기서 FAILED 가 된다: %s",
                        failureOf(execution))
                .isEqualTo(BatchStatus.COMPLETED);

        // 배치 메타가 들고 있는 JVM 존 벽시계. 저장된 값이 이것과 같으면 안 옮긴 것이다.
        LocalDateTime jvmWall = execution.getStartTime();
        LocalDateTime utcWall = jvmWall.atZone(ZoneId.systemDefault())
                .withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();

        assertThat(columnWallClock("started_at"))
                .as("배치 메타 시각을 그대로 넣으면 as_of(UTC)와 이 존의 오프셋만큼 벌어진다")
                .isEqualTo(format(utcWall))
                .isNotEqualTo(format(jvmWall));

        // ⚠️ **"종료가 시작보다 뒤" 만으로는 부족하다.** finished_at 만 안 옮기면 그 값이
        //    started_at 보다 오프셋만큼 **뒤로** 가서 그 단언을 그대로 통과한다 —
        //    실제로 그렇게 뒀다가 돌연변이 하나가 살아남았다. **간격**을 봐야 갈린다.
        assertThat(secondsBetweenStartAndFinish())
                .as("한 실행의 시작과 종료는 초 단위로 붙어 있다. 한쪽만 옮기면 그 간격이 "
                        + "JVM 존 오프셋(테스트 JVM 은 9시간)만큼 벌어진다")
                .isBetween(0L, 600L);
    }

    /** 한 실행의 시작~종료 간격(초). 축이 한쪽만 옮겨지면 여기가 오프셋만큼 벌어진다. */
    private long secondsBetweenStartAndFinish() {
        return jdbcClient.sql("""
                        SELECT TIMESTAMPDIFF(SECOND, started_at, finished_at)
                          FROM verification_runs ORDER BY id DESC LIMIT 1
                        """)
                .query(Long.class).single();
    }

    /** 실패 원인을 단언 메시지에 싣는다 — FAILED 만 보면 왜인지 모른다. */
    private static String failureOf(JobExecution execution) {
        return execution.getAllFailureExceptions().stream()
                .map(Throwable::getMessage)
                .findFirst()
                .orElse("(예외 없음)");
    }

    /** <b>DB 가 실제로 저장한 벽시계.</b> 자바 왕복은 대칭이라 축을 못 가른다. */
    private String columnWallClock(String column) {
        return jdbcClient.sql("SELECT DATE_FORMAT(" + column + ",'%Y-%m-%d %H:%i:%s')"
                        + " FROM verification_runs ORDER BY id DESC LIMIT 1")
                .query(String.class).single();
    }

    /**
     * <b>{@code toString()} 을 쓰면 안 된다.</b> 초가 0 이면 {@code :00} 을 생략해
     * {@code 2026-01-15 09:00} 을 주는데 DB 의 {@code DATE_FORMAT} 은 언제나
     * {@code 09:00:00} 이다 — 같은 시각인데 문자열이 달라 <b>1.7% 확률로</b> 빨개지고,
     * 실패 메시지는 축이 어긋났다고 말해 <b>정반대 방향으로 조사하게</b> 만든다.
     * {@code VerificationRunJdbcAdapter} 가 같은 함정을 이미 적어 뒀다.
     */
    private static String format(LocalDateTime value) {
        return SQL_WALL_CLOCK.format(value);
    }
}
