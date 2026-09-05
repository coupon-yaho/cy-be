package com.kafkick.api.admin.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.batch.BatchExecution;
import com.kafkick.core.batch.BatchStepExecution;
import com.kafkick.core.batch.BatchExecutionRepository;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * 이력 조회의 <b>정렬·매핑 규약</b>을 고정한다. 이 규약이 깨지는 방향은 전부 <b>예외가 안 나는</b>
 * 쪽이라 다른 테스트가 잡지 못한다 — 컨트롤러 테스트는 빈 목록 스텁을 쓰고, 풀 테스트는
 * 결과가 아니라 커넥션 획득 수만 센다.
 *
 * <p><b>가장 중요한 것은 {@code START_TIME IS NULL} 행이다.</b> 어댑터 javadoc 이
 * "시작도 못 한 실행이 첫 페이지에서 사라지지 않게 {@code CREATE_TIME} 을 1순위로 둔다" 는
 * 계약을 선언하는데, 누가 {@code ORDER BY e.START_TIME DESC} 로 바꾸면 MySQL 이 {@code NULL} 을
 * 뒤로 보내 <b>장애 신호 그 자체인 그 행이 첫 페이지에서 사라진다.</b>
 *
 * <p>행은 Spring Batch 를 거치지 않고 직접 넣는다. 원하는 시각·상태 조합
 * (특히 시작하지 못한 실행)을 잡 실행으로는 만들 수 없기 때문이다.
 */
@SpringBootTest(classes = com.kafkick.ApiApplication.class)
@Import(MySqlContainerConfig.class)
class BatchExecutionQueryContractTest {

    @Autowired
    BatchExecutionRepository repository;
    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * 이 테스트가 쓰는 id 대역. <b>공유 컨테이너를 통째로 비우지 않으려고 대역을 나눈다</b> —
     * 지금 이 모듈에서 {@code BATCH_*} 를 쓰는 것은 이 클래스뿐이지만, 나중에 행이 있어야
     * 하는 테스트가 하나 생기면 무조건 {@code DELETE} 가 그것을 밟는다. 그때 증상은
     * "혼자 돌리면 통과, 같이 돌리면 실패" 라 원인을 찾기 어렵다.
     */
    private static final long ID_BASE = 900_000L;

    @BeforeEach
    void seed() {
        jdbcTemplate.update("DELETE FROM BATCH_STEP_EXECUTION WHERE JOB_EXECUTION_ID >= ?", ID_BASE);
        jdbcTemplate.update("DELETE FROM BATCH_JOB_EXECUTION WHERE JOB_EXECUTION_ID >= ?", ID_BASE);
        jdbcTemplate.update("DELETE FROM BATCH_JOB_INSTANCE WHERE JOB_INSTANCE_ID >= ?", ID_BASE);

        instance(1, "alphaJob");
        instance(2, "betaJob");

        // 가장 오래된 것. 첫 페이지에서 밀려나야 한다.
        execution(10, 1, "2026-08-20 10:00:00.000000", "2026-08-20 10:00:01.000000",
                "2026-08-20 10:00:09.000000", "COMPLETED", "COMPLETED");
        // 시작도 못 한 실행. START_TIME · END_TIME 이 둘 다 비어 있다.
        execution(11, 1, "2026-08-22 10:00:00.000000", null, null, "STARTED", "UNKNOWN");
        // 위와 CREATE_TIME 이 같다. 동률은 JOB_EXECUTION_ID DESC 로 갈려야 한다.
        execution(12, 1, "2026-08-22 10:00:00.000000", "2026-08-22 10:00:01.000000",
                "2026-08-22 10:00:05.000000", "FAILED", "FAILED_ON_STEP_2");
        // 다른 잡. 잡 필터가 섞으면 안 된다.
        execution(13, 2, "2026-08-23 10:00:00.000000", "2026-08-23 10:00:01.000000",
                "2026-08-23 10:00:02.000000", "COMPLETED", "COMPLETED");

        // 실행 12(FAILED)의 스텝 둘. 순서가 흔들리면 같은 실행이 두 번 다르게 보인다.
        step(20, 12, "step-two", "FAILED", "FAILED",
                "org.springframework.dao.DataIntegrityViolationException: Duplicate entry"
                        + " 'x' for key 'uk_issuance_usage' ... 스택 2,000자",
                "2026-08-22 10:00:03.000000", "2026-08-22 10:00:05.000000",
                7, 3, 1, 2, 4, 1, 2, 3);
        step(21, 12, "step-one", "COMPLETED", "COMPLETED", "",
                "2026-08-22 10:00:01.000000", "2026-08-22 10:00:03.000000",
                100, 100, 0, 10, 0, 0, 0, 0);
        // 시작도 못 한 스텝. 카운터가 전부 0 이고 시각이 비어 있다.
        step(22, 11, "step-one", "STARTING", "EXECUTING", null, null, null,
                0, 0, 0, 0, 0, 0, 0, 0);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void step(long id, long executionId, String name, String status, String exitCode,
            String exitMessage, String start, String end,
            long read, long write, long filter, long commit,
            long rollback, long readSkip, long processSkip, long writeSkip) {
        jdbcTemplate.update("INSERT INTO BATCH_STEP_EXECUTION"
                + " (STEP_EXECUTION_ID, VERSION, STEP_NAME, JOB_EXECUTION_ID, CREATE_TIME,"
                + "  START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT,"
                + "  WRITE_COUNT, READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT,"
                + "  ROLLBACK_COUNT, EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED)"
                + " VALUES (?, 0, ?, ?, '2026-08-22 10:00:00.000000', ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                + "  ?, ?, ?, ?, '2026-08-22 10:00:00.000000')",
                ID_BASE + id, name, ID_BASE + executionId, start, end, status,
                commit, read, filter, write, readSkip, writeSkip, processSkip, rollback,
                exitCode, exitMessage);
    }

    private void instance(long id, String jobName) {
        jdbcTemplate.update("INSERT INTO BATCH_JOB_INSTANCE"
                + " (JOB_INSTANCE_ID, VERSION, JOB_NAME, JOB_KEY) VALUES (?, 0, ?, ?)",
                ID_BASE + id, jobName, "key-" + (ID_BASE + id));
    }

    private void execution(long id, long instanceId, String create, String start, String end,
            String status, String exitCode) {
        jdbcTemplate.update("INSERT INTO BATCH_JOB_EXECUTION"
                + " (JOB_EXECUTION_ID, VERSION, JOB_INSTANCE_ID, CREATE_TIME, START_TIME,"
                + "  END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED)"
                + " VALUES (?, 0, ?, ?, ?, ?, ?, ?, '', ?)",
                ID_BASE + id, ID_BASE + instanceId, create, start, end, status, exitCode, create);
    }

    /**
     * <b>카운터 여덟 개가 자기 자리에 들어간다.</b>
     *
     * <p>이 매핑이 어긋나는 방향은 전부 <b>예외가 안 나는</b> 쪽이다 — 전부 {@code long} 이라
     * read 와 write 를 바꿔 넣어도 컴파일도 되고 질의도 돈다. 그러면 화면은
     * <b>그럴듯한 숫자</b>를 보여 주고 아무도 모른다. 그래서 여덟 개를 <b>전부 다른 값</b>으로
     * 심는다 — 하나라도 자리가 바뀌면 여기서 걸린다.
     */
    @Test
    @DisplayName("스텝 카운터 8종이 자리를 바꾸지 않는다")
    void mapsAllEightCountersToTheirOwnFields() {
        BatchStepExecution failed = repository.findSteps(ID_BASE + 12).stream()
                .filter(step -> step.stepExecutionId() == ID_BASE + 20).findFirst().orElseThrow();

        assertThat(failed.readCount()).isEqualTo(7);
        assertThat(failed.writeCount()).isEqualTo(3);
        assertThat(failed.filterCount()).isEqualTo(1);
        assertThat(failed.commitCount()).isEqualTo(2);
        assertThat(failed.rollbackCount()).isEqualTo(4);
        assertThat(failed.readSkipCount()).isEqualTo(1);
        assertThat(failed.processSkipCount()).isEqualTo(2);
        assertThat(failed.writeSkipCount()).isEqualTo(3);
    }

    /**
     * <b>실패 원문이 그대로 나가면 안 된다.</b> {@code EXIT_MESSAGE} 에는 스택트레이스가
     * 통째로 들어가고 첫 줄에도 SQL 조각·제약 이름이 섞인다. 이 API 에는 <b>사용자 인증이
     * 없다</b> — 공유 비밀 관문은 소지만 묻고 누가 불렀는지는 안 가른다.
     */
    @Test
    @DisplayName("실패 원문 대신 요약만 나간다 — 제약 이름도 SQL 조각도 안 실린다")
    void neverLeaksTheRawExitMessage() {
        BatchStepExecution failed = repository.findSteps(ID_BASE + 12).stream()
                .filter(step -> step.stepExecutionId() == ID_BASE + 20).findFirst().orElseThrow();

        assertThat(failed.failure())
                .as("예외 클래스 이름까지만 남아야 합니다")
                .isEqualTo("DataIntegrityViolationException");
        assertThat(failed.failure()).doesNotContain("uk_issuance_usage", "Duplicate entry", "스택");
    }

    /**
     * <b>정렬이 흔들리면 같은 실행을 두 번 열었을 때 화면이 달라진다.</b> 시작 시각은
     * nullable 이고 병렬 스텝이면 같은 값이 여럿 나온다 — 그래서 {@code STEP_EXECUTION_ID}
     * 로 잇는다. 시드는 <b>일부러 이름 순서와 id 순서를 어긋나게</b> 넣었다
     * (id 20 = step-two, id 21 = step-one).
     */
    @Test
    @DisplayName("스텝이 STEP_EXECUTION_ID 순서로 나온다 — 이름 순서가 아니다")
    void ordersStepsByExecutionId() {
        assertThat(repository.findSteps(ID_BASE + 12))
                .extracting(BatchStepExecution::stepName)
                .containsExactly("step-two", "step-one");
    }

    /** 시작 못 한 스텝도 0 이나 생성 시각으로 메우지 않는다 — 화면이 "즉시 끝났다" 로 읽는다. */
    @Test
    @DisplayName("시작도 못 한 스텝의 시각이 비어 있다")
    void keepsNullTimesForANeverStartedStep() {
        BatchStepExecution notStarted = repository.findSteps(ID_BASE + 11).getFirst();

        assertThat(notStarted.startedAt()).isNull();
        assertThat(notStarted.endedAt()).isNull();
        assertThat(notStarted.createdAt()).isNotNull();
    }

    /**
     * <b>이 관제의 범용성이 여기 걸려 있다.</b> 어댑터가 잡 이름을 하나도 모르므로
     * 도메인이 바뀌어도(쿠폰 → 사전예약) 새 잡이 그날부터 이 화면에 뜬다.
     * 시드의 {@code alphaJob}·{@code betaJob} 은 이 저장소에 없는 이름이다 —
     * <b>없는 잡의 스텝이 나온다는 것 자체가 증거다.</b>
     */
    @Test
    @DisplayName("모르는 잡의 스텝도 그대로 나온다 — 잡 이름을 박지 않았다")
    void worksForJobsThisRepositoryDoesNotKnow() {
        assertThat(repository.findSteps(ID_BASE + 12)).isNotEmpty();
    }

    /** 없는 실행은 빈 목록이다. 예외로 죽으면 목록에서 눌러 들어온 사람이 500 을 본다. */
    @Test
    @DisplayName("없는 실행은 빈 목록이다")
    void returnsEmptyForAnUnknownExecution() {
        assertThat(repository.findSteps(ID_BASE + 9_999)).isEmpty();
    }

    @Test
    @DisplayName("최신부터 나오고 LIMIT 이 실제로 걸린다")
    void ordersNewestFirstAndAppliesLimit() {
        List<BatchExecution> recent = repository.findRecent(2);

        assertThat(recent).hasSize(2);
        assertThat(recent).extracting(BatchExecution::jobExecutionId)
                .containsExactly(ID_BASE + 13, ID_BASE + 12);
    }

    @Test
    @DisplayName("시작도 못 한 실행이 첫 페이지에서 사라지지 않는다")
    void neverStartedExecutionStaysOnFirstPage() {
        List<BatchExecution> recent = repository.findRecent(3);

        assertThat(recent).extracting(BatchExecution::jobExecutionId).contains(ID_BASE + 11);
        BatchExecution neverStarted = recent.stream()
                .filter(e -> e.jobExecutionId() == ID_BASE + 11).findFirst().orElseThrow();
        // 0 이나 CREATE_TIME 으로 메우면 화면이 "즉시 끝났다" 로 읽는다.
        assertThat(neverStarted.startedAt()).isNull();
        assertThat(neverStarted.endedAt()).isNull();
        assertThat(neverStarted.createdAt()).isNotNull();
    }

    @Test
    @DisplayName("CREATE_TIME 이 같으면 JOB_EXECUTION_ID 내림차순으로 갈린다")
    void breaksTieByExecutionId() {
        List<BatchExecution> recent = repository.findRecent(10);

        assertThat(recent).extracting(BatchExecution::jobExecutionId)
                .containsExactly(ID_BASE + 13, ID_BASE + 12, ID_BASE + 11, ID_BASE + 10);
    }

    @Test
    @DisplayName("잡 이름 필터가 다른 잡을 섞지 않는다")
    void filtersByJobName() {
        List<BatchExecution> alpha = repository.findRecentByJobName("alphaJob", 10);

        assertThat(alpha).extracting(BatchExecution::jobName).containsOnly("alphaJob");
        assertThat(alpha).extracting(BatchExecution::jobExecutionId)
                .containsExactly(ID_BASE + 12, ID_BASE + 11, ID_BASE + 10);
    }

    @Test
    @DisplayName("상태와 종료 코드가 뒤바뀌지 않는다")
    void mapsStatusAndExitCodeToTheirOwnFields() {
        BatchExecution failed = repository.findRecent(10).stream()
                .filter(e -> e.jobExecutionId() == ID_BASE + 12).findFirst().orElseThrow();

        // 둘 다 문자열이라 뒤바뀌어도 컴파일도 조회도 통과한다.
        // 값을 일부러 다르게 심었다. 같은 값이면 뒤바뀌어도 이 단언이 통과한다.
        assertThat(failed.status()).isEqualTo("FAILED");
        assertThat(failed.exitCode()).isEqualTo("FAILED_ON_STEP_2");
        assertThat(failed.jobName()).isEqualTo("alphaJob");
    }
}
