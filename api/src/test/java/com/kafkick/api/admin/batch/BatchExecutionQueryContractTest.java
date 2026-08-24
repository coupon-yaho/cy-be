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
