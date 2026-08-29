package com.kafkick.storage.db.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.batch.BatchRun;
import com.kafkick.storage.db.RepositoryTest;

/**
 * 배치 실행 이력 조회.
 *
 * <p>배치 메타에 직접 심는다. 여기서 재려는 것은 잡의 동작이 아니라 조회 SQL 이고,
 * 잡을 띄우면 상태·시각을 원하는 모양으로 못 만든다.
 */
@RepositoryTest
@Import(BatchRunJdbcAdapter.class)
class BatchRunHistoryTest {

    @Autowired
    private BatchRunJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    @DisplayName("최근 실행부터 준다 — START_TIME 이 아니라 실행 id 순이다")
    void ordersByExecutionIdDescending() {
        plant(1, "expireJob", "COMPLETED", true);
        plant(2, "verifyJob", "COMPLETED", true);

        assertThat(adapter.findRecent(null, 10, 0)).extracting(BatchRun::executionId)
                .containsExactly(2L, 1L);
    }

    @Test
    @DisplayName("시작조차 못 한 실행도 목록에 나온다 — 실행기가 거절한 행이 그 모양이다")
    void includesExecutionsWithoutStartTime() {
        plant(1, "verifyJob", "FAILED", false);

        List<BatchRun> recent = adapter.findRecent(null, 10, 0);

        assertThat(recent).hasSize(1);
        assertThat(recent.getFirst().startedAtInBatchMetaZone())
                .as("START_TIME 이 NULL 인 행을 정렬 키로 쓰거나 조인으로 걸러내면 사라진다")
                .isNull();
    }

    @Test
    @DisplayName("jobName 을 주면 그 잡만, 안 주면 전체")
    void filtersByJobName() {
        plant(1, "expireJob", "COMPLETED", true);
        plant(2, "verifyJob", "COMPLETED", true);

        assertThat(adapter.findRecent("expireJob", 10, 0)).extracting(BatchRun::jobName)
                .containsExactly("expireJob");
        assertThat(adapter.countRecent("expireJob")).isEqualTo(1);
        assertThat(adapter.countRecent(null)).isEqualTo(2);
    }

    @Test
    @DisplayName("limit·offset 이 페이지를 가른다 — 조인·서브쿼리와 함께 도는지는 따로 재야 한다")
    void paginates() {
        plant(1, "expireJob", "COMPLETED", true);
        plant(2, "verifyJob", "COMPLETED", true);
        plant(3, "cleanupJob", "COMPLETED", true);

        assertThat(adapter.findRecent(null, 2, 0)).extracting(BatchRun::executionId)
                .containsExactly(3L, 2L);
        assertThat(adapter.findRecent(null, 2, 2)).extracting(BatchRun::executionId)
                .as("두 번째 페이지가 첫 페이지와 겹치면 화면이 같은 행을 두 번 그린다")
                .containsExactly(1L);
    }

    @Test
    @DisplayName("Step 이 하나도 없으면 카운트가 비어 있다 — 0 이면 '아무것도 안 했다' 로 읽힌다")
    void hasNoCountsWhenNoStepRan() {
        plant(1, "verifyJob", "FAILED", false);

        BatchRun run = adapter.findRecent(null, 10, 0).getFirst();

        assertThat(run.stepReadTotal())
                .as("SUM() 은 대상이 없으면 NULL 이다. 널가드를 빼면 여기서 NPE 로 목록이 죽는다")
                .isNull();
        assertThat(run.stepWriteTotal()).isNull();
    }

    @Test
    @DisplayName("Step 이 여럿이면 합쳐서 준다 — 화면이 '몇 건 처리했나' 로 읽는다")
    void sumsStepCounts() {
        plant(1, "expireJob", "COMPLETED", true);
        step(1, 10, 100);
        step(1, 20, 200);

        assertThat(adapter.findRecent(null, 10, 0).getFirst().stepWriteTotal()).isEqualTo(300L);
        assertThat(adapter.findRecent(null, 10, 0).getFirst().stepReadTotal()).isEqualTo(30L);
    }

    private void plant(long id, String jobName, String status, boolean started) {
        jdbcClient.sql("""
                INSERT INTO BATCH_JOB_INSTANCE (JOB_INSTANCE_ID, VERSION, JOB_NAME, JOB_KEY)
                VALUES (:id, 0, :jobName, :key)
                """).param("id", id).param("jobName", jobName)
                .param("key", "k" + id).update();
        jdbcClient.sql("""
                INSERT INTO BATCH_JOB_EXECUTION
                    (JOB_EXECUTION_ID, VERSION, JOB_INSTANCE_ID, CREATE_TIME, START_TIME,
                     END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE)
                VALUES (:id, 0, :id, NOW(), :start, :end, :status, :status, '')
                """).param("id", id).param("status", status)
                .param("start", started ? LocalDateTime.now() : null)
                .param("end", started ? LocalDateTime.now().plusSeconds(3) : null)
                .update();
    }

    private void step(long executionId, long readCount, long writeCount) {
        jdbcClient.sql("""
                INSERT INTO BATCH_STEP_EXECUTION
                    (STEP_EXECUTION_ID, VERSION, STEP_NAME, JOB_EXECUTION_ID, CREATE_TIME,
                     STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT,
                     READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT)
                VALUES (:stepId, 0, 'step', :executionId, NOW(),
                        'COMPLETED', 1, :readCount, 0, :writeCount, 0, 0, 0, 0)
                """).param("stepId", executionId * 100 + writeCount)
                .param("executionId", executionId)
                .param("readCount", readCount).param("writeCount", writeCount).update();
    }
}
