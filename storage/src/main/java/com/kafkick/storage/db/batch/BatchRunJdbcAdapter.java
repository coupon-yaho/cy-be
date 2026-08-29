package com.kafkick.storage.db.batch;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.batch.BatchRun;
import com.kafkick.core.batch.BatchRunRepository;

/**
 * 배치 실행 이력을 Spring Batch 메타에서 읽는다.
 *
 * <p>읽기 전용이다. 이 테이블에 쓰는 것은 프레임워크와 정리 배치뿐이다.
 */
@Repository
public class BatchRunJdbcAdapter implements BatchRunRepository {

    /**
     * Step 집계를 함께 준다. 잡 하나에 Step 이 여럿이라 합쳐서 본다 —
     * 화면이 "몇 건을 처리했나" 로 읽는 값이다.
     *
     * <p>시작 시각이 아니라 실행 id 로 정렬한다. 실행기가 거절한 행은 START_TIME 이
     * NULL 이라 정렬 키로 쓰면 그 행이 어디로 갈지 정해지지 않는다.
     */
    private static final String SELECT_RECENT = """
            SELECT je.JOB_EXECUTION_ID, ji.JOB_NAME, je.STATUS, je.EXIT_CODE,
                   je.EXIT_MESSAGE, je.START_TIME, je.END_TIME,
                   (SELECT SUM(se.READ_COUNT) FROM BATCH_STEP_EXECUTION se
                     WHERE se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID) AS READ_TOTAL,
                   (SELECT SUM(se.WRITE_COUNT) FROM BATCH_STEP_EXECUTION se
                     WHERE se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID) AS WRITE_TOTAL
              FROM BATCH_JOB_EXECUTION je
              JOIN BATCH_JOB_INSTANCE ji ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
             WHERE (:jobName IS NULL OR ji.JOB_NAME = :jobName)
             ORDER BY je.JOB_EXECUTION_ID DESC
             LIMIT :limit OFFSET :offset
            """;

    private static final String COUNT_RECENT = """
            SELECT COUNT(*)
              FROM BATCH_JOB_EXECUTION je
              JOIN BATCH_JOB_INSTANCE ji ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
             WHERE (:jobName IS NULL OR ji.JOB_NAME = :jobName)
            """;

    private static final RowMapper<BatchRun> ROW_MAPPER = (rs, rowNum) -> new BatchRun(
            rs.getLong("JOB_EXECUTION_ID"),
            rs.getString("JOB_NAME"),
            rs.getString("STATUS"),
            rs.getString("EXIT_CODE"),
            rs.getString("EXIT_MESSAGE"),
            rs.getObject("START_TIME", LocalDateTime.class),
            rs.getObject("END_TIME", LocalDateTime.class),
            (Long) rs.getObject("READ_TOTAL"),
            (Long) rs.getObject("WRITE_TOTAL"));

    private final JdbcClient jdbcClient;

    public BatchRunJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<BatchRun> findRecent(String jobName, int limit, int offset) {
        return jdbcClient.sql(SELECT_RECENT)
                .param("jobName", jobName)
                .param("limit", limit)
                .param("offset", offset)
                .query(ROW_MAPPER)
                .list();
    }

    @Override
    public int countRecent(String jobName) {
        Integer count = jdbcClient.sql(COUNT_RECENT)
                .param("jobName", jobName)
                .query(Integer.class)
                .single();
        return count == null ? 0 : count;
    }
}
