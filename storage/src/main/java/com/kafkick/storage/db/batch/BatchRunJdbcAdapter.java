package com.kafkick.storage.db.batch;

import java.math.BigDecimal;
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
     *
     * <p>V2026082514 의 낡은 예측 정정. 그 파일이 "관리 화면의 이력 목록은
     * {@code ORDER BY CREATE_TIME DESC, JOB_EXECUTION_ID DESC} 가 될 것" 으로 적고
     * IX_JOB_EXEC_CREATE_TIME 을 그 근거로 들었는데, 실제로 짠 것은 이 문장이고
     * PK 하나로 정렬한다. 이미 적용된 마이그레이션이라 체크섬 때문에 그 파일을 못 고쳐
     * 정정을 여기 둔다. 그 인덱스는 여전히 정리 배치의 대상 선택이 쓰므로 지우면 안 된다.
     *
     * <p><b>실행계획은 필터 유무로 갈린다</b>(EXPLAIN ANALYZE, coupon_clean, 메타 10행):
     * <pre>
     * jobName = null   je 를 PRIMARY 역방향 스캔 → ji 는 PK 단건 조회. 정렬 자체가 없다
     * jobName 지정     ji 가 구동 테이블(JOB_INST_UN 커버링) → je 는 FK 인덱스
     *                  → **Sort(filesort)**. 정렬 키가 구동 테이블에 없어서다
     * </pre>
     * 즉 <b>"인덱스가 필요 없다" 는 필터가 없을 때만 참이다.</b> 그래도 인덱스를 안 더한다 —
     * {@code CleanupJobConfig} 가 {@code batch.cleanup.metadata-keep-days}(30일)로 배치 메타를
     * 걷어내 이 테이블이 <b>수십 행 규모로 묶여 있다</b>. 실측에서도 filesort 입력이 2행이었다.
     * 보존 창을 크게 늘리거나 잡 수가 늘면 그때 다시 재고 판단한다.
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
            toLong(rs.getBigDecimal("READ_TOTAL")),
            toLong(rs.getBigDecimal("WRITE_TOTAL")));

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

    /**
     * SUM() 은 BigDecimal 로 온다. Long 으로 캐스팅하면 ClassCastException 이다.
     *
     * <p>Step 이 하나도 없으면 NULL 이다 — 시작조차 못 한 실행이 그 모양이라 그대로 넘긴다.
     */
    private static Long toLong(BigDecimal value) {
        return value == null ? null : value.longValue();
    }
}
