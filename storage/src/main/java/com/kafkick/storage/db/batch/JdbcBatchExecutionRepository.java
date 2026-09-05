// 배치 실행 이력을 관측 풀로 읽습니다.
package com.kafkick.storage.db.batch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kafkick.core.batch.BatchExecution;
import com.kafkick.core.batch.BatchStepExecution;
import com.kafkick.core.batch.FailureSummary;
import com.kafkick.core.batch.BatchExecutionRepository;

/**
 * <b>관측 풀 하나만 문다.</b> 이 어댑터에는 운영 풀 필드가 없다 —
 * {@code JdbcBenchmarkRunRepository} 는 전이·적재가 있어 둘을 갖지만 여기는 읽기뿐이다.
 * 필드가 아예 없으면 나중에 누가 여기에 UPDATE 를 넣으려다 <b>쓸 것이 없어 멈춘다.</b>
 *
 * <p><b>{@code BATCH_JOB_INSTANCE} 를 조인해야 잡 이름이 나온다.</b>
 * {@code BATCH_JOB_EXECUTION} 에는 {@code JOB_INSTANCE_ID} 만 있고 이름이 없다.
 * 조인을 빠뜨리면 "언제 무엇이" 중 <b>무엇이</b> 가 통째로 사라진다.
 *
 * <p><b>정렬을 {@code JOB_EXECUTION_ID} 로 잇는다.</b> {@code START_TIME} 은 nullable 이고
 * (시작 못 한 실행), MySQL 은 {@code DESC} 정렬에서 {@code NULL} 을 뒤로 보낸다 — 그러면
 * <b>시작도 못 한 실행이 목록 맨 뒤로 밀려</b> 첫 페이지에서 사라진다. 그 실행이야말로
 * 사람이 봐야 하는 것이다. 그래서 {@code CREATE_TIME}(NOT NULL)을 1순위로 두고 같은 값일 때
 * id 로 가른다.
 *
 * <h2>관측 풀을 켠 모듈에서만 만들어진다</h2>
 *
 * <p>storage 를 얹은 모듈은 이 클래스를 자동으로 스캔한다. 조건이 없으면 관측을 끄고 뜨는
 * batch 가 이 빈을 만들려다 {@code @Qualifier("obs")} 대상을 못 찾아 <b>기동에서 죽는다</b> —
 * 실제로 {@code BatchObservationEscapeHatchTest} 가 그렇게 죽었다.
 * {@code JdbcBenchmarkRunRepository} 가 같은 이유로 같은 조건을 갖고 있다.
 *
 * <p>⚠️ 반대 방향 실패 — 관측을 끄면 이 빈이 없다. {@code BatchHistoryController} 는 이 빈을
 * {@code ObjectProvider} 로 늦게 받아, 없으면 <b>503 + {@code ADMIN-003}</b> 으로 답한다.
 * 생성자로 직접 받으면 그 환경에서 api 기동이 통째로 실패하고, 컨트롤러에 같은 조건을 달면
 * <b>404</b> 라 "기능 없음" 과 갈리지 않는다. 그 판단의 근거는 컨트롤러 javadoc 에 적었다.
 *
 * <p><b>테이블 이름을 대문자로 적는다.</b> {@code V2__batch_metadata.sql} 이 그렇게 만들었고,
 * MySQL 의 테이블 이름은 리눅스에서 대소문자를 가린다. 소문자로 적으면 로컬(macOS,
 * 기본 {@code lower_case_table_names=2})에서는 통과하고 <b>운영에서만 죽는다.</b>
 */
@Repository
@ConditionalOnProperty("observation.datasource.enabled")
public class JdbcBatchExecutionRepository implements BatchExecutionRepository {

    private static final String SELECT = """
            SELECT e.JOB_EXECUTION_ID,
                   i.JOB_NAME,
                   e.STATUS,
                   e.EXIT_CODE,
                   e.CREATE_TIME,
                   e.START_TIME,
                   e.END_TIME
              FROM BATCH_JOB_EXECUTION e
              JOIN BATCH_JOB_INSTANCE i ON i.JOB_INSTANCE_ID = e.JOB_INSTANCE_ID
            """;

    private static final String ORDER_AND_LIMIT =
            " ORDER BY e.CREATE_TIME DESC, e.JOB_EXECUTION_ID DESC LIMIT ?";

    private static final RowMapper<BatchExecution> MAPPER = (rs, rowNum) -> new BatchExecution(
            rs.getLong("JOB_EXECUTION_ID"),
            rs.getString("JOB_NAME"),
            rs.getString("STATUS"),
            rs.getString("EXIT_CODE"),
            instant(rs, "CREATE_TIME"),
            instant(rs, "START_TIME"),
            instant(rs, "END_TIME"));

    /** 조회 전용. 관측 풀이다. */
    private final JdbcTemplate observationJdbcTemplate;

    public JdbcBatchExecutionRepository(
            @Qualifier("obs") JdbcTemplate observationJdbcTemplate
    ) {
        this.observationJdbcTemplate = observationJdbcTemplate;
    }

    @Override
    public List<BatchExecution> findRecent(int limit) {
        return observationJdbcTemplate.query(SELECT + ORDER_AND_LIMIT, MAPPER, limit);
    }

    @Override
    public List<BatchExecution> findRecentByJobName(String jobName, int limit) {
        return observationJdbcTemplate.query(
                SELECT + " WHERE i.JOB_NAME = ?" + ORDER_AND_LIMIT, MAPPER, jobName, limit);
    }

    /**
     * <b>카운터 여덟 개를 그대로 읽는다.</b> 여기서 더하거나 해석하지 않는다 — 뜻의 주인이
     * Spring Batch 라, 이 계층이 해석을 넣으면 프레임워크가 뜻을 바꾸는 날 화면만 조용히
     * 틀린다.
     *
     * <p><b>{@code BATCH_JOB_INSTANCE} 조인이 없다.</b> 잡 이름은 상위 목록이 이미 준다.
     * 여기서 또 조인하면 같은 사실을 두 질의가 각자 읽는다.
     */
    private static final String SELECT_STEPS = """
            SELECT s.STEP_EXECUTION_ID,
                   s.JOB_EXECUTION_ID,
                   s.STEP_NAME,
                   s.STATUS,
                   s.EXIT_CODE,
                   s.EXIT_MESSAGE,
                   s.CREATE_TIME,
                   s.START_TIME,
                   s.END_TIME,
                   s.READ_COUNT,
                   s.WRITE_COUNT,
                   s.FILTER_COUNT,
                   s.COMMIT_COUNT,
                   s.ROLLBACK_COUNT,
                   s.READ_SKIP_COUNT,
                   s.PROCESS_SKIP_COUNT,
                   s.WRITE_SKIP_COUNT
              FROM BATCH_STEP_EXECUTION s
             WHERE s.JOB_EXECUTION_ID = ?
             ORDER BY s.STEP_EXECUTION_ID
            """;

    /**
     * 카운터 컬럼은 <b>nullable 이다</b>(공식 스키마의 {@code BIGINT}, NOT NULL 이 아니다).
     * {@code rs.getLong} 은 {@code NULL} 을 0 으로 돌려주는데, 여기서는 <b>그것이 맞다</b> —
     * 아직 아무것도 안 읽은 스텝의 read count 는 0 이다.
     */
    private static final RowMapper<BatchStepExecution> STEP_MAPPER =
            (rs, rowNum) -> new BatchStepExecution(
                    rs.getLong("STEP_EXECUTION_ID"),
                    rs.getLong("JOB_EXECUTION_ID"),
                    rs.getString("STEP_NAME"),
                    rs.getString("STATUS"),
                    rs.getString("EXIT_CODE"),
                    // **원문을 그대로 싣지 않는다.** 스택트레이스가 통째로 들어 있고
                    // 이 API 에는 사용자 인증이 없다 — 판단 근거는 FailureSummary 에 있다.
                    FailureSummary.of(rs.getString("EXIT_MESSAGE")),
                    instant(rs, "CREATE_TIME"),
                    instant(rs, "START_TIME"),
                    instant(rs, "END_TIME"),
                    rs.getLong("READ_COUNT"),
                    rs.getLong("WRITE_COUNT"),
                    rs.getLong("FILTER_COUNT"),
                    rs.getLong("COMMIT_COUNT"),
                    rs.getLong("ROLLBACK_COUNT"),
                    rs.getLong("READ_SKIP_COUNT"),
                    rs.getLong("PROCESS_SKIP_COUNT"),
                    rs.getLong("WRITE_SKIP_COUNT"));

    @Override
    public List<BatchStepExecution> findSteps(long jobExecutionId) {
        return observationJdbcTemplate.query(SELECT_STEPS, STEP_MAPPER, jobExecutionId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }
}
