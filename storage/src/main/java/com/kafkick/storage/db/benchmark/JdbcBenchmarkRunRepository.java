package com.kafkick.storage.db.benchmark;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import com.kafkick.core.benchmark.BenchmarkArchiveStatus;
import com.kafkick.core.benchmark.BenchmarkErrorCode;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.BenchmarkRunStatus;
import com.kafkick.core.benchmark.BenchmarkRunType;
import com.kafkick.core.benchmark.BenchmarkTopology;
import com.kafkick.core.benchmark.ClientLoadSummary;
import com.kafkick.core.benchmark.LoadProfile;
import com.kafkick.core.benchmark.LoadToolMeta;
import com.kafkick.core.benchmark.ServerLoadSummary;
import com.kafkick.core.benchmark.StartBenchmarkRunCommand;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.exception.BusinessException;

/**
 * {@code benchmark_runs} JDBC 어댑터.
 *
 * <h2>읽기와 쓰기의 풀이 다르다</h2>
 *
 * 조회는 관측 풀({@code @Qualifier("obs")}), 전이·적재는 운영 풀이다. 하나의 빈으로 받으면
 * read-only 풀에서 INSERT 가 터지고, 관측 풀이 SELECT 전용 계정으로 바뀌면 그때는 권한 오류로
 * 더 시끄럽게 깨진다. 두 템플릿을 <b>필드 이름으로도</b> 갈라 둔다 — 한쪽을 다른 쪽 자리에 쓰는
 * 실수가 컴파일에서는 안 잡히기 때문이다.
 *
 * <h2>관측 풀을 켠 모듈에서만 만들어진다</h2>
 *
 * storage 를 얹은 모듈은 이 클래스를 자동으로 스캔한다. 그대로 두면 관측을 쓰지 않는 batch 도
 * 이 빈을 만들려다 {@code @Qualifier("obs")} 대상을 못 찾아 <b>기동에서 죽는다</b> — 관측을 위한
 * 코드가 관측과 무관한 프로세스를 멈춰 세우는 셈이라, 관측 풀을 나눈 이유와 정면으로 어긋난다.
 * {@code ObservationDataSourceConfig} · {@code DomainObservationConfig} 가 같은 스위치를 쓴다.
 *
 * <p>관측 풀은 크기가 2다. 회차 조회가 무거우면 그 둘을 다 먹고 도메인 Gauge 수집이 멈춘다 —
 * 이 테이블이 수십 행뿐이라 지금은 문제가 아니지만, 목록 조회에 {@code limit} 을 강제하는 이유다.
 *
 * <h2>전이는 조건부 UPDATE 한 문장이다</h2>
 *
 * 현재 상태를 WHERE 절에 넣고 갱신 행 수를 돌려준다. "읽고 판단하고 쓰기" 로 나누면 api 가 N 대라
 * 두 인스턴스가 같은 창을 통과할 수 있다. {@code storage} 에는 {@code spring-tx} 가 있지만
 * 트랜잭션으로 묶어도 그 창은 남는다 — 원자성은 문장 하나에서 나온다.
 */
@Repository
@ConditionalOnProperty("observation.datasource.enabled")
public class JdbcBenchmarkRunRepository implements BenchmarkRunRepository {

    private static final String INSERT = """
            INSERT INTO benchmark_runs (
                run_key, run_type, scenario_code,
                engine_version, release_stage, queue_mode, coupon_id,
                run_status, archive_status, started_at, requested_by,
                app_replicas, available_processors, cpu_millicores_total, memory_mb_total,
                tomcat_workers_total, tomcat_max_connections, tomcat_accept_count,
                hikari_pool_total, mysql_max_connections,
                offered_rps, load_hold_seconds, observation_hold_seconds,
                stock_total, generator_idle_rtt_millis,
                load_tool, load_tool_version, load_script_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING', 'NONE', ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT = "SELECT * FROM benchmark_runs";

    /** {@code Check constraint 'ck_run_time_order' is violated} 에서 이름만 뽑는다. */
    private static final Pattern CONSTRAINT_NAME =
            Pattern.compile("[Cc]heck constraint '([^']+)' is violated");

    private static final RowMapper<BenchmarkRun> MAPPER = (rs, rowNumber) -> new BenchmarkRun(
            rs.getLong("id"),
            rs.getString("run_key"),
            BenchmarkRunType.valueOf(rs.getString("run_type")),
            rs.getString("scenario_code"),
            EngineVersion.valueOf(rs.getString("engine_version")),
            ReleaseStage.valueOf(rs.getString("release_stage")),
            QueueMode.valueOf(rs.getString("queue_mode")),
            nullableLong(rs, "coupon_id"),
            BenchmarkRunStatus.valueOf(rs.getString("run_status")),
            BenchmarkArchiveStatus.valueOf(rs.getString("archive_status")),
            rs.getString("archive_failure_reason"),
            instant(rs, "started_at"),
            instant(rs, "load_stopped_at"),
            instant(rs, "observation_stopped_at"),
            instant(rs, "finalized_at"),
            rs.getString("requested_by"),
            rs.getString("load_stop_reason"),
            new BenchmarkTopology(
                    rs.getInt("app_replicas"),
                    rs.getInt("available_processors"),
                    nullableInt(rs, "cpu_millicores_total"),
                    nullableInt(rs, "memory_mb_total"),
                    rs.getInt("tomcat_workers_total"),
                    nullableInt(rs, "tomcat_max_connections"),
                    nullableInt(rs, "tomcat_accept_count"),
                    rs.getInt("hikari_pool_total"),
                    rs.getInt("mysql_max_connections")),
            new LoadProfile(
                    rs.getInt("offered_rps"),
                    rs.getInt("load_hold_seconds"),
                    rs.getInt("observation_hold_seconds"),
                    nullableInt(rs, "stock_total"),
                    nullableDouble(rs, "generator_idle_rtt_millis")),
            new LoadToolMeta(
                    rs.getString("load_tool"),
                    rs.getString("load_tool_version"),
                    rs.getString("load_script_hash")),
            clientSummary(rs),
            serverSummary(rs),
            nullableLong(rs, "observed_lag_total"));

    /**
     * 전이·적재 전용. 운영 풀이다.
     */
    private final JdbcTemplate writeJdbcTemplate;

    /**
     * 조회 전용. 관측 풀이다.
     */
    private final JdbcTemplate observationJdbcTemplate;

    public JdbcBenchmarkRunRepository(
            JdbcTemplate jdbcTemplate,
            @Qualifier("obs") JdbcTemplate observationJdbcTemplate
    ) {
        this.writeJdbcTemplate = jdbcTemplate;
        this.observationJdbcTemplate = observationJdbcTemplate;
    }

    @Override
    public long open(StartBenchmarkRunCommand command, Instant startedAt) {
        BenchmarkTopology topology = command.topology();
        LoadProfile profile = command.loadProfile();
        LoadToolMeta tool = command.toolMeta();
        try {
            GeneratedKeyHolder keys = new GeneratedKeyHolder();
            writeJdbcTemplate.update(connection -> {
                PreparedStatement statement =
                        connection.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
                int index = 1;
                statement.setString(index++, command.runKey());
                statement.setString(index++, command.runType().name());
                statement.setString(index++, command.scenarioCode());
                statement.setString(index++, command.engineVersion().name());
                statement.setString(index++, command.releaseStage().name());
                statement.setString(index++, command.queueMode().name());
                setNullableLong(statement, index++, command.couponId());
                statement.setTimestamp(index++, timestamp(startedAt));
                statement.setString(index++, command.requestedBy());
                statement.setInt(index++, topology.appReplicas());
                statement.setInt(index++, topology.availableProcessors());
                setNullableInt(statement, index++, topology.cpuMillicoresTotal());
                setNullableInt(statement, index++, topology.memoryMbTotal());
                statement.setInt(index++, topology.tomcatWorkersTotal());
                setNullableInt(statement, index++, topology.tomcatMaxConnections());
                setNullableInt(statement, index++, topology.tomcatAcceptCount());
                statement.setInt(index++, topology.hikariPoolTotal());
                statement.setInt(index++, topology.mysqlMaxConnections());
                statement.setInt(index++, profile.offeredRps());
                statement.setInt(index++, profile.loadHoldSeconds());
                statement.setInt(index++, profile.observationHoldSeconds());
                setNullableInt(statement, index++, profile.stockTotal());
                setNullableDouble(statement, index++, profile.generatorIdleRttMillis());
                statement.setString(index++, tool.tool());
                statement.setString(index++, tool.version());
                statement.setString(index, tool.scriptHash());
                return statement;
            }, keys);
            Number key = keys.getKey();
            if (key == null) {
                throw new IllegalStateException("benchmark_runs 생성 키를 받지 못했다: " + command.runKey());
            }
            return key.longValue();
        } catch (DuplicateKeyException exception) {
            throw conflict(command.runKey(), exception);
        } catch (DataAccessException exception) {
            throw valueRejected(exception);
        }
    }

    /**
     * 어느 유니크 키에 걸렸는지로 원인을 가른다.
     *
     * <p>둘을 하나로 뭉치면 회차 중에 "start 를 두 번 눌렀다" 와 "앞 회차가 아직 안 끝났다" 가
     * 같은 메시지로 보인다. 조치가 정반대라(키를 바꾼다 / 앞 회차를 닫는다) 구분이 필요하다.
     *
     * <p>제약 이름으로 판정한다 — MySQL 의 중복 키 메시지는 키 이름을 담고 있다. 이름이 안
     * 보이면 회차 키 쪽으로 본다: 두 원인 중 사람이 먼저 확인할 것이 그쪽이고,
     * {@code uk_run_running} 은 애초에 진행 중인 회차가 있을 때만 걸린다.
     */
    private static BusinessException conflict(String runKey, DuplicateKeyException exception) {
        String message = String.valueOf(exception.getMostSpecificCause().getMessage());
        BenchmarkErrorCode code = message.contains("uk_run_running")
                ? BenchmarkErrorCode.RUN_ALREADY_RUNNING
                : BenchmarkErrorCode.RUN_KEY_DUPLICATED;
        return new BusinessException(code, "runKey=" + runKey, exception);
    }

    @Override
    public Optional<BenchmarkRun> findById(long id) {
        return observationJdbcTemplate
                .query(SELECT + " WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    @Override
    public Optional<BenchmarkRun> findByRunKey(String runKey) {
        return observationJdbcTemplate
                .query(SELECT + " WHERE run_key = ?", MAPPER, runKey).stream().findFirst();
    }

    @Override
    public Optional<BenchmarkRun> findRunning() {
        return observationJdbcTemplate
                .query(SELECT + " WHERE run_status = 'RUNNING'", MAPPER).stream().findFirst();
    }

    @Override
    public List<BenchmarkRun> findRecent(int limit) {
        return observationJdbcTemplate.query(
                SELECT + " ORDER BY started_at DESC, id DESC LIMIT ?", MAPPER, limit);
    }

    @Override
    public boolean markLoadStopped(long id, Instant loadStoppedAt, String reason) {
        return update("""
                UPDATE benchmark_runs
                   SET run_status = 'LOAD_STOPPED', load_stopped_at = ?, load_stop_reason = ?
                 WHERE id = ? AND run_status = 'RUNNING'
                """, timestamp(loadStoppedAt), reason, id);
    }

    @Override
    public boolean markObserved(long id, Instant observationStoppedAt, long observedLagTotal) {
        return update("""
                UPDATE benchmark_runs
                   SET run_status = 'OBSERVED', observation_stopped_at = ?, observed_lag_total = ?
                 WHERE id = ? AND run_status = 'LOAD_STOPPED'
                """, timestamp(observationStoppedAt), observedLagTotal, id);
    }

    /**
     * 공식 요약 유무를 WHERE 절이 함께 본다. 서비스가 먼저 읽어 판단하면 그 사이에 요약이
     * 지워질 수 있고, 그러면 DB 의 {@code ck_run_final_summary} 가 대신 거부하는데 그 예외는
     * 사람이 읽을 수 있는 형태가 아니다.
     */
    @Override
    public boolean markFinalized(long id, Instant finalizedAt) {
        return update("""
                UPDATE benchmark_runs
                   SET run_status = 'FINALIZED', finalized_at = ?
                 WHERE id = ? AND run_status = 'OBSERVED' AND client_measured_at IS NOT NULL
                """, timestamp(finalizedAt), id);
    }

    @Override
    public boolean updateClientSummary(long id, ClientLoadSummary summary) {
        return update("""
                UPDATE benchmark_runs
                   SET client_request_count = ?, client_failure_count = ?, client_dropped_iterations = ?,
                       client_tps = ?, client_p95_millis = ?, client_p99_millis = ?, client_measured_at = ?
                 WHERE id = ? AND run_status <> 'FINALIZED'
                """,
                summary.requestCount(), summary.failureCount(), summary.droppedIterations(),
                summary.tps(), summary.p95Millis(), summary.p99Millis(),
                timestamp(summary.measuredAt()), id);
    }

    @Override
    public boolean updateServerSummary(long id, ServerLoadSummary summary) {
        return update("""
                UPDATE benchmark_runs
                   SET server_request_count = ?, server_failure_count = ?,
                       server_tps = ?, server_p95_millis = ?, server_p99_millis = ?, server_measured_at = ?
                 WHERE id = ? AND run_status <> 'FINALIZED'
                """,
                summary.requestCount(), summary.failureCount(),
                summary.tps(), summary.p95Millis(), summary.p99Millis(),
                timestamp(summary.measuredAt()), id);
    }

    /**
     * 회차 상태를 WHERE 절에 넣지 않는다. archive 는 회차 수명주기와 독립이라 확정된 회차에도
     * 다시 돌 수 있어야 한다 — 그게 Prometheus 에 원본을 남겨 두는 이유다.
     */
    @Override
    public boolean updateArchiveStatus(long id, BenchmarkArchiveStatus status, String failureReason) {
        return update("""
                UPDATE benchmark_runs SET archive_status = ?, archive_failure_reason = ? WHERE id = ?
                """, status.name(), failureReason, id);
    }

    @Override
    public boolean claimFailedArchive(long id) {
        return update("""
                UPDATE benchmark_runs
                SET archive_status = 'NONE', archive_failure_reason = NULL
                WHERE id = ? AND archive_status = 'FAILED'
                """, id);
    }

    /**
     * 조건부 UPDATE 한 문장을 돌리고, 갱신 행 수가 1 인지 돌려준다.
     *
     * <p><b>상태 조건과 값 조건은 막는 주체가 다르다.</b> 상태는 WHERE 절이 보고 0행으로 알리지만,
     * 값은 DB CHECK 만 본다. 번역하지 않으면 {@code DataIntegrityViolationException} 이 그대로 올라가
     * {@code GlobalExceptionHandler} 의 {@code @ExceptionHandler(Exception.class)} 에서 500 이 된다 —
     * 회차 도중에 원인이 안 적힌 서버 오류가 뜬다.
     *
     * <p>서비스와 값 객체를 지나면 대부분 여기 안 걸린다. 그런데 <b>안 걸린다고 단정할 수 없는 경로가
     * 하나 있다</b> — 회차를 연 인스턴스와 부하를 끊는 인스턴스가 다르면 시계 차이로
     * {@code load_stopped_at < started_at} 이 만들어져 {@code ck_run_time_order} 에 걸린다.
     * 앱이 미리 못 막는 조건이라 여기서 받는 게 맞다.
     */
    private boolean update(String sql, Object... args) {
        try {
            return 1 == writeJdbcTemplate.update(sql, args);
        } catch (DataAccessException exception) {
            throw valueRejected(exception);
        }
    }

    /**
     * 어떤 제약이 거부했는지만 남긴다. <b>값은 넣지 않는다</b> — detail 은 로그로 나가고,
     * 거기에는 회차를 연 사람의 식별자를 비롯해 원문 값이 섞여 있다.
     */
    private static RuntimeException valueRejected(DataAccessException exception) {
        String constraint = constraintName(exception);
        if (constraint == null) {
            // 제약 이름이 없으면 CHECK 위반이 아니다 — 커넥션 끊김·타임아웃 같은 진짜 장애다.
            // 그런 것까지 도메인 오류로 바꾸면 인프라 장애가 "값이 잘못됐다" 로 보고되고,
            // 재시도하면 되는 상황과 값을 고쳐야 하는 상황이 한 코드로 뭉쳐진다.
            return exception;
        }
        return new BusinessException(BenchmarkErrorCode.RUN_VALUE_REJECTED,
                "constraint=" + constraint, exception);
    }

    /**
     * MySQL 의 CHECK 위반 메시지는 {@code Check constraint 'ck_run_time_order' is violated} 형태다.
     *
     * <p><b>예외 타입으로 가르지 않는다.</b> MySQL 의 CHECK 위반(3819)은 Spring 의 MySQL 에러코드
     * 매핑에 없어서 {@code DataIntegrityViolationException} 이 아니라
     * {@code UncategorizedSQLException} 으로 올라온다(실측). 타입으로 잡으려다 놓치면 그대로 500 이 된다.
     *
     * <p>이름을 못 찾으면 {@code null} 이다. 지어내지 않는다 — 없는 제약 이름이 로그에 남으면
     * 그걸 찾느라 시간을 쓴다.
     *
     * @param exception 저장소가 던진 예외
     * @return 위반된 CHECK 제약 이름; CHECK 위반이 아니면 null
     */
    private static String constraintName(DataAccessException exception) {
        Throwable cause = exception.getMostSpecificCause();
        Matcher matcher = CONSTRAINT_NAME.matcher(String.valueOf(cause.getMessage()));
        return matcher.find() ? matcher.group(1) : null;
    }

    /**
     * 컬럼이 {@code datetime(6)} 이라 마이크로초까지만 담긴다. MySQL 은 초과 정밀도를 버리지 않고
     * <b>반올림</b>하므로, 나노초를 그대로 주면 저장된 값이 준 값보다 나중이 될 수 있다.
     * 그러면 시각 순서 CHECK 가 방금 쓴 행에서 거짓으로 걸린다 — {@code issue_attempts} 의
     * Consumer 가 같은 이유로 잘라 넣는다.
     */
    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant.truncatedTo(ChronoUnit.MICROS));
    }

    private static void setNullableLong(PreparedStatement statement, int index, Long value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.BIGINT);
        } else {
            statement.setLong(index, value);
        }
    }

    private static void setNullableInt(PreparedStatement statement, int index, Integer value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }

    private static void setNullableDouble(PreparedStatement statement, int index, Double value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.DECIMAL);
        } else {
            statement.setBigDecimal(index, BigDecimal.valueOf(value));
        }
    }


    /**
     * 요약은 통째로 있거나 통째로 없다({@code ck_run_*_summary_atomic}). 그래서 측정 시각 하나로
     * 유무를 가른다 — 값 컬럼으로 가르면 "TPS 가 0 인 회차" 가 요약 없음으로 읽힌다.
     */
    private static ClientLoadSummary clientSummary(ResultSet rs) throws SQLException {
        Instant measuredAt = instant(rs, "client_measured_at");
        if (measuredAt == null) {
            return null;
        }
        return new ClientLoadSummary(
                rs.getLong("client_request_count"),
                rs.getLong("client_failure_count"),
                rs.getLong("client_dropped_iterations"),
                rs.getDouble("client_tps"),
                rs.getDouble("client_p95_millis"),
                rs.getDouble("client_p99_millis"),
                measuredAt);
    }

    private static ServerLoadSummary serverSummary(ResultSet rs) throws SQLException {
        Instant measuredAt = instant(rs, "server_measured_at");
        if (measuredAt == null) {
            return null;
        }
        return new ServerLoadSummary(
                rs.getLong("server_request_count"),
                rs.getLong("server_failure_count"),
                rs.getDouble("server_tps"),
                rs.getDouble("server_p95_millis"),
                rs.getDouble("server_p99_millis"),
                measuredAt);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static Double nullableDouble(ResultSet rs, String column) throws SQLException {
        BigDecimal value = rs.getBigDecimal(column);
        return value == null ? null : value.doubleValue();
    }
}
