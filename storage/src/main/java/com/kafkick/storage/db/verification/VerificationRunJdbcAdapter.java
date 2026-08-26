package com.kafkick.storage.db.verification;

import static com.kafkick.storage.db.verification.ColumnValues.toEnum;
import static com.kafkick.storage.db.verification.ColumnValues.toName;

import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * JPA 엔티티를 만들지 않고 JdbcClient 로 간다.
 *
 * <p>검증 배치는 300만~534만 행을 읽고 결과만 쓴다. 영속성 컨텍스트가 할 일이 없고,
 * 규칙은 전부 집계 SQL 이라 엔티티를 두면 관리 비용만 는다.
 */
@Repository
public class VerificationRunJdbcAdapter implements VerificationRunRepository {

    /**
     * {@code VerifyJobConfig.JOB_NAME} 과 같아야 한다. 의존 방향이 반대라
     * ({@code batch} 가 {@code storage} 를 보지 그 반대가 아니다) 문자열로 둔다 —
     * 일치는 {@code VerificationRunJdbcAdapterTest.ignoresOtherJobs} 가 지킨다 — 이름이 어긋나면 배치 메타 축이 0을 준다.
     */
    private static final String VERIFY_JOB_NAME = "verifyJob";

    private static final String INSERT = """
            INSERT INTO verification_runs
                (as_of, from_ts, scope, dataset, attempt, finding_count, started_at)
            VALUES (:asOf, :fromTs, :scope, :dataset, :attempt, :findingCount, :startedAt)
            """;

    private static final String UPDATE = """
            UPDATE verification_runs
               SET verdict             = :verdict,
                   stats_status        = :statsStatus,
                   finding_count       = :findingCount,
                   findings_checksum   = :findingsChecksum,
                   dataset_fingerprint = :datasetFingerprint,
                   finished_at         = :finishedAt
             WHERE id = :id
            """;

    private static final String SELECT_BY_ID = """
            SELECT id, as_of, from_ts, scope, dataset, attempt,
                   verdict, stats_status, finding_count,
                   findings_checksum, dataset_fingerprint, started_at, finished_at,
                   seed_run_id
              FROM verification_runs
             WHERE id = :id
            """;

    private static final String SELECT_BY_PARAMS = """
            SELECT id, as_of, from_ts, scope, dataset, attempt,
                   verdict, stats_status, finding_count,
                   findings_checksum, dataset_fingerprint, started_at, finished_at,
                   seed_run_id
              FROM verification_runs
             WHERE as_of = :asOf AND dataset = :dataset
               AND scope = :scope AND attempt = :attempt
            """;

    /**
     * 닫힌 실행 중 가장 최근 것 하나.
     *
     * <p><b>{@code finished_at} 으로 고른다.</b> {@code started_at} 은 돌다 만 실행도 갖고
     * 있어서, 그것으로 고르면 판정 없는 행이 최신으로 올라온다.
     *
     * <p>동률이면 {@code id} 로 가른다 — 같은 초에 둘이 닫히면 순서가 실행마다 달라져
     * 지표가 흔들린다.
     *
     * <p><b>{@code origin = 'BATCH'} 가 없으면 시드가 심은 기준 행을 판정으로 읽는다.</b>
     * 시드는 완결된 과거 run 을 일부러 심는다 — CLEAN 은 {@code PASS}, CORRUPT 는
     * {@code FAIL} 과 정답 800행이다. 그것을 읽으면 <b>검증을 한 번도 안 돌려도 통과가
     * 나가고</b>, CORRUPT 쪽은 알림이 영원히 안 꺼진다. {@code rejectExistingRun} 이 같은
     * 함정을 이미 막고 있었는데 이 경로에는 그 방어가 없었다.
     *
     * <p><b>{@code verdict IS NOT NULL} 도 함께 건다.</b> 컬럼이 nullable 이라 닫혔는데
     * 판정이 없는 행이 있을 수 있고, 그것을 {@code FAIL} 로 접으면 가짜 알림이 뜬다 —
     * <i>"판정이 아니다"</i> 라는 뜻은 {@code finished_at} 축과 같다.
     */
    private static final String SELECT_LATEST_CLOSED = """
            SELECT id, as_of, from_ts, scope, dataset, attempt,
                   verdict, stats_status, finding_count,
                   findings_checksum, dataset_fingerprint, started_at, finished_at,
                   seed_run_id
              FROM verification_runs
             WHERE dataset = :dataset AND scope = :scope
               AND origin = 'BATCH'
               AND verdict IS NOT NULL
               AND finished_at IS NOT NULL
             ORDER BY finished_at DESC, id DESC
             LIMIT 1
            """;

    private static final RowMapper<VerificationRun> ROW_MAPPER = (rs, rowNum) -> VerificationRun.restore(
            rs.getLong("id"),
            rs.getObject("as_of", LocalDateTime.class),
            rs.getObject("from_ts", LocalDateTime.class),
            ScopeType.valueOf(rs.getString("scope")),
            DatasetType.valueOf(rs.getString("dataset")),
            rs.getInt("attempt"),
            toEnum(rs.getString("verdict"), VerdictType::valueOf),
            toEnum(rs.getString("stats_status"), StatsStatus::valueOf),
            rs.getInt("finding_count"),
            rs.getString("findings_checksum"),
            rs.getString("dataset_fingerprint"),
            rs.getObject("started_at", LocalDateTime.class),
            rs.getObject("finished_at", LocalDateTime.class),
            rs.getObject("seed_run_id", Long.class)
    );

    private final JdbcClient jdbcClient;

    public VerificationRunJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public VerificationRun save(VerificationRun run) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcClient.sql(INSERT)
                .param("asOf", run.asOf())
                .param("fromTs", run.fromTs())
                .param("scope", run.scope().name())
                .param("dataset", run.dataset().name())
                .param("attempt", run.attempt())
                .param("findingCount", run.findingCount())
                .param("startedAt", run.startedAt())
                .update(keyHolder);

        Number generated = keyHolder.getKey();
        if (generated == null) {
            // 여기만 BusinessException 이 아니다. 행이 없다는 뜻이 아니라 INSERT 가 성공했는데
            // 드라이버가 생성 키를 안 준 것이라, 업무 조건이 아니라 인프라 계약 위반이다.
            throw new IllegalStateException("검증 실행 ID가 생성되지 않았습니다.");
        }

        return VerificationRun.restore(
                generated.longValue(),
                run.asOf(), run.fromTs(), run.scope(), run.dataset(), run.attempt(),
                run.verdict(), run.statsStatus(), run.findingCount(),
                run.findingsChecksum(), run.datasetFingerprint(),
                run.startedAt(), run.finishedAt(), run.seedRunId()
        );
    }

    @Override
    public void update(VerificationRun run) {
        if (run.id() == null) {
            throw new IllegalArgumentException("갱신하려면 검증 실행 ID가 필요합니다.");
        }

        int updated = jdbcClient.sql(UPDATE)
                .param("verdict", toName(run.verdict()))
                .param("statsStatus", toName(run.statsStatus()))
                .param("findingCount", run.findingCount())
                .param("findingsChecksum", run.findingsChecksum())
                .param("datasetFingerprint", run.datasetFingerprint())
                .param("finishedAt", run.finishedAt())
                .param("id", run.id())
                .update();

        // save() 는 실패를 예외로 알리는데 update() 만 조용히 넘어가면, 정리 배치가 지운 run 을
        // 갱신하려 할 때 판정이 안 써졌는데도 잡이 COMPLETED 로 끝난다.
        if (updated != 1) {
            throw new BusinessException(
                    VerificationErrorCode.RUN_ROW_VANISHED,
                    "검증 실행을 갱신하지 못했습니다. id=" + run.id() + " 갱신된 행=" + updated);
        }
    }

    @Override
    public Optional<VerificationRun> findByParams(
            LocalDateTime asOf,
            DatasetType dataset,
            ScopeType scope,
            int attempt
    ) {
        return jdbcClient.sql(SELECT_BY_PARAMS)
                .param("asOf", asOf)
                .param("dataset", dataset.name())
                .param("scope", scope.name())
                .param("attempt", attempt)
                .query(ROW_MAPPER)
                .optional();
    }

    @Override
    public Optional<VerificationRun> findById(long id) {
        return jdbcClient.sql(SELECT_BY_ID)
                .param("id", id)
                .query(ROW_MAPPER)
                .optional();
    }

    @Override
    public Optional<VerificationRun> findLatestClosed(DatasetType dataset, ScopeType scope) {
        return jdbcClient.sql(SELECT_LATEST_CLOSED)
                .param("dataset", dataset.name())
                .param("scope", scope.name())
                .query(ROW_MAPPER)
                .optional();
    }

    @Override
    public void recordComparedManifest(long runId, long seedRunId) {
        int updated = jdbcClient.sql("""
                        UPDATE verification_runs SET seed_run_id = :seedRunId WHERE id = :id
                        """)
                .param("seedRunId", seedRunId)
                .param("id", runId)
                .update();

        if (updated != 1) {
            throw new BusinessException(
                    VerificationErrorCode.RUN_ROW_VANISHED,
                    "대조한 정답 묶음을 기록하지 못했습니다. runId=" + runId + " 갱신행=" + updated);
        }
    }

    @Override
    public void updateStatsStatus(long runId, StatsStatus status) {
        int updated = jdbcClient.sql("""
                        UPDATE verification_runs SET stats_status = :statsStatus WHERE id = :id
                        """)
                .param("statsStatus", status.name())
                .param("id", runId)
                .update();

        if (updated != 1) {
            throw new BusinessException(
                    VerificationErrorCode.RUN_ROW_VANISHED,
                    "통계 상태를 갱신하지 못했습니다. runId=" + runId + " 갱신행=" + updated);
        }
    }

    @Override
    public int nextAttempt(LocalDateTime asOf, DatasetType dataset, ScopeType scope) {
        // 두 소스를 함께 본다.
        //
        // verification_runs 는 startRunStep 이 **가드 여덟을 통과한 뒤에야** INSERT 한다.
        // 반면 BATCH_JOB_INSTANCE 는 잡이 시작하는 순간 만들어진다. 그래서 가드에 걸려
        // 죽은 시도는 앞 테이블에 흔적이 없고 뒤 테이블에만 남는다.
        //
        // 앞만 보면 그 번호를 다시 주는데, verifyJob 은 preventRestart 라 같은 파라미터
        // 조합을 거절한다 — **몇 번을 눌러도 400 이고 자기 치유가 없다.** attempt 를 손으로
        // 넣지 않게 하려고 만든 것이 오히려 트리거를 영구히 막는다. 실측으로 확인했다.
        //
        // 배치 메타는 값을 전부 문자열로 저장한다. 그래서 두 번 정규화한다.
        //
        // attempt 는 숫자로 캐스팅하고, asOf 는 **시각으로** 캐스팅한다 — 문자열로 비교하면
        // 안 된다. LocalDateTime.toString() 은 초가 0이면 생략해 "2026-03-01T09:00" 을 주는데
        // 배치는 언제나 "2026-03-01T09:00:00" 으로 적는다. 같은 시각인데 문자열이 다르다.
        // 실측으로 확인했다.
        return jdbcClient.sql("""
                        SELECT GREATEST(
                                 (SELECT COALESCE(MAX(attempt), 0)
                                    FROM verification_runs
                                   WHERE as_of = :asOf AND dataset = :dataset AND scope = :scope),
                                 (SELECT COALESCE(MAX(CAST(a.PARAMETER_VALUE AS UNSIGNED)), 0)
                                    FROM BATCH_JOB_EXECUTION_PARAMS a
                                    JOIN BATCH_JOB_EXECUTION e
                                      ON e.JOB_EXECUTION_ID = a.JOB_EXECUTION_ID
                                    JOIN BATCH_JOB_INSTANCE i
                                      ON i.JOB_INSTANCE_ID = e.JOB_INSTANCE_ID
                                     AND i.JOB_NAME = :jobName
                                    JOIN BATCH_JOB_EXECUTION_PARAMS o
                                      ON o.JOB_EXECUTION_ID = a.JOB_EXECUTION_ID
                                     AND o.PARAMETER_NAME = 'asOf'
                                    JOIN BATCH_JOB_EXECUTION_PARAMS d
                                      ON d.JOB_EXECUTION_ID = a.JOB_EXECUTION_ID
                                     AND d.PARAMETER_NAME = 'dataset'
                                    JOIN BATCH_JOB_EXECUTION_PARAMS s
                                      ON s.JOB_EXECUTION_ID = a.JOB_EXECUTION_ID
                                     AND s.PARAMETER_NAME = 'scope'
                                   WHERE a.PARAMETER_NAME = 'attempt'
                                     AND CAST(REPLACE(o.PARAMETER_VALUE, 'T', ' ')
                                              AS DATETIME(6)) = :asOf
                                     AND d.PARAMETER_VALUE = :dataset
                                     AND s.PARAMETER_VALUE = :scope)
                               ) + 1
                        """)
                .param("asOf", asOf)
                // 잡 이름을 안 보면 질문이 "누구든 그 네 이름의 파라미터를 그 값으로
                // 쓴 적 있나" 로 넓어진다. 지금 expireJob 은 asOf 하나만 써서 우연히
                // 안 걸릴 뿐이고, 거기에 dataset·scope·attempt 가 붙는 날 뒤섞인다.
                .param("jobName", VERIFY_JOB_NAME)
                .param("dataset", dataset.name())
                .param("scope", scope.name())
                .query(Integer.class)
                .single();
    }
}
