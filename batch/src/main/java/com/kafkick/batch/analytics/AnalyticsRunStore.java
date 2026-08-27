package com.kafkick.batch.analytics;

import static com.kafkick.batch.analytics.AnalyticsAggregateReader.utc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.batch.analytics.AnalyticsAggregateReader.DailyRow;
import com.kafkick.batch.analytics.AnalyticsAggregateReader.HourlyRow;
import com.kafkick.batch.analytics.AnalyticsAggregateReader.StatusRow;

/**
 * 회차 이력과 집계 행을 <b>운영 풀</b>로 쓴다. 관측 계정은 SELECT 만 가지므로 쓰기는 여기뿐이다.
 *
 * <p>기준 시각({@code as_of})과 축 상태도 여기서 읽는다 — {@code analytics_runs} 는 이 배치가
 * 소유한 표라 관측 계정에 열어 줄 이유가 없다.
 */
public class AnalyticsRunStore {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsRunStore.class);

    /** {@code failure_reason} 은 varchar(500) 이다. 넘치면 STRICT 모드에서 회차 마감이 죽는다. */
    private static final int REASON_LIMIT = 500;

    /**
     * 이만큼 매달린 {@code IN_PROGRESS} 회차는 죽은 것으로 본다.
     *
     * <p>크론이 1시간인데 한 회차는 걸음 상한(20) × 축 3개 × 문장 상한(4초)이라 넉넉잡아 몇 분이다.
     * 6시간이면 정상 회차와 겹칠 수 없으면서 같은 날 안에 정리된다.
     */
    private static final Duration ABANDONED_AFTER = Duration.ofHours(6);

    /** 한 번에 보내는 행 수. 아카이브의 500행 청크와 같은 단위다. */
    private static final int BATCH_CHUNK = 500;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public AnalyticsRunStore(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * 앞서 죽은 회차를 거둔다. 프로세스가 집계 도중 종료되면 그 행이 {@code IN_PROGRESS} 로
     * 영구히 남아, 나중에 이력을 보는 사람이 "지금도 도는 중" 으로 오독한다.
     *
     * <p>정확성에는 무해하다 — 축 상태가 그 회차의 진행분을 이미 들고 있고, 워터마크도 축 상태만
     * 본다. 여기서 고치는 것은 <b>진단</b>이다.
     *
     * <p>⚠️ <b>살아 있을 수 있는 회차는 건드리지 않는다.</b> 겹쳐 뜬 다른 컨테이너의 진행 중인
     * 회차까지 실패로 적으면, 그쪽이 마감할 때 사유가 남아 있어 CHECK 에 걸린다. 그래서
     * {@link #ABANDONED_AFTER} 만큼 매달린 것만 거둔다.
     */
    private void reapAbandonedRuns(Instant now) {
        int reaped = jdbcTemplate.update(
                "UPDATE analytics_runs SET status = 'FAILED',"
                        + " failure_reason = '앞선 회차가 마감되지 않았다(프로세스 종료 추정)'"
                        + " WHERE status = 'IN_PROGRESS' AND started_at < ?",
                utc(now.minus(ABANDONED_AFTER)));
        if (reaped > 0) {
            log.warn("analytics 마감되지 않은 회차를 거뒀다: count={}", reaped);
        }
    }

    public long openRun(Instant asOf, Instant startedAt) {
        reapAbandonedRuns(startedAt);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO analytics_runs (as_of, started_at, status) VALUES (?, ?, 'IN_PROGRESS')",
                    PreparedStatement.RETURN_GENERATED_KEYS);
            statement.setObject(1, utc(asOf));
            statement.setObject(2, utc(startedAt));
            return statement;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) {
            throw new IllegalStateException("analytics_runs 회차 id 를 받지 못했다.");
        }
        return key.longValue();
    }

    /**
     * 축을 <b>어디까지 셌나</b>. 다음 회차는 여기서부터 이어 간다.
     *
     * <p><b>축마다 따로</b> 본다 — 한 축만 실패한 회차 뒤에 회차 단위 기준을 쓰면, 성공했던 축까지
     * 그 구간을 통째로 다시 훑는다.
     *
     * <p>{@code as_of} 가 아니라 {@code aggregated_through} 를 읽는다. 밀린 구간을 여러 걸음에
     * 나눠 따라잡는 동안 둘은 갈린다 — {@code as_of} 를 읽으면 아직 도달하지 못한 지점을
     * "다 셌다" 로 읽어 그 사이 구간을 <b>영영</b> 건너뛴다.
     *
     * @return 아직 한 번도 성공한 적 없으면 {@link Instant#EPOCH}
     */
    public Instant watermark(AnalyticsAxis axis) {
        LocalDateTime latest = jdbcTemplate.queryForObject(
                "SELECT MAX(" + axis.aggregatedThroughColumn() + ") FROM analytics_runs"
                        + " WHERE " + axis.statusColumn() + " = 'AVAILABLE'",
                LocalDateTime.class);
        return latest == null ? Instant.EPOCH : latest.toInstant(ZoneOffset.UTC);
    }

    /**
     * 축의 행 쓰기와 축 상태 갱신을 <b>한 트랜잭션</b>으로 묶는다.
     *
     * <p>나누면 행은 들어갔는데 축이 {@code PENDING} 인 회차가 생긴다. 값은 다음 회차가 맞추지만
     * A 는 그동안 계속 PENDING 을 본다 — 집계는 돌고 있는데 화면은 미집계다.
     */
    public int writeDaily(
            long runId, List<DailyRow> rows, Instant completedAt, Instant aggregatedThrough) {
        return inTransaction(runId, AnalyticsAxis.MONTHLY_TREND, completedAt, aggregatedThrough, () -> batch("""
                INSERT INTO analytics_daily_issues
                    (issue_date, coupon_id, brand_id, issue_count, run_id)
                VALUES (?, ?, ?, ?, ?) AS new
                ON DUPLICATE KEY UPDATE
                    brand_id = new.brand_id, issue_count = new.issue_count, run_id = new.run_id""",
                rows, (statement, row) -> {
                    statement.setObject(1, row.date());
                    statement.setLong(2, row.couponId());
                    statement.setLong(3, row.brandId());
                    statement.setLong(4, row.issueCount());
                    statement.setLong(5, runId);
                }));
    }

    public int writeHourly(
            long runId, List<HourlyRow> rows, Instant completedAt, Instant aggregatedThrough) {
        return inTransaction(runId, AnalyticsAxis.HOURLY_HEATMAP, completedAt, aggregatedThrough, () -> batch("""
                INSERT INTO analytics_hourly_issues
                    (issue_date, issue_hour, coupon_id, brand_id, issue_count, run_id)
                VALUES (?, ?, ?, ?, ?, ?) AS new
                ON DUPLICATE KEY UPDATE
                    brand_id = new.brand_id, issue_count = new.issue_count, run_id = new.run_id""",
                rows, (statement, row) -> {
                    statement.setObject(1, row.date());
                    statement.setInt(2, row.hour());
                    statement.setLong(3, row.couponId());
                    statement.setLong(4, row.brandId());
                    statement.setLong(5, row.issueCount());
                    statement.setLong(6, runId);
                }));
    }

    /**
     * 상태 축은 회차를 키에 넣어 <b>누적</b>한다(A 확정). 조회는 버킷별 최신 회차 하나를 고른다.
     * 안 바뀐 버킷은 여기 들어오지 않으므로 누적량은 실제 변경량을 따른다.
     *
     * <p><b>회차 안에서는 덮어쓴다.</b> 한 회차가 밀린 구간을 여러 걸음에 나눠 따라잡으므로 같은
     * 버킷이 두 걸음에 걸쳐 나올 수 있는데, 그때는 <b>나중 걸음이 맞다</b> — 더 뒤 지점까지 센
     * 값이고 observed_at 도 더 최신이다. 이것이 없으면 그 회차가 PK 충돌로 죽는다
     * (실측 — 걸음 나누기를 넣자마자 이 자리에서 터졌다).
     */
    public int writeStatuses(
            long runId, List<StatusRow> rows, Instant completedAt, Instant aggregatedThrough) {
        return inTransaction(runId, AnalyticsAxis.ISSUANCE_STATUS, completedAt, aggregatedThrough, () -> batch("""
                INSERT INTO analytics_issuance_statuses
                    (issue_date, coupon_id, brand_id, total_issued, currently_issued,
                     used, cancelled, expired, observed_at, run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new
                ON DUPLICATE KEY UPDATE
                    brand_id = new.brand_id, total_issued = new.total_issued,
                    currently_issued = new.currently_issued, used = new.used,
                    cancelled = new.cancelled, expired = new.expired,
                    observed_at = new.observed_at""",
                rows, (statement, row) -> {
                    statement.setObject(1, row.date());
                    statement.setLong(2, row.couponId());
                    statement.setLong(3, row.brandId());
                    statement.setLong(4, row.totalIssued());
                    statement.setLong(5, row.currentlyIssued());
                    statement.setLong(6, row.used());
                    statement.setLong(7, row.cancelled());
                    statement.setLong(8, row.expired());
                    statement.setObject(9, utc(row.observedAt()));
                    statement.setLong(10, runId);
                }));
    }

    /**
     * 회차를 마감하면서 실패한 축까지 <b>한 문장</b>으로 적는다.
     *
     * <p>축 표시를 따로 내면 그 UPDATE 가 실패했을 때(운영 풀이 흔들리는 순간이라 드물지 않다)
     * 축이 {@code PENDING} 으로 남아 <b>미집계와 장애가 구분되지 않는다.</b> 회차 마감과 같은
     * 문장에 두면 둘이 함께 성공하거나 함께 실패한다.
     *
     * <p><b>이미 AVAILABLE 인 축은 되돌리지 않는다.</b> 한 회차가 여러 걸음으로 따라잡는 동안 앞
     * 걸음이 성공했다면 그 지점까지는 실제로 집계된 것이라 AVAILABLE 이 참이고, 되돌리려면
     * {@code ck_analytics_run_*} 의 짝 조건 때문에 완료 시각과 <b>집계 지점까지 함께 비워야</b> 한다 —
     * 그러면 따라잡은 진행분이 사라져 다음 회차가 그 구간을 다시 훑는다.
     * [A 확정 2026-08-26] <i>"회차 전체 상태가 FAILED여도 개별 축이 AVAILABLE이면 해당 축 결과는
     * 사용할 수 있습니다"</i> — 그 합의가 이 동작의 근거다.
     */
    public void closeRun(long runId, Map<AnalyticsAxis, String> failures) {
        if (failures.isEmpty()) {
            // ⚠️ 사유를 함께 지운다. 거두기가 이 회차를 먼저 FAILED + 사유로 적어 놨을 수 있고,
            //    그 상태에서 status 만 바꾸면 ck_analytics_run_failure(FAILED ↔ 사유 있음)에 걸려
            //    마감 자체가 터진다.
            jdbcTemplate.update(
                    "UPDATE analytics_runs SET status = 'SUCCEEDED', failure_reason = NULL"
                            + " WHERE id = ?", runId);
            return;
        }
        StringBuilder sql = new StringBuilder(
                "UPDATE analytics_runs SET status = 'FAILED', failure_reason = ?");
        for (AnalyticsAxis axis : failures.keySet()) {
            sql.append(", ").append(axis.statusColumn())
                    .append(" = CASE WHEN ").append(axis.statusColumn())
                    .append(" = 'AVAILABLE' THEN ").append(axis.statusColumn())
                    .append(" ELSE 'UNAVAILABLE' END");
        }
        sql.append(" WHERE id = ?");
        String reason = failures.entrySet().stream()
                .map(entry -> entry.getKey() + ": " + entry.getValue())
                .reduce((left, right) -> left + " | " + right)
                .orElseThrow();
        jdbcTemplate.update(sql.toString(),
                reason.length() <= REASON_LIMIT ? reason : reason.substring(0, REASON_LIMIT),
                runId);
    }

    /**
     * 한 걸음의 행 쓰기와 축 표시를 한 트랜잭션에 묶는다.
     *
     * <p>걸음마다 커밋하므로 따라잡는 중에 죽어도 <b>거기까지는 남는다</b> — 다음 회차가 그 지점에서
     * 이어 간다. 한 회차를 통째로 한 트랜잭션에 묶으면 마지막 걸음에서 죽을 때 앞의 모든 걸음이
     * 함께 사라진다.
     *
     * <h2>뒤처진 회차가 앞서 간 집계를 덮어쓰지 못하게 한다</h2>
     *
     * <p>축 표시 UPDATE 에 <b>"내 지점이 이미 도달한 지점보다 뒤가 아닐 때만"</b> 이라는 조건을
     * 건다. 조건이 깨지면 갱신 행이 0이고, 그때는 예외를 올려 <b>이 걸음의 행 쓰기까지 함께
     * 되돌린다</b> — 그래서 값이 작아진 채로 남지 않는다.
     *
     * <p><b>왜 필요한가.</b> 배포 중에 구·신 컨테이너가 잠깐 같이 뜨면 각자 회차를 연다. 이 배치는
     * Spring Batch Job 이 아니라 {@code @Scheduled} 라 중복 실행 방지가 안 걸린다(저장소의 다른
     * 배치도 마찬가지다). 늦게 열린 쪽이 덜 따라잡은 채 쓰면 회차 번호는 그쪽이 더 커서 상태 축의
     * 최신이 되고, 발급 수 두 축은 그냥 덮어써진다 — 재계수가 {@code issued_at <= 기준} 이라
     * <b>값이 작아진 채로 남고 되돌아오지 않는다.</b>
     *
     * <p>호출부의 as_of 역행 검사로는 못 막는다. 그쪽은 자기가 읽은 값만 보므로, 읽은 뒤에 다른
     * 회차가 앞서 간 경우를 모른다. 그래서 <b>커밋하는 문장 자체</b>에 조건을 건다.
     *
     * <p><b>동률({@code >=})은 일부러 허용한다.</b> {@code >} 로 조이면 같은 지점을 다시 도는
     * 멱등 재시도가 실패한다. 동률이면 두 회차가 같은 지점까지 센 것이라 값이 같고, 상태 축에만
     * 같은 버킷 행이 회차별로 두 벌 남는다 — 최신 선택이 어느 쪽을 골라도 값이 같으므로 무해하다.
     *
     * <p>⚠️ 반대 방향 실패 — 두 노드가 상시로 겹쳐 돌면 진 쪽의 축이 매 회차 실패한다. 값은
     * 안전하지만 회차 이력이 FAILED 로 시끄러워진다. 그때는 노드를 하나로 줄이는 것이 답이다.
     */
    private int inTransaction(
            long runId, AnalyticsAxis axis, Instant completedAt, Instant aggregatedThrough,
            RowWrite write) {
        Integer written = transactionTemplate.execute(status -> {
            int count = write.run();
            int marked = jdbcTemplate.update(
                    "UPDATE analytics_runs SET " + axis.statusColumn() + " = 'AVAILABLE', "
                            + axis.completedAtColumn() + " = ?, "
                            + axis.aggregatedThroughColumn() + " = ? WHERE id = ?"
                            // 파생 표로 감싸야 한다 — MySQL 은 갱신 중인 표를 서브쿼리에서 직접
                            // 읽지 못한다.
                            + " AND ? >= COALESCE((SELECT MAX(reached) FROM ("
                            + "     SELECT " + axis.aggregatedThroughColumn() + " AS reached"
                            + "     FROM analytics_runs"
                            + "     WHERE " + axis.statusColumn() + " = 'AVAILABLE') already),"
                            + "   '1970-01-01')",
                    utc(completedAt), utc(aggregatedThrough), runId, utc(aggregatedThrough));
            if (marked == 0) {
                throw new IllegalStateException(
                        "다른 회차가 이미 더 앞까지 집계했다: axis=" + axis
                                + ", aggregatedThrough=" + aggregatedThrough);
            }
            return count;
        });
        return written == null ? 0 : written;
    }

    /**
     * ⚠️ 배치 크기를 행 수 전체로 주면 밀린 구간을 따라잡을 때 한 문장에 수천 행이 실린다 —
     * 패킷 하나가 그만큼 커지고 실패했을 때 되돌리는 단위도 그만큼이다. 청크로 나눈다
     * (아카이브의 500행 청크와 같은 단위 — {@code MainDataSourceConfig} 의 실측이 그 크기다).
     */
    private <T> int batch(String sql, List<T> rows, RowBinder<T> binder) {
        if (rows.isEmpty()) {
            return 0;
        }
        jdbcTemplate.batchUpdate(sql, rows, BATCH_CHUNK, binder::bind);
        return rows.size();
    }

    @FunctionalInterface
    private interface RowWrite {
        int run();
    }

    @FunctionalInterface
    private interface RowBinder<T> {
        void bind(PreparedStatement statement, T row) throws SQLException;
    }
}
