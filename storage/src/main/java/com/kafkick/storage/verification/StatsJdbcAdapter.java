// 통계 스냅샷 세 테이블을 집계 SQL 로 채웁니다.
package com.kafkick.storage.verification;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.HourlyIssued;
import com.kafkick.core.verification.StatsRepository;

/**
 * <b>{@code asof_state} 를 쓰지 않는다.</b> PRD 는 <i>"앞 Step 이 만든 {@code asof_state} 를
 * 재사용하므로 원본 300만 건을 다시 읽지 않는다"</i> 고 적었지만, 세 테이블 중 하나도 그것만으로는
 * 안 된다:
 *
 * <ul>
 *   <li>통계가 세는 값은 {@code issuances.status} 다. {@code asof_state.state} 는 리플레이 결과이고
 *       <b>그 둘이 다를 수 있는 것이 검증 대상</b>이다 — 통계가 검증 결과에 기대면 순환이 된다.</li>
 *   <li>{@code asof_state.coupon_id} 는 <b>발급건 id</b> 다(어휘 반전). 회차·등급으로 묶으려면
 *       {@code issuances} 를 조인해야 해서 스캔이 조인으로 바뀔 뿐 이득이 없다.</li>
 *   <li>요일·시각 분포는 {@code issuance_histories} 에만 있다. {@code asof_state} 는 발급건당
 *       한 행(마지막 상태)뿐이다.</li>
 * </ul>
 *
 * <p><b>{@code asOf} 필터를 안 거는 자리가 있다.</b> {@code startRunStep} 의
 * {@code rejectIssuancesUpdatedAfterAsOf} 가 {@code updated_at > asOf} 인 발급건이 하나라도 있으면
 * 실행을 거부하므로, 통과했다면 <b>{@code issuances.status} 가 곧 {@code asOf} 시점 status</b> 다.
 * {@code docs/01-what-we-build.md} 결정 2번이 <i>"{@code asOf} 는 실행 순간 고정, 과거 조회 아님"</i>
 * 이라 정한 것과 같은 얘기다.
 *
 * <p>이력은 다르다 — 거기는 <b>리플레이가 얼린 창</b>을 그대로 써야 한다({@link #issuedByHour}).
 */
@Repository
public class StatsJdbcAdapter implements StatsRepository {

    /**
     * <b>회차 전체를 드라이빙으로 둔다.</b> 발급이 0건인 회차도 행을 써야 하므로
     * {@code issuances} 를 드라이빙으로 하면 그 회차가 빠진다. 시드도 카탈로그 전체를 돈다.
     *
     * <p><b>재고 행도 {@code LEFT JOIN} 이다.</b> 재고 행이 없는 회차를 {@code INNER JOIN} 으로
     * 떨어뜨리면 그 회차가 통계에서 사라진다 — V1(재고 정합)이 {@code coupons} 를 드라이빙으로
     * 고른 근거가 <b>바로 그 회차를 잡는 것</b>이라, 통계가 반대로 가면 안 된다.
     * 그 회차의 완판 판정은 자연히 {@code NULL} 이다({@code NULL} 비교가 참이 아니다).
     *
     * <p><b>완판은 {@code total_quantity} 로 판정한다.</b> 시드가 완판을 정하는 식이
     * {@code issue_count >= total_quantity} 다({@code cy-seed/seedgen/catalog.py}).
     * {@code active_count} 로 판정하면 안 된다 — 그것은 <i>현재 보유량</i>(ISSUED + USED)이라
     * 취소·만료로 줄어들어, 미달 회차도 0 이 될 수 있고 완판 회차도 0 이 아닐 수 있다.
     *
     * <p><b>마지막 발급 시각은 {@code issuances.issued_at} 에서 온다.</b> 시드는 이력을 읽지 않고
     * 루프 변수 {@code last_issue_at}(= {@code issued_at})을 쓴다. CLEAN 에서는 ISSUE 이력의
     * {@code created_at} 과 같은 값이지만, <b>같다는 사실에 기대지 않는다</b> — 시드가 실제로
     * 읽는 컬럼을 쓴다.
     */
    private static final String AGGREGATE_COUPON_STATS = """
            INSERT INTO coupon_stats
                (run_id, coupon_id, issued_total, issued, used, cancelled, expired,
                 sold_out_seconds)
            SELECT :runId,
                   c.id,
                   COALESCE(a.issued_total, 0),
                   COALESCE(a.issued, 0),
                   COALESCE(a.used, 0),
                   COALESCE(a.cancelled, 0),
                   COALESCE(a.expired, 0),
                   CASE WHEN COALESCE(a.issued_total, 0) >= s.total_quantity
                        THEN TIMESTAMPDIFF(SECOND, c.open_at, a.last_issued_at)
                   END
              FROM coupons c
              LEFT JOIN coupon_stocks s ON s.coupon_id = c.id
              LEFT JOIN (SELECT coupon_id,
                                COUNT(*)                        AS issued_total,
                                SUM(status = 'ISSUED')          AS issued,
                                SUM(status = 'USED')            AS used,
                                SUM(status = 'CANCELLED')       AS cancelled,
                                SUM(status = 'EXPIRED')         AS expired,
                                MAX(issued_at)                  AS last_issued_at
                           FROM issuances
                          GROUP BY coupon_id) a
                     ON a.coupon_id = c.id
            """;

    /**
     * <b>존재하는 쌍만 쓴다.</b> 시드는 {@code totals.grade} 에 누적된 키만 쓰므로, 없는
     * {@code (회차, 등급)} 조합에 0 행을 만들면 행 수가 어긋난다.
     *
     * <p>등급은 {@code issued_grade} <b>스냅샷</b>이다. {@code members} 를 조인하면 그 뒤 등급이
     * 바뀐 회원의 과거 발급이 현재 등급으로 분류된다 — V6 이 스냅샷을 쓰는 이유와 같다.
     */
    private static final String AGGREGATE_GRADE_STATS = """
            INSERT INTO grade_stats (run_id, coupon_id, grade, issued_total, used_total)
            SELECT :runId, coupon_id, issued_grade, COUNT(*), SUM(status = 'USED')
              FROM issuances
             GROUP BY coupon_id, issued_grade
            """;

    /**
     * <b>창이 리플레이와 같아야 한다.</b> {@code created_at <= asOf} 만 걸면 다시 재는 것이라,
     * 얼린 상한보다 큰 id 인데 시각이 과거인 백데이트 이력을 리플레이는 못 읽고 통계는 읽는다.
     *
     * <p><b>요일 변환을 SQL 이 한다.</b> {@code WEEKDAY()} 는 0 = 월요일로 파이썬
     * {@code weekday()} 와 같아, {@code ELT(WEEKDAY(x) + 1, 'MON', …)} 이 시드의
     * {@code DOW[at.weekday()]} 와 글자 단위로 대응한다. {@code DAYNAME()} 은
     * {@code lc_time_names} 에 의존하므로 쓰지 않는다.
     */
    private static final String SELECT_ISSUED_BY_HOUR = """
            SELECT ELT(WEEKDAY(created_at) + 1,
                       'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN') AS day_of_week,
                   HOUR(created_at)                                      AS hour_of_day,
                   COUNT(*)                                             AS issued_total
              FROM issuance_histories
             WHERE event_type = 'ISSUE'
               AND id <= :maxHistoryId
               AND created_at <= :asOf
             GROUP BY day_of_week, hour_of_day
            """;

    private final JdbcClient jdbcClient;
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public StatsJdbcAdapter(JdbcClient jdbcClient, NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcClient = jdbcClient;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void clear(long runId) {
        for (String table : List.of("coupon_stats", "grade_stats", "hourly_stats")) {
            jdbcClient.sql("DELETE FROM " + table + " WHERE run_id = :runId")
                    .param("runId", runId)
                    .update();
        }
    }

    @Override
    public int aggregateCouponStats(long runId) {
        return jdbcClient.sql(AGGREGATE_COUPON_STATS).param("runId", runId).update();
    }

    @Override
    public int aggregateGradeStats(long runId) {
        return jdbcClient.sql(AGGREGATE_GRADE_STATS).param("runId", runId).update();
    }

    @Override
    public List<HourlyIssued> issuedByHour(long frozenMaxHistoryId, LocalDateTime asOf) {
        return jdbcClient.sql(SELECT_ISSUED_BY_HOUR)
                .param("maxHistoryId", frozenMaxHistoryId)
                .param("asOf", asOf)
                .query((rs, rowNum) -> new HourlyIssued(
                        rs.getString("day_of_week"),
                        rs.getInt("hour_of_day"),
                        rs.getInt("issued_total")))
                .list();
    }

    /**
     * 168행을 한 번에 보낸다. {@code rewriteBatchedStatements=true} 가 URL 에 있어 드라이버가
     * 하나의 다중 VALUES 문으로 합친다 — 168회 왕복이 아니다.
     */
    @Override
    public void appendHourlyStats(long runId, List<HourlyIssued> hourly) {
        SqlParameterSource[] batch = hourly.stream()
                .map(h -> new MapSqlParameterSource()
                        .addValue("runId", runId)
                        .addValue("dayOfWeek", h.dayOfWeek())
                        .addValue("hour", h.hour())
                        .addValue("issuedTotal", h.issuedTotal()))
                .toArray(SqlParameterSource[]::new);

        jdbcTemplate.batchUpdate("""
                INSERT INTO hourly_stats (run_id, day_of_week, hour, issued_total)
                VALUES (:runId, :dayOfWeek, :hour, :issuedTotal)
                """, batch);
    }
}
