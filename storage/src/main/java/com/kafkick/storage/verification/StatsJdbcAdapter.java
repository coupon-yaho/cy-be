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
 * <p><b>컷이 네 축에 걸린다.</b> 발급건은 {@code updated_at <= asOf}, 이력은
 * <b>리플레이가 얼린 창</b>({@link #issuedByHour}), 회차는 {@code created_at <= asOf},
 * 재고는 {@code coupon_stocks.updated_at <= asOf} 다.
 *
 * <p><b>{@code ON} 이냐 {@code WHERE} 냐를 고를 수 있었던 축은 재고뿐이다.</b> 회차 컷은
 * 드라이빙 테이블({@code coupons}) 자체의 필터라 애초에 {@code ON} 에 넣을 자리가 없다 —
 * 둘 다 {@code WHERE} 에 있지만 재고 쪽만 선택의 여지가 있었다.
 * {@code LEFT JOIN} 의 {@code ON} 에 두면
 * 조건이 거짓일 때 회차 행은 남고 {@code s.*} 만 {@code NULL} 이 되는데, 완판 판정이
 * {@code issued_total >= s.total_quantity} 라 <b>완판 회차가 미달로 뒤집힌다</b> —
 * 행 수는 그대로여서 {@code couponRows == couponCount} 검사도 통과하고, 어긋난 값이
 * 조용히 {@code COMPLETE} 스냅샷에 들어간다. {@code WHERE} 에 두면 그 회차는 행 자체가
 * 안 써져 그 등식이 실행을 죽인다. 재고 행이 <b>없는</b> 회차(발급 0건)와 재고가
 * <b>움직인</b> 회차를 갈라야 해서 {@code s.updated_at IS NULL OR …} 형태다.
 *
 * <p>한때 발급건 쪽 컷을 <i>"{@code rejectIssuancesUpdatedAfterAsOf} 가 이미 거부하니 중복"</i>
 * 이라며 뺐는데 <b>틀렸다.</b> 그 가드는 {@code startRunStep} 과 {@code assertFrozenStep} 에서만
 * 돌고 <b>통계 Step 은 그 둘보다 뒤</b>다. 게다가 {@code rejectRunningSchedulers} 는 이 JVM 의
 * {@code batch.scheduling.enabled} 만 보므로 api 프로세스는 그 플래그로 멈추지 않는다 —
 * 집계 도중 발급 한 건이 들어오면 발급건 집계에는 반영되고 이력 집계에는 안 들어가
 * <b>같은 스냅샷 안에서 두 총합이 어긋난다.</b>
 * {@code issuances} 를 읽는 규칙 여섯이 전부 같은 컷을 갖는 이유가 이것이다.
 */
@Repository
public class StatsJdbcAdapter implements StatsRepository {

    /**
     * <b>회차 전체를 드라이빙으로 둔다.</b> 발급이 0건인 회차도 행을 써야 하므로
     * {@code issuances} 를 드라이빙으로 하면 그 회차가 빠진다. 시드도 카탈로그 전체를 돈다.
     *
     * <p><b>재고 컷을 {@code WHERE} 에 두고 {@code IS NULL} 을 명시한다.</b> V1 과 같은 모양이다.
     * 한때 {@code ON} 절에 넣었는데 <b>모양이 틀렸다</b> — {@code LEFT JOIN} 이라 조건이 거짓이면
     * 회차는 남고 {@code total_quantity} 만 {@code NULL} 이 되어, <b>완판 회차가 조용히 미달로
     * 뒤집힌다.</b> 한 행에서 앞 다섯 컬럼은 맞고 {@code sold_out_seconds} 만 거짓이 된다.
     *
     * <p>{@code WHERE} 로 옮기면 그 회차는 <b>행이 아예 안 써지고</b>
     * {@code couponRows != couponCount} 검사가 {@code DATASET_MUTATED_DURING_RUN} 으로 잡는다 —
     * 값을 뭉개지 말고 죽인다. 재고 행이 <b>없는</b> 회차는 {@code IS NULL} 로 살려 둔다.
     * V1 이 {@code coupons} 를 드라이빙으로 고른 근거가 그 회차를 잡는 것이라, 통계가 반대로
     * 가면 안 된다 — 그 회차의 완판 판정만 {@code NULL} 이다.
     *
     * <p><b>{@code coupons} 도 {@code created_at} 으로 자른다.</b> {@code asOf} 시점에 없던
     * 회차가 스냅샷에 들어오면 같은 {@code asOf} 재실행이 다른 행 수를 낸다.
     * {@code couponCount} 도 같은 컷을 써야 등식이 뜻을 갖는다 — 양쪽에 다 없으면 서로 상쇄된다.
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
                          WHERE updated_at <= :asOf
                          GROUP BY coupon_id) a
                     ON a.coupon_id = c.id
             WHERE c.created_at <= :asOf
               AND (s.updated_at IS NULL OR s.updated_at <= :asOf)
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
             WHERE updated_at <= :asOf
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
    public int aggregateCouponStats(long runId, LocalDateTime asOf) {
        return jdbcClient.sql(AGGREGATE_COUPON_STATS)
                .param("runId", runId)
                .param("asOf", asOf)
                .update();
    }

    @Override
    public int aggregateGradeStats(long runId, LocalDateTime asOf) {
        return jdbcClient.sql(AGGREGATE_GRADE_STATS)
                .param("runId", runId)
                .param("asOf", asOf)
                .update();
    }

    @Override
    public int couponCount(LocalDateTime asOf) {
        return jdbcClient.sql("SELECT COUNT(*) FROM coupons WHERE created_at <= :asOf")
                .param("asOf", asOf)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>짝으로 본다.</b> 총합 비교는 대칭 오차를 못 잡는다 —
     * {@code StatsRepository#countIssuancesWithoutIssueHistory} javadoc 에 근거를 적었다.
     */
    private static final String SELECT_WITHOUT_ISSUE_HISTORY = """
            SELECT %s
              FROM issuances i
             WHERE i.updated_at <= :asOf
               AND NOT EXISTS (SELECT 1
                                 FROM issuance_histories h
                                WHERE h.issuance_id = i.id
                                  AND h.event_type = 'ISSUE'
                                  AND h.id <= :maxHistoryId
                                  AND h.created_at <= :asOf)
            """;

    @Override
    public int countIssuancesWithoutIssueHistory(LocalDateTime asOf, long frozenMaxHistoryId) {
        return jdbcClient.sql(SELECT_WITHOUT_ISSUE_HISTORY.formatted("COUNT(*)"))
                .param("asOf", asOf)
                .param("maxHistoryId", frozenMaxHistoryId)
                .query(Integer.class)
                .single();
    }

    @Override
    public List<Long> sampleIssuancesWithoutIssueHistory(
            LocalDateTime asOf, long frozenMaxHistoryId, int limit) {
        return jdbcClient.sql(
                        SELECT_WITHOUT_ISSUE_HISTORY.formatted("i.id")
                                + " ORDER BY i.id LIMIT " + limit)
                .param("asOf", asOf)
                .param("maxHistoryId", frozenMaxHistoryId)
                .query(Long.class)
                .list();
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
