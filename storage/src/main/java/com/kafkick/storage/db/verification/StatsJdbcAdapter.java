// 통계 스냅샷 세 테이블을 집계 SQL 로 채웁니다.
package com.kafkick.storage.db.verification;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
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
 * 돌고 <b>통계 Step 은 그 둘보다 뒤</b>다. 게다가 {@code rejectRunningExpire} 는 배치 메타에서
 * <b>만료 잡의 실행</b>만 보므로 api 프로세스의 발급 트래픽은 애초에 그 조회에 안 잡힌다 —
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
     *
     * <p><b>⚠️ 회차 하나씩 나간다. 회차 전체를 한 문장으로 묶으면 서버가 죽는다</b>(CY-470 실측).
     * {@code GROUP BY (coupon_id, issued_grade)} 를 덮는 인덱스가 없어 옵티마이저가
     * {@code Aggregate using temporary table} 을 고르는데, MySQL 8 의 TempTable 엔진은
     * {@code temptable_max_ram}(기본 1GiB)까지 <b>디스크로 안 넘기고 RAM 에 쥔다.</b>
     * 300만 발급에서 mysqld 상주가 907MiB → 1,163MiB 로 12초 만에 부풀고 그 자리에서
     * 강제 종료됐다(세 번 재현). <b>결과가 936행인데도 그렇다</b> — 부푸는 축은 결과 크기가
     * 아니라 <b>한 문장이 훑는 입력 행 수</b>다.
     *
     * <p>같은 집계를 회차 단위 147회로 쪼개면 상주가 900 → 903MiB 로 <b>평평했고</b> 26초에
     * 끝났다(한 문장 판은 12초에 죽었으므로 완주 시간은 오히려 이쪽이 짧다).
     * {@code SQL_BIG_RESULT} 힌트와 {@code tmp_table_size} 인하는 둘 다 같은 곡선으로 죽었고,
     * {@code internal_tmp_mem_storage_engine} 은 앱 계정에 권한이 없다 — <b>쪼개는 것만 들었다.</b>
     *
     * <p>{@code coupons} 조인이 사라진 자리는 {@link #couponIdsAsOf} 가 같은 컷으로 대신한다.
     */
    private static final String AGGREGATE_GRADE_STATS_FOR_COUPON = """
            INSERT INTO grade_stats (run_id, coupon_id, grade, issued_total, used_total)
            SELECT :runId, i.coupon_id, i.issued_grade, COUNT(*), SUM(i.status = 'USED')
              FROM issuances i
             WHERE i.coupon_id = :couponId
               AND i.updated_at <= :asOf
             GROUP BY i.coupon_id, i.issued_grade
            """;

    /**
     * <b>회차 컷은 이 한 자리에서만 나온다.</b> {@link #couponCount} 와 {@code couponIdsAsOf}
     * 가 이것을 공유하므로 <b>둘이 갈릴 자리가 없다</b> — 목록 길이는 언제나 개수와 같고,
     * 그 개수는 호출부의 {@code couponRows != coupons} 검사를 통해 <b>회차 집계가 본 카탈로그</b>
     * 와 이어진다. 세 질의가 한 술어로 묶인다.
     *
     * <p><b>이것이 없으면 부분 집계가 조용히 {@code COMPLETE} 로 닫힌다.</b> 등급 집계는
     * CY-470 에서 회차 단위 루프가 됐는데, 목록을 뜨는 술어가 회차 집계 쪽과 어긋나면
     * 빠진 회차의 발급이 {@code grade_stats} 에서 통째로 사라진다 — 그런데 회차 집계는
     * 멀쩡하므로 등식 검사가 통과하고, 대시보드는 등급 퍼널만 과소 집계된 값을 읽는다.
     * 한 문장이던 시절엔 그 상태가 <b>구조적으로 불가능했다</b>({@code INSERT … SELECT} 는
     * 원자적이라 936행이 다 들어가거나 하나도 안 들어간다).
     *
     * <p>{@code AGGREGATE_COUPON_STATS} 의 {@code WHERE c.created_at <= :asOf} 는 이 상수를
     * 못 쓴다 — 그쪽은 조인 안의 별칭({@code c})을 쓰는 큰 문장의 일부다. <b>그 자리는
     * 술어를 공유하는 대신 실행 때 등식으로 검사된다.</b>
     */
    private static final String COUPON_CUT = "created_at <= :asOf";

    /** 등급 집계 루프가 돌 회차 목록. */
    private static final String SELECT_COUPON_IDS_AS_OF =
            "SELECT id FROM coupons WHERE " + COUPON_CUT + " ORDER BY id";

    /** 그 목록의 길이. 같은 술어라 위 질의와 언제나 같은 값이다. */
    private static final String COUNT_COUPONS_AS_OF =
            "SELECT COUNT(*) FROM coupons WHERE " + COUPON_CUT;

    /**
     * <b>창이 리플레이와 같아야 한다.</b> {@code created_at <= asOf} 만 걸면 다시 재는 것이라,
     * 얼린 상한보다 큰 id 인데 시각이 과거인 백데이트 이력을 리플레이는 못 읽고 통계는 읽는다.
     *
     * <p><b>요일 변환을 SQL 이 한다.</b> {@code WEEKDAY()} 는 0 = 월요일로 파이썬
     * {@code weekday()} 와 같아, {@code ELT(WEEKDAY(x) + 1, 'MON', …)} 이 시드의
     * {@code DOW[at.weekday()]} 와 글자 단위로 대응한다. {@code DAYNAME()} 은
     * {@code lc_time_names} 에 의존하므로 쓰지 않는다.
     *
     * <p><b>⚠️ 구간 하나씩 나간다.</b> {@code GROUP BY} 가 표현식이라 어떤 인덱스로도 정렬할 수
     * 없고, 그래서 {@code AGGREGATE_GRADE_STATS_FOR_COUPON} 과 <b>같은 병</b>을 앓는다 —
     * 516만 이력을 한 문장으로 묶으니 mysqld 상주가 902 → 1,077MiB 로 뛰었다(CY-470 실측).
     * 그때 죽지는 않았지만 <b>강제 종료가 관측된 1,163MiB 까지 여유가 86MiB 뿐</b>이라
     * 안전하다고 부를 수 없다. 부분 합은 {@link #issuedByHour} 가 접는다.
     */
    private static final String SELECT_ISSUED_BY_HOUR_IN_RANGE = """
            SELECT ELT(WEEKDAY(created_at) + 1,
                       'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN') AS day_of_week,
                   HOUR(created_at)                                      AS hour_of_day,
                   COUNT(*)                                             AS issued_total
              FROM issuance_histories
             WHERE event_type = 'ISSUE'
               AND id > :fromIdExclusive
               AND id <= :toIdInclusive
               AND id <= :maxHistoryId
               AND created_at <= :asOf
             GROUP BY day_of_week, hour_of_day
            """;

    private static final String SELECT_BROKEN_ISSUE_HISTORY = """
            SELECT %s
              FROM issuances i
              LEFT JOIN (SELECT issuance_id, COUNT(*) AS issue_events
                           FROM issuance_histories
                          WHERE event_type = 'ISSUE'
                            AND id <= :maxHistoryId
                            AND created_at <= :asOf
                          GROUP BY issuance_id) h ON h.issuance_id = i.id
             WHERE i.updated_at <= :asOf
               AND COALESCE(h.issue_events, 0) <> 1
            """;

    /**
     * {@link #issuedByHour} 가 이력 id 를 훑는 폭의 <b>상한이자 기본값</b>. 즉 이 손잡이는
     * <b>내릴 수만 있다</b> — 올릴 수 있게 두면 막으려던 사고가 그대로 돌아온다.
     * 더 작은 서버에서 더 좁혀야 할 수는 있으므로 내리는 쪽만 열어 둔다.
     *
     * <p><b>{@code batch.verify.replay-window-size} 와 축이 다르다 — 손잡이를 합치면 안 된다.</b>
     * 그쪽은 <b>발급건</b> id 창이고 막는 것은 <b>JVM 힙</b>이다(창 하나가 통째로 힙에 올라온다,
     * {@code IssuanceHistoryGroupReader}). 이쪽은 <b>이력</b> id 창이고 막는 것은
     * <b>MySQL 서버의 RAM</b>이다 — 자바로는 {@code (요일, 시각)} 부분합 최대 168행만 돌아온다.
     * 한 값으로 묶으면 힙을 넓히려고 올린 값이 서버를 죽인다.
     *
     * <p><b>값의 근거.</b> CY-470 에서 516만 이력 전수(그중 {@code event_type='ISSUE'} 258만)를
     * 한 문장으로 묶었을 때 서버 상주가 +175MiB 였다. 50만 id 폭이면 그 1/10 남짓이라
     * 강제 종료가 관측된 지점(+256MiB)까지 열 배 여유가 남는다.
     */
    static final long MAX_HISTORY_SCAN_WINDOW = 500_000L;

    /**
     * 같은 손잡이의 <b>바닥</b>. 내리는 쪽만 열어 뒀으니 내리다 생기는 사고도 여기서 막는다.
     *
     * <p><b>0 이하만 막으면 부족하다.</b> 1 은 통과하는데 그러면 이력 id 를 한 칸씩 훑어
     * 516만 셋에서 왕복이 500만 회를 넘고, {@code statsAggregateStep} 이
     * {@code batch.verify.step-timeout-ms} 에 걸려 죽는다. <b>그때 나는 알림은
     * {@code VerifyNotSucceeding} 인데 그 runbook 은 "그 슬롯에 발급이 있었다" 로 사람을
     * 보낸다</b> — 원인과 처방이 완전히 다른 곳을 가리킨다.
     *
     * <p>1만이면 같은 셋에서 왕복이 534회다. 왕복당 비용이 창 하나의 이득을 넘기 시작하는
     * 지점을 여기로 잡았다.
     */
    static final long MIN_HISTORY_SCAN_WINDOW = 10_000L;

    private final JdbcClient jdbcClient;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final long historyScanWindow;

    public StatsJdbcAdapter(
            JdbcClient jdbcClient,
            NamedParameterJdbcTemplate jdbcTemplate,
            @Value("${batch.verify.history-scan-window:" + MAX_HISTORY_SCAN_WINDOW + "}")
            long historyScanWindow) {
        // 위로도 아래로도 막는다. 넓히면 서버가 죽고, 좁히면 왕복이 폭증해 Step 이
        // 데드라인에 걸린다 — 뒤엣것은 엉뚱한 알림으로 나가 원인까지 가는 길이 멀다.
        if (historyScanWindow < MIN_HISTORY_SCAN_WINDOW
                || historyScanWindow > MAX_HISTORY_SCAN_WINDOW) {
            throw new IllegalArgumentException(
                    "batch.verify.history-scan-window 는 " + MIN_HISTORY_SCAN_WINDOW + " 이상 "
                            + MAX_HISTORY_SCAN_WINDOW + " 이하여야 합니다. 이 값은 요일·시각 "
                            + "집계 한 문장이 훑는 이력 id 폭입니다 — 넓히면 MySQL 의 TempTable 이 "
                            + "RAM 을 놓지 않아 서버가 강제 종료되고(CY-470 실측), 좁히면 왕복이 "
                            + "이력 수 ÷ 이 값만큼 늘어 statsAggregateStep 이 "
                            + "batch.verify.step-timeout-ms 에 걸립니다. 값=" + historyScanWindow);
        }
        this.jdbcClient = jdbcClient;
        this.jdbcTemplate = jdbcTemplate;
        this.historyScanWindow = historyScanWindow;
    }

    @Override
    public void clear(long runId) {
        for (String table : List.of("coupon_stats", "grade_stats", "hourly_stats")) {
            jdbcClient.sql("DELETE FROM " + table + " WHERE run_id = :runId")
                    .param("runId", runId)
                    .update();
        }
        // 포인터를 함께 내린다. 근거는 StatsRepository#clear 에 적었다 — 세 테이블만 비우면
        // v_latest_stats_run 이 행 0개짜리 실행을 "완결된 최신 스냅샷" 으로 가리킨다.
        //
        // ⚠️ **COMPLETE 였던 행만 내린다.** SKIPPED 는 지우면 안 되는 값이다 — CORRUPT 는
        //    "오염셋이라 집계를 안 했다", CLEAN 인데 verdict != PASS 면 "불합격이라 안 했다"
        //    를 뜻하고 뒤쪽은 경보다(StatsStatus javadoc · docs/11). 무조건 NULL 로 덮으면
        //    그 둘과 "통계 Step 이 죽었다(NULL)" 가 한 값으로 접혀, finalizeRunStep 이
        //    컬럼을 쓴 이유가 사라진다. 뷰는 COMPLETE 만 보므로 이 조건으로 충분하다.
        //
        // updateStatsStatus(runId, ...) 를 안 쓴다. StatsStatus 에 "없음" 값이 없어서다 —
        // 여기서 필요한 상태는 NULL, 즉 "이제 스냅샷이 없다" 다.
        jdbcClient.sql("""
                        UPDATE verification_runs SET stats_status = NULL
                         WHERE id = :runId AND stats_status = 'COMPLETE'
                        """)
                .param("runId", runId)
                .update();
    }

    @Override
    public int aggregateCouponStats(long runId, LocalDateTime asOf) {
        return jdbcClient.sql(AGGREGATE_COUPON_STATS)
                .param("runId", runId)
                .param("asOf", asOf)
                .update();
    }

    /**
     * <b>회차 하나씩 나간다.</b> 한 문장으로 묶으면 서버가 죽는 근거는
     * {@link #AGGREGATE_GRADE_STATS_FOR_COUPON} 에 적었다.
     *
     * <p><b>⚠️ 쪼갠 147회는 서로 원자적이지 않다.</b> 트랜잭션은 하나지만 그 문장들이
     * {@code REPEATABLE READ} 의 읽기 뷰를 <b>공유하지 않는다</b> — {@code INSERT … SELECT} 는
     * <b>락 리드</b>라 각 문장이 자기 실행 시점의 최신 커밋을 본다. 두 서버에서 재 봤다
     * (CY-470): 스냅샷을 확보한 트랜잭션 안에서, 그 뒤 다른 세션이 커밋한 행을
     * {@code INSERT … SELECT} 가 <b>그대로 옮겼다</b>. {@code binlog_format=ROW} 든
     * {@code log_bin=0} 이든 같았다.
     *
     * <p>그래서 <b>커밋을 나누는 것과는 다르지만 스냅샷이 하나인 것도 아니다.</b> 루프 중간에
     * {@code issuances} 가 {@code updated_at <= asOf} 로 백데이트 커밋되면 앞뒤 회차의 귀속이
     * 갈리는데, 그 경우는 {@code assertStillFrozen} 의 발급건 축({@code updated_at > asOf})에
     * 안 걸린다. <b>이 자리를 청크로 쪼개 커밋을 나누는 것이 안전해 보이지만 아니라는 것</b>이
     * 이 문단의 요지다 — 이 Step 이 데드라인에 걸리는 날 가장 먼저 나올 처방이 그것이다.
     *
     * <p><b>회차 목록은 반대로 컨시스턴트 리드다.</b> {@link #SELECT_COUPON_IDS_AS_OF} 는
     * 평범한 읽기라 트랜잭션 스냅샷을 본다. 그 비대칭이 만드는 창은 호출부가 바로 앞에서
     * {@code couponRows != coupons} 로 잡고({@code DATASET_MUTATED_DURING_RUN}), 뒤에서
     * {@code assertStillFrozen} 이 새 트랜잭션으로 한 번 더 본다. <b>개수가 상쇄되는
     * 동시 +1/−1 은 그 등식이 못 잡는다</b> — 다만 운영 경로에 {@code coupons.created_at} 을
     * 갱신하는 코드가 없어 그 창이 열리지 않는다(회차 전이는 {@code status} 만 쓴다).
     *
     * @return 쓴 행 수의 합. 한 문장이던 시절과 같은 값이다
     */
    @Override
    public int aggregateGradeStats(long runId, LocalDateTime asOf) {
        int written = 0;
        for (long couponId : couponIdsAsOf(asOf)) {
            written += jdbcClient.sql(AGGREGATE_GRADE_STATS_FOR_COUPON)
                    .param("runId", runId)
                    .param("couponId", couponId)
                    .param("asOf", asOf)
                    .update();
        }
        return written;
    }

    @Override
    public int couponCount(LocalDateTime asOf) {
        return jdbcClient.sql(COUNT_COUPONS_AS_OF)
                .param("asOf", asOf)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>짝으로 본다.</b> 총합 비교는 대칭 오차를 못 잡는다 —
     * {@code StatsRepository#countIssuancesWithBrokenIssueHistory} javadoc 에 근거를 적었다.
     */
    @Override
    public int countIssuancesWithBrokenIssueHistory(LocalDateTime asOf, long frozenMaxHistoryId) {
        return jdbcClient.sql(SELECT_BROKEN_ISSUE_HISTORY.formatted("COUNT(*)"))
                .param("asOf", asOf)
                .param("maxHistoryId", frozenMaxHistoryId)
                .query(Integer.class)
                .single();
    }

    @Override
    public List<Long> sampleIssuancesWithBrokenIssueHistory(
            LocalDateTime asOf, long frozenMaxHistoryId, int limit) {
        return jdbcClient.sql(
                        SELECT_BROKEN_ISSUE_HISTORY.formatted("i.id")
                                + " ORDER BY i.id LIMIT " + limit)
                .param("asOf", asOf)
                .param("maxHistoryId", frozenMaxHistoryId)
                .query(Long.class)
                .list();
    }

    /**
     * <b>이력 id 를 {@code batch.verify.history-scan-window} 폭으로 훑고 부분합을 접는다.</b> 한 문장으로
     * 묶으면 안 되는 근거는 {@link #SELECT_ISSUED_BY_HOUR_IN_RANGE} 에 적었다.
     *
     * <p><b>여기서 접어야 한다 — 부분합을 그대로 돌려주면 안 된다.</b> 호출부의
     * {@code HourlyIssued.fillAll} 이 {@code Collectors.toMap} 으로 칸을 찾는데, 같은
     * {@code (요일, 시각)} 이 두 구간에 걸치면 키가 겹쳐 {@code IllegalStateException} 이 난다.
     * 300만 시드는 두 달치라 <b>모든 칸이 여러 구간에 걸친다</b> — 즉 접지 않으면 언제나 죽는다.
     *
     * <p><b>구간은 정확히 나뉜다.</b> {@code (fromExclusive, toInclusive]} 라 경계 id 가 어느
     * 한쪽에만 들어간다 — 겹쳐 세거나 빠뜨리는 칸이 없다. 창을 몇 개로 쪼개든 합이 같은 이유다.
     *
     * <p>돌려주는 순서는 {@code (요일, 시각)} 삽입 순이라 실행마다 다를 수 있지만,
     * {@code fillAll} 이 168칸을 자기 순서로 채우므로 스냅샷은 결정적이다.
     */
    @Override
    public List<HourlyIssued> issuedByHour(long frozenMaxHistoryId, LocalDateTime asOf) {
        // **먼저 실제 상한으로 좁힌다.** frozenMaxHistoryId 는 호출부가 정한 값이라
        // Long.MAX_VALUE 일 수 있는데(얼릴 이력이 없을 때), 그 값으로 창을 걸으면 빈 구간을
        // 1,800경 번 도는 루프가 된다. 이력이 하나도 없으면 0 이라 루프가 안 돈다.
        long ceiling = scanCeiling(frozenMaxHistoryId);

        // 칸이 168개뿐이라 자바가 들고 있어도 된다. 창을 넓혀도 이 맵은 안 자란다.
        Map<String, HourlyIssued> merged = new LinkedHashMap<>();

        // **0 부터 시작한다.** 첫 ISSUE 이력 앞의 빈 창 몇 개를 더 도는 대신, 하한을 구하려고
        // event_type·created_at 컷이 붙은 집계를 한 번 더 돌리지 않는다 — 그 컷을 덮는
        // 인덱스가 없어 534만 행 전수 스캔이 되고, 이 Step 은 이미 그 테이블을 두 번 훑는다.
        for (long from = 0; from < ceiling; from += historyScanWindow) {
            long to = Math.min(from + historyScanWindow, ceiling);

            jdbcClient.sql(SELECT_ISSUED_BY_HOUR_IN_RANGE)
                    .param("fromIdExclusive", from)
                    .param("toIdInclusive", to)
                    .param("maxHistoryId", frozenMaxHistoryId)
                    .param("asOf", asOf)
                    .query((rs, rowNum) -> new HourlyIssued(
                            rs.getString("day_of_week"),
                            rs.getInt("hour_of_day"),
                            rs.getInt("issued_total")))
                    .list()
                    .forEach(partial -> merged.merge(
                            partial.dayOfWeek() + partial.hour(),
                            partial,
                            (a, b) -> new HourlyIssued(a.dayOfWeek(), a.hour(),
                                    a.issuedTotal() + b.issuedTotal())));
        }

        return List.copyOf(merged.values());
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

    private List<Long> couponIdsAsOf(LocalDateTime asOf) {
        return jdbcClient.sql(SELECT_COUPON_IDS_AS_OF)
                .param("asOf", asOf)
                .query(Long.class)
                .list();
    }

    /**
     * 루프를 끝낼 id. <b>PK 만 건다 — {@code event_type}·{@code created_at} 컷은 안 건다.</b>
     * 그 둘을 덮는 인덱스가 없어 걸면 534만 행 전수 스캔이 되는데, 이 Step 은 이미
     * {@code issuance_histories} 를 두 번 훑는다({@code SELECT_BROKEN_ISSUE_HISTORY} 의
     * 파생 테이블과 창 질의들). PK 만 걸면 역순 한 행이라 사실상 공짜다.
     *
     * <p>컷을 여기서 안 걸어도 결과가 같은 이유 — 창 질의가 그 셋을 <b>자기가</b> 들고 있다.
     * 여기서 정하는 것은 <b>어디까지 도느냐</b>뿐이고, 컷에 안 맞는 구간은 0행을 돌려준다.
     *
     * @return 이력이 하나도 없으면 0 — 그러면 루프가 한 번도 안 돈다
     */
    private long scanCeiling(long frozenMaxHistoryId) {
        Long ceiling = jdbcClient.sql("""
                        SELECT COALESCE(MAX(id), 0)
                          FROM issuance_histories
                         WHERE id <= :maxHistoryId
                        """)
                .param("maxHistoryId", frozenMaxHistoryId)
                .query(Long.class)
                .single();
        return ceiling == null ? 0L : ceiling;
    }
}
