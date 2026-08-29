package com.kafkick.batch.coupon.v2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.v2.port.RebuiltIssued;
import com.kafkick.core.observation.EngineVersion;

/**
 * 게이트를 여는 두 경로 — 워밍업({@link CouponRoundWarmupRunner})과
 * 재구성({@link CouponRoundRebuildRunner}) — 이 공유하는 DB 접근.
 *
 * <p><b>한 곳인 것이 계약이다.</b> 두 경로가 같은 카운터를 쓰는데 집계 조건이 갈리면, 어느
 * 경로로 올렸느냐에 따라 같은 회차의 {@code issued_ever} 가 달라진다. 그 차는 즉시
 * {@code LUA_GAP} 이고, 원인이 "어느 쪽으로 올렸나" 라는 사실은 아무 값에도 안 남는다.
 */
public class CouponRoundGateJdbc {

    /**
     * 활성 집계. {@code stock} 카운터의 짝이라 {@code CANCELLED}·{@code EXPIRED} 를 뺀다
     * (설계 §9.2).
     */
    private static final String ACTIVE_STATUS_LIST = statusList(
            EnumSet.of(IssuanceStatus.ISSUED, IssuanceStatus.USED));

    /**
     * 누적 집계. {@code issued} Hash 와 {@code issued_ever} 의 짝이라 <b>취소·만료를 넣는다</b> —
     * 1인 1매가 평생 기준이므로 취소한 회원도 재발급이 막혀야 한다. 활성과 조건이 다른 것이
     * 여기의 요점이다.
     *
     * <p>{@code COUNT(*)} 로 세지 않고 네 상태를 <b>열거</b>한다. 계약 밖 상태가 생기면 그 행까지
     * 세어 정합성 리더의 누적값과 어긋나는데, 새 상태가 "발급 이력" 인지는 사람이 정해야 한다.
     * 리더({@code ConsistencyRawValueReader})가 같은 이유로 같은 목록을 갖는다 — 둘이 갈리면
     * 워밍업 직후부터 {@code PERSIST_GAP} 이 0 이 아니다.
     */
    private static final String EVER_STATUS_LIST = statusList(EnumSet.of(
            IssuanceStatus.ISSUED, IssuanceStatus.USED,
            IssuanceStatus.CANCELLED, IssuanceStatus.EXPIRED));

    private static final String ROUND_SQL = """
            SELECT c.open_at, c.close_at, c.eligible_grades_mask, c.issuance_engine_version,
                   s.total_quantity
              FROM coupons c
              LEFT JOIN coupon_stocks s ON s.coupon_id = c.id
             WHERE c.id = ?
            """;

    /**
     * 총재고를 <b>집계와 같은 스냅샷에서</b> 다시 읽는다. 회차 조회에서 읽은 값을 그대로 쓰면
     * 그 사이에 재고가 바뀌었을 때 {@code stock} 의 기준과 {@code meta.totalQuantity} 가 갈리고,
     * 그러면 복원의 상한(§9.1)이 재고 계산과 다른 수를 보게 된다.
     */
    private static final String TOTAL_QUANTITY_SQL =
            "SELECT total_quantity FROM coupon_stocks WHERE coupon_id = ?";

    private static final String ACTIVE_COUNT_SQL = """
            SELECT COUNT(*) FROM issuances
             WHERE coupon_id = ? AND status IN (%s)
            """.formatted(ACTIVE_STATUS_LIST);

    private static final String EVER_COUNT_SQL = """
            SELECT COUNT(*) FROM issuances
             WHERE coupon_id = ? AND status IN (%s)
            """.formatted(EVER_STATUS_LIST);

    /**
     * 회차를 그 엔진에 <b>확정</b>한다. 게이트를 여는 것과 엔진을 정하는 것은 같은 사건이라,
     * {@code meta} 를 쓰기 직전에 이걸 통과해야 한다.
     *
     * <p><b>{@code issuance_engine_locked = FALSE} 를 조건에 넣지 않는다.</b> 넣으면 워밍업을
     * 두 번 못 돌린다 — 첫 실행이 잠금 뒤 {@code meta} 쓰기 전에 죽으면 그 회차는 잠긴 채
     * 게이트가 없고, 재실행이 0행으로 거절돼 <b>되살릴 방법이 없어진다</b>. 빼면 멱등이고,
     * 그래서 <b>재구성도 같은 문장을 그대로 쓴다</b> — 두 경로 모두 여기서 갈리는 것은
     * {@code ENGINE_NOT_V2} 하나다.
     *
     * <p>재실행이 1행으로 잡히는 근거는 Connector/J 의 기본값 {@code CLIENT_FOUND_ROWS} 다 —
     * 값이 안 바뀌어도 matched 를 돌려준다. 이 저장소는 어느 URL 에도 {@code useAffectedRows}
     * 를 적지 않았고, {@code CouponRoundRepositoryImpl.lockAndFindById} 가 이미 같은 성질에
     * 기대고 있다(안 그러면 두 번째 발급 요청부터 회차 정의가 안 읽힌다).
     *
     * <p>창이 닫히는 근거는 이 조건이 아니라 <b>{@code locked} 가 TRUE 가 되면
     * {@code updateIssuanceEngineWhenNotOpen} 이 더는 안 먹는다</b>는 사실이다.
     */
    private static final String LOCK_ENGINE_SQL = """
            UPDATE coupons SET issuance_engine_locked = TRUE
             WHERE id = ? AND issuance_engine_version = 'V2'
            """;

    private static final String EVER_MEMBERS_SQL = """
            SELECT member_id, issued_at FROM issuances
             WHERE coupon_id = ? AND status IN (%s)
             ORDER BY member_id
            """.formatted(EVER_STATUS_LIST);

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public CouponRoundGateJdbc(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    public Optional<RoundRow> findRound(long couponRoundId) {
        try {
            return Optional.ofNullable(
                    jdbcTemplate.queryForObject(ROUND_SQL, CouponRoundGateJdbc::mapRound, couponRoundId));
        } catch (EmptyResultDataAccessException exception) {
            return Optional.empty();
        }
    }

    /**
     * 활성 수·누적 수·회원 목록·총재고를 <b>한 트랜잭션으로</b> 읽는다. 네 문장을 각자 읽으면
     * 그 사이의 커밋이 그대로 gap 이 되고, 그 gap 은 이 코드가 만든 것이라 아무도 원인을
     * 못 찾는다.
     */
    public Aggregate readAggregate(long couponRoundId) {
        return transactionTemplate.execute(status -> new Aggregate(
                jdbcTemplate.query(TOTAL_QUANTITY_SQL,
                        rs -> rs.next() ? rs.getObject(1, Long.class) : null, couponRoundId),
                jdbcTemplate.queryForObject(ACTIVE_COUNT_SQL, Long.class, couponRoundId),
                jdbcTemplate.queryForObject(EVER_COUNT_SQL, Long.class, couponRoundId),
                jdbcTemplate.query(EVER_MEMBERS_SQL, CouponRoundGateJdbc::mapMember, couponRoundId)));
    }

    /**
     * 활성 건수만 다시 센다 — 재구성의 4′ 다. 앞선 집계와의 차가 그 창 동안 커밋된
     * 취소·사용취소·만료다.
     *
     * <p><b>이 값이 단조 감소한다고 가정하지 마라.</b> 게이트를 닫아도 이미 선점을 끝낸 발급의
     * DB 커밋은 그 뒤에 도착한다({@link CouponRoundRebuildRunner} 의 "4′ 로도 안 닫히는 창").
     */
    public long readActiveCount(long couponRoundId) {
        return jdbcTemplate.queryForObject(ACTIVE_COUNT_SQL, Long.class, couponRoundId);
    }

    /** @return 1행이면 확정됐다. 0행이면 회차가 없거나 엔진이 V2 가 아니다 */
    public int lockEngineToV2(long couponRoundId) {
        return jdbcTemplate.update(LOCK_ENGINE_SQL, couponRoundId);
    }

    /**
     * 활성 건수를 다시 세고 {@code coupon_stocks.active_count} 를 <b>같은 트랜잭션 안에서</b>
     * 갱신한다 — 재구성의 4′ 다.
     *
     * <p><b>{@code FOR UPDATE} 가 요점이다.</b> 세는 것과 쓰는 것이 다른 트랜잭션이면 그 사이에
     * 커밋된 취소의 감소분을 절대값 UPDATE 가 덮어 없앤다. 그러면 재구성이 정리하겠다고 만든
     * {@code DB_COUNTER_GAP}(§9.1 I6)이 재구성 <b>직후부터</b> 0 이 아니다.
     *
     * <p>재고 행을 먼저 잠그므로, 같은 행을 갱신하는 발급·취소는 이 트랜잭션이 끝날 때까지
     * 기다린다. 잠근 <b>뒤에</b> 세는 것이라 그 시점까지 커밋된 것은 전부 들어온다.
     *
     * <p><b>총재고를 넘으면 쓰지 않고 돌아온다.</b> {@code ck_coupon_stock_active_range}
     * ({@code active_count <= total_quantity}, V3)가 그 UPDATE 를 거절하므로, 검사를 호출부에
     * 두면 이 자리에서 제약 위반 예외가 먼저 터져 <b>초과 발급이라는 진단이 SQL 오류로
     * 바뀐다</b>. 판정과 쓰기가 같은 트랜잭션 안에 있어야 하는 이유이기도 하다.
     *
     * @param totalQuantity 범위 판정의 기준. {@code meta} 에 실릴 값과 <b>같은 수</b>를 넘겨라 —
     *     여기서 따로 읽으면 판정과 게이트가 다른 총재고를 보게 된다
     * @return 다시 센 활성 건수와, 그 값을 실제로 썼는지
     */
    public Recount recountAndUpdateActiveCount(
            long couponRoundId, long totalQuantity, Instant now) {
        return transactionTemplate.execute(status -> {
            if (jdbcTemplate.queryForList(
                    "SELECT total_quantity FROM coupon_stocks WHERE coupon_id = ? FOR UPDATE",
                    Long.class, couponRoundId).isEmpty()) {
                // meta 를 쓰기 전이라 여기서 던지면 게이트가 닫힌 채 남는다.
                throw new IllegalStateException(
                        "회차 " + couponRoundId + " 의 coupon_stocks 행이 사라졌습니다.");
            }
            long activeCount = jdbcTemplate.queryForObject(
                    ACTIVE_COUNT_SQL, Long.class, couponRoundId);
            // 누적도 같은 스냅샷에서 센다. 시딩에 들어간 수와 다르면 그 차가 곧 시딩 뒤에
            // 커밋된 발급이고, issued Hash 에서 빠진 회원 수다. 따로 읽으면 그 차가 이 재집계가
            // 만든 것인지 아닌지조차 말할 수 없다.
            long everCount = jdbcTemplate.queryForObject(
                    EVER_COUNT_SQL, Long.class, couponRoundId);
            if (activeCount > totalQuantity) {
                return new Recount(activeCount, everCount, false);
            }
            updateActiveCount(couponRoundId, activeCount, now);
            return new Recount(activeCount, everCount, true);
        });
    }

    /**
     * {@code DB_COUNTER_GAP}(§9.1 I6)을 정리한다.
     *
     * <p><b>0행이면 던진다.</b> 그 축이 즉시 0 이 아니라는 뜻이고, 두 경로 모두 {@code meta} 를
     * 쓰기 전에 이걸 부르므로 여기서 던지면 게이트가 닫힌 채 남는다 — 성공으로 보고하는 것보다
     * 낫다. <b>검사를 호출부에 두지 않는 이유</b>는 워밍업과 재구성이 각자 적으면 한쪽만 고쳐진
     * 채 남기 때문이다.
     */
    public void updateActiveCount(long couponRoundId, long activeCount, Instant now) {
        int updated = jdbcTemplate.update(
                "UPDATE coupon_stocks SET active_count = ?, updated_at = ? WHERE coupon_id = ?",
                activeCount, LocalDateTime.ofInstant(now, ZoneOffset.UTC), couponRoundId);
        if (updated != 1) {
            throw new IllegalStateException(
                    "회차 " + couponRoundId + " 의 coupon_stocks 갱신이 " + updated + "행입니다.");
        }
    }

    /**
     * 잠금이 0행인 이유를 갈라 준다. 합쳐 두면 <b>회차가 없는데 "엔진이 V2 가 아니다"</b> 라고
     * 말하게 되고, 운영은 엉뚱한 데를 본다. 이미 실패한 뒤라 한 번 더 읽는 비용은 무의미하다.
     *
     * @return 회차 행이 있으면 {@code true} — 즉 엔진이 V2 가 아니다
     */
    public boolean roundExists(long couponRoundId) {
        return !jdbcTemplate.queryForList(
                "SELECT issuance_engine_version FROM coupons WHERE id = ?",
                String.class, couponRoundId).isEmpty();
    }

    private static RoundRow mapRound(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        Long totalQuantity = rs.getObject("total_quantity", Long.class);
        String engine = rs.getString("issuance_engine_version");
        return new RoundRow(
                utc(rs.getObject("open_at", LocalDateTime.class)),
                utc(rs.getObject("close_at", LocalDateTime.class)),
                rs.getInt("eligible_grades_mask"),
                // NULL 은 하위 호환으로 V1 이다(V2026082801 의 컬럼 주석). 여기서 V2 로 읽으면
                // 엔진을 지정한 적 없는 회차에 v2 키가 올라간다.
                engine == null ? EngineVersion.V1 : EngineVersion.valueOf(engine),
                totalQuantity);
    }

    private static RebuiltIssued mapMember(java.sql.ResultSet rs, int rowNum)
            throws java.sql.SQLException {
        return new RebuiltIssued(
                rs.getLong("member_id"),
                utc(rs.getObject("issued_at", LocalDateTime.class)).toEpochMilli());
    }

    /**
     * <b>{@code getTimestamp} 를 쓰지 않는다.</b> 그쪽은 커넥션 타임존으로 해석한 뒤 JVM 기본
     * 타임존으로 한 번 더 옮겨, 배포와 테스트의 타임존이 다르면 {@code openAt} 이 몇 시간씩
     * 밀린다. 밀린 게이트는 예외가 아니라 <b>정상적인 {@code -2}(미오픈)</b> 로 나가서 경보가
     * 뜨지 않는다. {@code getObject(LocalDateTime.class)} 는 어느 타임존도 타지 않고 컬럼에 적힌
     * 값을 그대로 준다.
     *
     * <p>그 값을 UTC 로 못박는 근거는 <b>쓰는 쪽</b>이다 — 시각의 원본이
     * {@code Clock.systemUTC()} 라(core 의 {@code TimeConfiguration}) 저장된 것이 UTC 벽시계다.
     * MySQL 의 {@code default-time-zone} 이 아니다. compose 의 mysql 은 그 설정을 주지 않는다.
     */
    private static Instant utc(LocalDateTime value) {
        return value.toInstant(ZoneOffset.UTC);
    }

    private static String statusList(EnumSet<IssuanceStatus> statuses) {
        return Arrays.stream(IssuanceStatus.values())
                .filter(statuses::contains)
                .map(status -> "'" + status.name() + "'")
                .collect(Collectors.joining(", "));
    }

    public record RoundRow(
            Instant openAt,
            Instant closeAt,
            int gradeMask,
            EngineVersion engineVersion,
            Long totalQuantity) {
    }

    /**
     * 4′ 재집계의 결과.
     *
     * @param applied {@code false} 면 활성 건수가 총재고를 넘어 아무것도 쓰지 않았다.
     *     호출부는 게이트를 <b>열면 안 된다</b>
     */
    public record Recount(long activeCount, long everCount, boolean applied) {
    }

    public record Aggregate(
            Long totalQuantity,
            long activeCount,
            long everCount,
            List<RebuiltIssued> everMembers) {
    }
}
