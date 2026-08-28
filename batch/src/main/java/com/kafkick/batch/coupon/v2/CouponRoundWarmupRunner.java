package com.kafkick.batch.coupon.v2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.coupon.v2.port.RebuiltIssued;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

/**
 * 회차 하나를 Redis 에 올린다 — 설계 §6.2 의 <b>처음 여는 경우</b>만이다.
 *
 * <h2>순서가 전부다</h2>
 *
 * <pre>
 * 1. meta 가 이미 있으면 거절     ← 열린 게이트 뒤에서 카운터를 갈아엎지 않는다
 * 2. DB 집계를 한 트랜잭션으로 읽는다
 * 3. issued Hash · issued_ever · stock   (어댑터가 이 순서로 한 번에)
 * 4. issuance_engine_locked = TRUE  ← 엔진 확정. 게이트를 여는 것과 같은 사건이다
 * 5. coupon_stocks.active_count
 * 6. meta                          ← 게이트를 여는 행위. 반드시 마지막
 * </pre>
 *
 * <p><b>도중에 죽으면 {@code meta} 가 없는 상태로 남는다.</b> 그 회차의 발급은 전부
 * {@code -9} → 503 이고, 그것이 안전한 상태다. 반대로 {@code meta} 를 먼저 쓰면 그 창의 모든
 * 요청이 오경보이거나 — 더 나쁘게 — 낡은 카운터 위에서 성립한다.
 *
 * <h2>batch 가 소유하는 이유</h2>
 *
 * <p>batch 는 1대다. 재구성을 batch 만 수행하면 <b>프로세스 간</b> 겹침이 사라진다(07 의 (a)).
 * api 는 여러 대라 같은 코드를 거기 두면 한쪽이 게이트를 연 뒤 다른 쪽이 {@code stock} 을
 * 덮어쓴다 — 초과 발급 방향이다.
 *
 * <p><b>그것으로 끝이 아니다.</b> 07 의 겹침 시퀀스는 프로세스 수가 아니라 <b>실행의 겹침</b>으로
 * 성립하는데, 트리거가 HTTP 라 이 프로세스 안에도 워커 스레드가 여럿이다. 그래서
 * <b>프로세스 내 차단은 {@link #inFlight} 가 진다</b> — 배포 토폴로지가 대신해 주지 않는 몫이다.
 * 프로세스가 여럿이 되는 날에는 이걸로도 부족하고, 그때는 Redis 락(07 의 (b))이 S8b 가 아니라
 * 선행 조건이다.
 *
 * <h2>여기 없는 것</h2>
 *
 * <p>게이트를 닫고 다시 만드는 경로 · {@code meta} 쓰기 직전 재집계(4′) · 파손 회수 ·
 * 재동기화는 전부 <b>장애 대응</b>이라 S8b 다. 이 클래스가 그것들을 흉내 내면 안 된다 —
 * 이미 열린 회차를 만나면 {@link CouponRoundWarmupStatus#GATE_ALREADY_OPEN} 으로 손을 뗀다.
 */
public class CouponRoundWarmupRunner {

    private static final Logger log = LoggerFactory.getLogger(CouponRoundWarmupRunner.class);

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
     * 게이트가 없고, 재실행이 0행으로 거절돼 <b>되살릴 방법이 없어진다</b>. 빼면 멱등이다.
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

    /**
     * 진행 중인 회차. <b>{@code readMeta} 선검사로는 못 막는다</b> — 검사와 첫 쓰기 사이가
     * 열려 있고, 무엇보다 먼저 시작한 쪽이 이미 게이트를 연 뒤에도 늦은 쪽은 검사를 통과한
     * 상태로 {@code issued} 를 지우러 들어온다.
     */
    private final ConcurrentHashMap<Long, Boolean> inFlight = new ConcurrentHashMap<>();

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final IssuanceGatePort gate;
    private final IssuanceWarmupPort warmupPort;
    private final TimeProvider timeProvider;

    public CouponRoundWarmupRunner(
            JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate,
            IssuanceGatePort gate,
            IssuanceWarmupPort warmupPort,
            TimeProvider timeProvider) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.gate = gate;
        this.warmupPort = warmupPort;
        this.timeProvider = timeProvider;
    }

    public CouponRoundWarmupResult warmUp(long couponRoundId) {
        if (inFlight.putIfAbsent(couponRoundId, Boolean.TRUE) != null) {
            log.warn("워밍업을 건너뛴다 — 회차 {} 의 워밍업이 이미 돌고 있다.", couponRoundId);
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.WARMUP_IN_PROGRESS);
        }
        try {
            return warmUpExclusively(couponRoundId);
        } finally {
            inFlight.remove(couponRoundId);
        }
    }

    private CouponRoundWarmupResult warmUpExclusively(long couponRoundId) {
        // 1. 게이트가 이미 열려 있으면 손대지 않는다.
        // 여기서 걸리는 것은 "예전에 올라간 회차" 다. 지금 겹쳐 들어온 호출은 위 가드가 잡는다.
        if (gate.readMeta(couponRoundId).isPresent()) {
            log.warn("워밍업을 건너뛴다 — 회차 {} 의 게이트가 이미 열려 있다.", couponRoundId);
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.GATE_ALREADY_OPEN);
        }

        RoundRow round;
        try {
            round = jdbcTemplate.queryForObject(ROUND_SQL, this::mapRound, couponRoundId);
        } catch (EmptyResultDataAccessException exception) {
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.ROUND_NOT_FOUND);
        }
        if (round.totalQuantity() == null) {
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.STOCK_ROW_MISSING);
        }
        if (round.engineVersion() != EngineVersion.V2) {
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.ENGINE_NOT_V2);
        }
        if (!timeProvider.instant().isBefore(round.openAt())) {
            // 살아있는 회차를 v1 → v2 로 전환하는 경로는 만들지 않는다(10 의 "범위에서 빠진 것").
            // 그 경로는 게이트를 닫는 단계와 4′ 재집계를 함께 요구한다 — 둘 다 S8b 다.
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.ROUND_ALREADY_OPENED);
        }

        // 2. 활성 수·누적 수·회원 목록을 한 트랜잭션으로 읽는다. 세 문장을 각자 읽으면 그 사이의
        //    커밋이 그대로 gap 이 되고, 그 gap 은 워밍업이 만든 것이라 아무도 원인을 못 찾는다.
        Aggregate aggregate = transactionTemplate.execute(status -> new Aggregate(
                jdbcTemplate.query(TOTAL_QUANTITY_SQL,
                        rs -> rs.next() ? rs.getObject(1, Long.class) : null, couponRoundId),
                jdbcTemplate.queryForObject(ACTIVE_COUNT_SQL, Long.class, couponRoundId),
                jdbcTemplate.queryForObject(EVER_COUNT_SQL, Long.class, couponRoundId),
                jdbcTemplate.query(EVER_MEMBERS_SQL, this::mapMember, couponRoundId)));
        if (aggregate.totalQuantity() == null) {
            // 회차 조회와 이 스냅샷 사이에 재고 행이 사라졌다.
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.STOCK_ROW_MISSING);
        }

        // uk_coupon_member 가 회차당 회원 한 행을 강제하므로 이 둘은 같아야 한다(§9.1 I2).
        // 다르면 그 제약이 깨진 것이라, 여기서 조용히 목록 쪽을 택하면 I4 위반을 워밍업이
        // 만들어 낸다. 세지 않은 채 넘기지 않는다.
        if (aggregate.everCount() != aggregate.everMembers().size()) {
            throw new IllegalStateException(
                    "회차 " + couponRoundId + " 의 누적 건수(" + aggregate.everCount()
                            + ")와 회원 수(" + aggregate.everMembers().size() + ")가 다릅니다."
                            + " uk_coupon_member 가 깨졌을 때만 나오는 상태입니다.");
        }

        long totalQuantity = aggregate.totalQuantity();
        long remainingStock = totalQuantity - aggregate.activeCount();
        if (remainingStock < 0) {
            // DB 에 이미 §9.1 I1 위반이 있다. 선점 Lua 는 음수 stock 을 정상 값으로 읽어
            // -5(매진)를 내므로 초과 발급으로 번지지는 않지만, 그 사고를 못 본 채
            // "워밍업 성공" 으로 보고하면 아무도 다시 안 본다.
            log.error("회차 {} 의 활성 건수({})가 총재고({})를 넘는다. 초과 발급이 이미 있다.",
                    couponRoundId, aggregate.activeCount(), totalQuantity);
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.OVER_ISSUED_ROUND);
        }

        // 3. issued Hash · issued_ever · stock 을 한 번에.
        warmupPort.seedCounters(couponRoundId, aggregate.everMembers(), remainingStock);

        // 4. 회차를 V2 로 확정한다. **게이트를 여는 것과 엔진을 정하는 것은 같은 사건**이라,
        //    이 뒤로는 게이트를 여는 쓰기만 남는다. 여기서 잡히는 것은 워밍업이 도는 동안 엔진이
        //    뒤집힌 경우다 — 엔진 변경이 허용되는 조건(NOW < open_at)과 워밍업이 도는 조건이
        //    같아서 그 창은 실재한다. 잠근 뒤 죽으면 meta 가 없어 게이트가 닫힌 채 남고,
        //    잠금이 멱등이라 재실행이 복구한다.
        if (jdbcTemplate.update(LOCK_ENGINE_SQL, couponRoundId) != 1) {
            return CouponRoundWarmupResult.rejected(couponRoundId, lockFailureReason(couponRoundId));
        }

        // 5. DB_COUNTER_GAP(§9.1 I6)까지 정리한다. 이걸 빼면 워밍업 직후부터 그 축이 0 이 아니다.
        int updated = jdbcTemplate.update(
                "UPDATE coupon_stocks SET active_count = ?, updated_at = ? WHERE coupon_id = ?",
                aggregate.activeCount(),
                LocalDateTime.ofInstant(timeProvider.instant(), ZoneOffset.UTC),
                couponRoundId);
        if (updated != 1) {
            // 0건이면 DB_COUNTER_GAP 이 워밍업 직후부터 0 이 아니다. meta 를 쓰기 전이라
            // 여기서 던지면 게이트가 닫힌 채 남는다 — 성공으로 보고하는 것보다 낫다.
            throw new IllegalStateException(
                    "회차 " + couponRoundId + " 의 coupon_stocks 갱신이 " + updated + "행입니다.");
        }

        // 6. 게이트를 연다. 다섯 필드가 한 덩어리라 부분 상태가 남지 않는다.
        gate.writeMeta(couponRoundId, new GateMeta(
                GateStatus.OPEN,
                round.openAt().toEpochMilli(),
                round.closeAt().toEpochMilli(),
                round.gradeMask(),
                totalQuantity));

        log.info("회차 {} 워밍업 완료 — 총재고 {} · 활성 {} · 누적 {} · 잔여 {}",
                couponRoundId, totalQuantity, aggregate.activeCount(),
                aggregate.everCount(), remainingStock);
        return new CouponRoundWarmupResult(
                couponRoundId, CouponRoundWarmupStatus.WARMED, totalQuantity,
                aggregate.activeCount(), aggregate.everCount(), remainingStock);
    }

    /**
     * 잠금이 0행인 이유를 갈라 준다. 합쳐 두면 <b>회차가 없는데 "엔진이 V2 가 아니다"</b> 라고
     * 말하게 되고, 운영은 엉뚱한 데를 본다. 이미 실패한 뒤라 한 번 더 읽는 비용은 무의미하다.
     */
    private CouponRoundWarmupStatus lockFailureReason(long couponRoundId) {
        List<String> engines = jdbcTemplate.queryForList(
                "SELECT issuance_engine_version FROM coupons WHERE id = ?",
                String.class, couponRoundId);
        return engines.isEmpty()
                ? CouponRoundWarmupStatus.ROUND_NOT_FOUND
                : CouponRoundWarmupStatus.ENGINE_NOT_V2;
    }

    private RoundRow mapRound(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
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

    private RebuiltIssued mapMember(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
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

    private record RoundRow(
            Instant openAt,
            Instant closeAt,
            int gradeMask,
            EngineVersion engineVersion,
            Long totalQuantity) {
    }

    private record Aggregate(
            Long totalQuantity,
            long activeCount,
            long everCount,
            List<RebuiltIssued> everMembers) {
    }
}
