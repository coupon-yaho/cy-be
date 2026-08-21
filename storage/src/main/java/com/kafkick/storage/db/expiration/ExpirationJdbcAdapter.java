// 만료 처리 SQL 일곱입니다 — 청크가 쓰는 여섯과, 실행당 한 번 도는 제외 판정 하나입니다. 전부 집합 단위로 돌고, 행을 미리 골라 두는 잠금 읽기를 쓰지 않습니다.
package com.kafkick.storage.db.expiration;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.expiration.ExpirationRepository;

/**
 * <b>여섯 문장이 하나의 청크를 이룬다.</b> 넘기고 · 경계를 찾고 · 이력을 남기고 · 회차를 세고 ·
 * 재고 행을 세고 · 재고를 되돌린다. 여섯이 한 트랜잭션 안에서 돌아야 한다 — 상태만 바뀌고
 * 재고가 안 돌아온 중간 상태가 남으면 검증이 그것을 재고 불일치로 잡는다.
 *
 * <p><b>시각이 둘이다.</b> {@code asOf} 는 만료 여부를 가르는 컷({@code expires_at < asOf})이고,
 * {@code committedAt} 은 실제로 쓴 시각이다. 쓰는 시각을 {@code asOf} 로 백데이트하면
 * 잡이 도는 동안 들어온 이력보다 앞서게 찍혀 <b>리플레이가 순서를 뒤집는다.</b>
 *
 * <p><b>{@code updated_at = :committedAt} 이 그 청크의 표식이다.</b> 뒤 문장들이 이 표식으로
 * 방금 넘어간 집합을 다시 찾고, {@code id > :afterId AND id <= :lastId} 로 그 구간을 닫는다.
 *
 * <p><b>상한이 하는 일은 두 층이다.</b>
 *
 * <ol>
 *   <li><b>지금(READ COMMITTED)</b> — 뒤 문장들의 <b>스캔 범위</b>를 청크로 닫는다.
 *       상한이 없으면 그 문장들이 테이블 끝까지 훑는다.
 *   <li><b>격리가 RR 로 되돌아가는 날</b> — {@code INSERT … SELECT} 와 {@code UPDATE … JOIN} 의
 *       원본 읽기가 공유 next-key 잠금 읽기가 되어 supremum 까지 잠근다. 그때 이 상한이
 *       발급 봉쇄를 막는 마지막 겹이다.
 * </ol>
 *
 * <p><b>상한이 발급 봉쇄를 푼 것이 아니다.</b> 그것을 푼 것은 READ COMMITTED 이고, 상한은
 * 첫 문장에 걸 수도 없다. 예전 주석이 "상한을 걸면 발급이 통과한다" 고 적었는데
 * <b>만료 대상이 {@code LIMIT} 을 채우는 조건에서만 그랬다</b> —
 * {@code docs/12-expire-lock-measurement.md} 의 <i>"실측이 뒤집은 것"</i> 절이 그 문장을
 * 철회했다.
 *
 * <p><b>여섯 중 셋이 상태를 바꾼다.</b> {@code LAST_EXPIRED_ID} · {@code EXPIRED_COUPON_COUNT} ·
 * {@code STOCK_ROW_COUNT} 는 짝을 만드는 가드용이라 아무것도 안 바꾼다 — 지우면 재고 행 없는
 * 회차와 재고가 모자란 회차가 조용히 빠진다.
 *
 * <p>설계 단계에서 MySQL <b>8.4</b> 컨테이너에 문장들을 손으로 돌려 기계를 확인했다 — {@code USED} 로 바뀐 건을 건너뛰고 다음 id 로 이어 가며, 남은 대상이
 * 없을 때 첫 문장이 0 을 돌려준다.
 *
 * <p><b>다만 CI 가 도는 버전은 그것이 아니다.</b> {@code MySqlContainerConfig} 가
 * {@code mysql:latest} 라 그때그때 다르다. 이 SQL 을 계속 지키는 것은
 * {@code ExpirationJdbcAdapterTest} 다. 두 버전이 갈리는 자리가 나오면 그 테스트가 먼저 말한다.
 */
@Repository
public class ExpirationJdbcAdapter implements ExpirationRepository {

    /**
     * <b>거르는 조건이 안에 있다.</b> {@code status = 'ISSUED'} 를 여기 두어, 뽑아 둔 사이에
     * 사용된 건은 매치되지 않고 조용히 빠진다. 밖에서 후보를 뽑는 방식이면 그 건들 때문에
     * 진도가 안 나가는 청크가 생긴다.
     *
     * <p>{@code useAffectedRows=false} 라 드라이버가 <b>매치된 행 수</b>를 돌려준다.
     * 여기서는 매치된 행이 전부 실제로 바뀌므로 두 값이 같다 — 같은 값을 다시 쓰는
     * {@code UPDATE} 를 여기 추가하면 그 등식이 깨진다.
     *
     * <p><b>{@code (status, expires_at)} 인덱스({@code V11})가 이 문장을 받친다.</b> 그것이
     * 없으면 옵티마이저가 PRIMARY 를 id 순으로 훑는데, 넘길 것이 {@code LIMIT} 보다 적은
     * 실행은 끝까지 훑고 supremum 까지 잠가 <b>신규 발급 INSERT 를 죽였다.</b>
     * 실측(200,000행 중 뒤 1,000건만 대상): 읽은 행 201,000 → <b>2,001</b>.
     * (스캔 축 정의 — {@code Handler_read_next|rnd_next|first|key} 합)
     *
     * <p><b>인덱스만으로는 발급 봉쇄가 안 풀렸다.</b> 막던 것이 스캔 범위가 아니라 보조 인덱스의
     * gap 이라, 만료 Step 의 격리를 READ COMMITTED 로 내려서 풀었다({@code ExpireJobConfig}).
     * 둘은 서로 다른 문제를 푼다 — 인덱스는 스캔 비용, 격리는 가용성.
     * 수치와 재현 방법은 {@code docs/12-expire-lock-measurement.md} 에 있다.
     *
     * <p>{@code ORDER BY id} 가 인덱스 순서와 달라 filesort 가 붙지만, 정렬량이
     * {@code LIMIT} 에 묶여 청크당 1,000행이다 — 후보가 아무리 많아도 그 이상 안 는다.
     *
     * <p><b>{@code updated_at <= :committedAt} 가 캡처 창을 닫는다.</b> 잡은 시각을 먼저 잡고
     * 그 뒤에 스캔이 도는데, 그 사이에 상태가 바뀐 행까지 매치하면 <b>그 행에 과거 시각이
     * 찍힌다.</b> 리플레이 정렬이 {@code (issuance_id, created_at, id)} 라 우리 이력이
     * 나중에 일어난 취소보다 앞서게 되고, {@code USED → EXPIRE} 같은 전이표에 없는 조합이
     * 만들어져 V4·V3·V1 이 한 행에서 함께 운다.
     *
     * <p>{@code asOf} 를 백데이트하지 않는 것과 <b>같은 사고를 다른 쪽에서 막는 것</b>이다 —
     * 그쪽은 컷을, 이쪽은 시각을 잡은 뒤의 창을 막는다. 여기서 밀린 행은 다음 주기가
     * 새 {@code committedAt} 으로 집는다(같은 실행 안에서는 {@code afterId} 가 지나갔다).
     */
    private static final String EXPIRE_BATCH = """
            UPDATE issuances
               SET status = 'EXPIRED', updated_at = :committedAt
             WHERE status = 'ISSUED'
               AND expires_at < :asOf
               AND updated_at <= :committedAt
               AND id > :afterId
               AND coupon_id NOT IN (:blockedCoupons)
             ORDER BY id
             LIMIT :limit
            """;

    /** 회차 id 가 될 수 없는 값. auto-increment 라 음수가 나오지 않는다. */
    private static final long NOT_A_COUPON_ID = -1L;

    /**
     * <b>지금 만료시킬 수 없는 회차.</b> 재고 행이 없거나, 남은 대기를 다 빼면 음수가 된다.
     *
     * <p><b>{@code LEFT JOIN} 이라야 한다.</b> 안쪽 조인이면 재고 행 없는 회차가 조용히
     * 빠져서 — 그것이 정확히 찾으려는 대상 하나다 — 가려는 두 경우 중 하나를 못 본다.
     *
     * <p><b>{@code EXPIRE_BATCH} 의 {@code updated_at <= :committedAt} 창을 여기서는
     * 일부러 안 건다.</b> 창을 걸면 이 값이 <i>"첫 청크 시각 기준의 대기"</i> 가 되는데,
     * {@code committedAt} 은 청크마다 새로 잡혀 <b>뒤 청크의 창이 더 넓다.</b> 그러면
     * 그 사이에 {@code updated_at} 이 갱신된 행이 여기 안 세졌는데 만료는 되어,
     * 회차별 차감 합계가 여기서 센 값을 넘고 {@code STOCK_UNDERFLOW} 로 잡이 죽는다 —
     * 이 제외 논리가 없애려던 바로 그 실패다.
     *
     * <p>창을 빼면 이 값은 <b>그 실행이 넘길 수 있는 모든 행의 상계</b>가 된다.
     * {@code committedAt} 이 뒤로 밀려도 제외는 보수적인 쪽으로만 움직인다.
     *
     * <p><b>{@code ORDER BY} 는 로그를 위한 것이다.</b> 알림이 <i>"어느 회차인지는 WARN
     * 로그에 있다"</i> 로 사람을 보내는데, 순서가 실행마다 달라지면 두 실행의 로그를
     * 견주는 흔한 동작이 안 통한다. 회차는 수백 개라 정렬 비용이 없다.
     *
     * <p><b>이 정렬을 테스트로 못 박지는 않았다.</b> 지금 실행계획은 파생테이블을 회차 id
     * 순으로 내보내서, {@code ORDER BY} 를 지워도 결과가 안 바뀐다 — 실제로 지워 보고
     * 확인했다. 깨지지 않는 단언은 없는 단언과 같아서 두지 않는다. 여기서 지키는 것은
     * <b>순서가 우연이 아니라 계약이 되게 하는 것</b>이고, 알림이 실제로 가리키는 WARN
     * 로그 쪽은 {@code ExpireBlockedCouponIsolationTest} 가 문구까지 잡는다.
     */
    private static final String BLOCKED_COUPONS = """
            SELECT x.coupon_id
              FROM (SELECT coupon_id, COUNT(*) AS pending
                      FROM issuances
                     WHERE status = 'ISSUED'
                       AND expires_at < :asOf
                     GROUP BY coupon_id) x
              LEFT JOIN coupon_stocks s ON s.coupon_id = x.coupon_id
             WHERE s.coupon_id IS NULL
                OR s.active_count < x.pending
             ORDER BY x.coupon_id
            """;

    /**
     * <b>이 문장만 상한이 없다.</b> 상한을 구하는 것이 이 문장의 일이라서다.
     *
     * <p><b>대신 {@code expires_at < :asOf} 는 나머지와 똑같이 건다.</b> 이 문장이 만드는
     * {@code lastId} 가 뒤 문장 전부의 창이 되므로, 여기서 남의 행이 {@code MAX(id)} 를
     * 밀어 올리면 <b>그 창이 통째로 넓어진다.</b> 다른 다섯에만 걸고 여기를 빼 두면
     * 겹을 하나 세워 놓고 문을 열어 두는 셈이다. 대신 평범한
     * {@code SELECT} 라 REPEATABLE READ 의 consistent read 로 돌고 <b>락을 잡지 않는다</b> —
     * 뒤 문장들과 달리 발급을 막지 않는다.
     *
     * <p><b>이 Step 은 READ COMMITTED 다.</b> 그래서 문장마다 스냅샷이 새로 잡힌다 —
     * 청크 트랜잭션 전체가 하나의 스냅샷을 공유하지 않는다. 그래도 우리 집합은 안전하다:
     * {@code (afterId, lastId]} 안쪽은 {@code EXPIRE_BATCH} 가 X 락으로 쥐고 있다.
     *
     * <p><b>표식은 고유하지 않다 — 무엇이 그것을 메우는지 적어 둔다.</b>
     * {@code committedAt} 은 {@code datetime(6)} 시각일 뿐이라, <b>원리적으로는</b> 다른
     * 프로세스가 같은 마이크로초에 {@code EXPIRED} 를 써서 이 {@code MAX(id)} 를 밀어 올릴 수
     * 있다. 그러면 뒤 문장 다섯의 창이 넓어진다. 지금 그것을 막는 것은 <b>둘</b>이고,
     * 그중 창 안쪽까지 막는 것은 <b>첫째뿐이다.</b>
     *
     * <ol>
     *   <li><b>같은 {@code asOf} 로 두 번 못 돈다.</b> {@code asOf} 가 잡 파라미터라 스프링
     *       배치가 같은 파라미터의 실행을 거부한다. <b>단, 그 사실이 DB 에 남아야 성립한다</b> —
     *       {@code BatchJobRepositoryConfig} 가 없으면 저장소가
     *       {@code ResourcelessJobRepository} 라 인스턴스가 한 줄도 안 남고, 프로세스가 둘이면
     *       서로를 못 본다.</li>
     *   <li>창 밖으로 새는 방향은 나머지 다섯의 {@code id <= :lastId} 가 막는다
     *       (그 축은 {@code ExpirationJdbcAdapterTest} 가 실제로 남의 행을 심어 확인한다).</li>
     * </ol>
     *
     * <p><b>여기 <i>"다른 {@code asOf} 면 {@code expires_at < :asOf} 가 갈라 준다"</i> 고 셋째
     * 겹을 적었었다. 틀렸다.</b> 그 술어는 단조 <b>포함</b>이다 — {@code asOf} 가 이른 쪽의
     * 집합이 늦은 쪽의 부분집합이라, 어느 방향으로도 두 실행을 가르지 못한다. 겹은 둘이다.
     *
     * <p><b>그래서 남는 구멍은 하나다</b> — 다른 프로세스가 <b>같은 마이크로초</b>에,
     * <b>우리 창 안쪽</b> id 로 {@code EXPIRED} 를 쓰는 경우. 발급 경로가 만료를 안 쓰고
     * 배치가 한 대인 지금은 닿을 수 없고, 그 전제가 바뀌면 표식을 시각이 아니라
     * <b>실행 고유값</b>(run_id 컬럼)으로 올려야 한다. 스키마 변경이라 이 티켓 밖이다.
     */
    private static final String LAST_EXPIRED_ID = """
            SELECT COALESCE(MAX(id), :afterId)
              FROM issuances
             WHERE status = 'EXPIRED'
               AND updated_at = :committedAt
               AND expires_at < :asOf
               AND id > :afterId
            """;

    /**
     * {@code from_status} 를 {@code 'ISSUED'} 로 못 박는다. 넘어온 경로가 그것뿐이기 때문이다 —
     * {@code EXPIRE_BATCH} 가 {@code ISSUED} 만 매치한다.
     */
    private static final String APPEND_HISTORIES = """
            INSERT INTO issuance_histories
                        (issuance_id, event_type, from_status, to_status, reason, created_at)
            SELECT id, 'EXPIRE', 'ISSUED', 'EXPIRED', '만료 배치', :committedAt
              FROM issuances
             WHERE status = 'EXPIRED'
               AND updated_at = :committedAt
               AND expires_at < :asOf
               AND id > :afterId AND id <= :lastId
            """;

    /**
     * 회차별로 센 만큼 뺀다. 회차마다 한 번만 갱신되도록 파생테이블에서 먼저 접는다 —
     * 건마다 갱신하면 같은 행을 여러 번 때리고, 그 사이 발급이 들어오면 순서에 따라 값이 갈린다.
     *
     * <p><b>{@code updated_at} 은 {@code GREATEST} 로 민다.</b> 이 문장은 청크의 마지막이라,
     * 앞 다섯 문장이 도는 동안 다른 트랜잭션이 같은 재고 행에 더 늦은 시각을 써 둘 수 있다.
     * 그것을 덮어써서 <b>시각이 뒤로 물러나면</b> 검증의 {@code hasStocksUpdatedAfter} 가
     * 그 변경을 못 본다 — 그 가드는 재고 시각이 단조 증가한다는 전제 위에 서 있고,
     * 이력 축과 달리 재고 축에는 백데이트 전용 가드가 따로 없다.
     * {@code VerificationSeed.syncStock} 이 같은 이유로 이미 {@code GREATEST} 를 쓴다 —
     * 시드가 지키는 규약을 운영 SQL 이 안 지키고 있었다.
     *
     * <p><b>{@code active_count >= x.expired} 가 음수를 막는다.</b> {@code ck_stock_range} 에만
     * 기대면 그 제약을 떼어 낸 CORRUPT 스키마에서 음수가 그대로 커밋된다 — 불변식을 DB 제약으로
     * 표현한다는 원칙이 스키마에 따라 무력해지는 자리다. 조건으로도 걸어 두면 어느 스키마에서든
     * 그 회차만 갱신되지 않고, 호출자가 {@code STOCK_ROW_COUNT} 와 대조해 알아챈다.
     */
    private static final String RELEASE_STOCK = """
            UPDATE coupon_stocks s
              JOIN (SELECT coupon_id, COUNT(*) AS expired
                      FROM issuances
                     WHERE status = 'EXPIRED'
                       AND updated_at = :committedAt
                       AND expires_at < :asOf
                       AND id > :afterId AND id <= :lastId
                     GROUP BY coupon_id) x ON x.coupon_id = s.coupon_id
               SET s.active_count = s.active_count - x.expired,
                   s.updated_at   = GREATEST(s.updated_at, :committedAt)
             WHERE s.active_count >= x.expired
            """;

    private static final String EXPIRED_COUPON_COUNT = """
            SELECT COUNT(DISTINCT coupon_id)
              FROM issuances
             WHERE status = 'EXPIRED'
               AND updated_at = :committedAt
               AND expires_at < :asOf
               AND id > :afterId AND id <= :lastId
            """;

    /**
     * 재고 행이 <b>실제로 있는</b> 회차 수. {@code RELEASE_STOCK} 의 {@code JOIN} 이 닿는 범위와
     * 같은 집합을 센다 — 다른 것은 {@code active_count >= x.expired} 조건뿐이다.
     * 그 차이가 곧 "뺄 재고가 모자란 회차" 이고, 그것을 갈라 보려고 이 문장이 있다.
     */
    private static final String STOCK_ROW_COUNT = """
            SELECT COUNT(*)
              FROM coupon_stocks s
              JOIN (SELECT coupon_id
                      FROM issuances
                     WHERE status = 'EXPIRED'
                       AND updated_at = :committedAt
                       AND expires_at < :asOf
                       AND id > :afterId AND id <= :lastId
                     GROUP BY coupon_id) x ON x.coupon_id = s.coupon_id
            """;

    private final JdbcClient jdbcClient;

    public ExpirationJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public int expireBatch(LocalDateTime asOf, LocalDateTime committedAt, long afterId, int limit,
            List<Long> blockedCoupons) {
        return jdbcClient.sql(EXPIRE_BATCH)
                .param("asOf", asOf)
                .param("committedAt", committedAt)
                .param("afterId", afterId)
                .param("limit", limit)
                .param("blockedCoupons", withSentinel(blockedCoupons))
                .update();
    }

    @Override
    public List<Long> blockedCoupons(LocalDateTime asOf) {
        return jdbcClient.sql(BLOCKED_COUPONS)
                .param("asOf", asOf)
                .query(Long.class)
                .list();
    }

    @Override
    public long lastExpiredId(LocalDateTime asOf, LocalDateTime committedAt, long afterId) {
        return jdbcClient.sql(LAST_EXPIRED_ID)
                .param("asOf", asOf)
                .param("committedAt", committedAt)
                .param("afterId", afterId)
                .query(Long.class)
                .single();
    }

    @Override
    public int appendExpireHistories(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId) {
        return chunk(APPEND_HISTORIES, asOf, committedAt, afterId, lastId).update();
    }

    @Override
    public int expiredCouponCount(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId) {
        return chunk(EXPIRED_COUPON_COUNT, asOf, committedAt, afterId, lastId)
                .query(Integer.class)
                .single();
    }

    @Override
    public int releaseStock(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId) {
        return chunk(RELEASE_STOCK, asOf, committedAt, afterId, lastId).update();
    }

    @Override
    public int stockRowCount(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId) {
        return chunk(STOCK_ROW_COUNT, asOf, committedAt, afterId, lastId)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>{@code NOT IN ()} 은 문법 오류다</b>(1064). 목록이 비는 것이 정상이므로 —
     * 오염이 없는 날이 대부분이다 — 빈 목록을 그대로 넘길 수 없다.
     *
     * <p>SQL 을 두 벌로 가르는 대신 <b>회차 id 가 될 수 없는 값</b>을 항상 넣는다.
     * {@code coupons.id} 는 auto-increment 라 음수가 없다. 문자열을 두 벌로 두면
     * 한쪽만 고치는 사고가 나고, 그 사고는 조건이 통째로 빠진 채 조용히 돈다.
     */
    private static List<Long> withSentinel(List<Long> blockedCoupons) {
        List<Long> withSentinel = new ArrayList<>(blockedCoupons);
        withSentinel.add(NOT_A_COUPON_ID);
        return withSentinel;
    }

    /**
     * 청크를 가리키는 파라미터 넷은 뒤 문장 전부가 똑같이 쓴다.
     *
     * <p>한 곳에 모으는 이유는 손이 덜 가서가 아니라, <b>한 문장만 상한을 빠뜨리는 일을 막으려는
     * 것</b>이다. 상한이 없는 문장 하나면 그 문장이 테이블 끝까지 공유 락을 잡아 발급이 멈춘다 —
     * 클래스 주석의 실측이 그 값이다.
     */
    private JdbcClient.StatementSpec chunk(String sql, LocalDateTime asOf,
            LocalDateTime committedAt, long afterId, long lastId) {
        return jdbcClient.sql(sql)
                .param("asOf", asOf)
                .param("committedAt", committedAt)
                .param("afterId", afterId)
                .param("lastId", lastId);
    }
}
