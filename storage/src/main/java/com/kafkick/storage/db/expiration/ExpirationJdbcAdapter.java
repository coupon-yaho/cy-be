// 만료 처리 SQL 일곱입니다 — 청크가 쓰는 다섯과, 실행당 한 번 도는 읽기 둘입니다. 청크의 첫 쓰기 락은 재고 행 하나를 FOR UPDATE 로 잡는 읽기입니다.
package com.kafkick.storage.db.expiration;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.expiration.ExpirationRepository;
import com.kafkick.core.expiration.ExpireCandidate;
import com.kafkick.core.expiration.PendingExpiration;

/**
 * <b>다섯 문장이 하나의 청크를 이룬다.</b> 후보를 읽고 · 재고를 잠그고 · 넘기고 · 이력을
 * 남기고 · 재고를 되돌린다. 다섯이 한 트랜잭션 안에서 돌아야 한다 — 상태만 바뀌고
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
 *   <li><b>격리가 RR 로 되돌아가는 날</b> — {@code INSERT … SELECT} 의 원본 읽기가 공유
 *       next-key 잠금 읽기가 되어 supremum 까지 잠근다. 그때 이 상한이 발급 봉쇄를 막는
 *       마지막 겹이다. ({@code UPDATE … JOIN} 은 이제 없다 — 청크가 회차 하나라
 *       {@code RELEASE_STOCK} 이 PK 단건 {@code UPDATE} 다.)
 * </ol>
 *
 * <p><b>상한이 발급 봉쇄를 푼 것이 아니다.</b> 그것을 푼 것은 READ COMMITTED 이고, 상한은
 * 첫 문장에 걸 수도 없다. 예전 주석이 "상한을 걸면 발급이 통과한다" 고 적었는데
 * <b>만료 대상이 {@code LIMIT} 을 채우는 조건에서만 그랬다</b> —
 * {@code docs/12-expire-lock-measurement.md} 의 <i>"실측이 뒤집은 것"</i> 절이 그 문장을
 * 철회했다.
 *
 * <p><b>다섯 중 셋이 상태를 바꾼다.</b> {@code NEXT_CANDIDATES} 는 락 없는 읽기이고
 * {@code LOCK_STOCK} 은 잠그기만 한다.
 *
 * <p><b>재고 행 없음과 재고 모자람은 서로 다른 자리에서 갈린다</b> — 없음은
 * {@code LOCK_STOCK} 이 빈 결과로, 모자람은 {@code RELEASE_STOCK} 의 갱신 행 수 0 으로.
 * 한때는 넘긴 <i>뒤에</i> {@code EXPIRED_COUPON_COUNT} 와 {@code STOCK_ROW_COUNT} 를 견줘
 * 알았는데, 그 시점이면 이미 "재고 없이 만료된 상태" 가 트랜잭션 안에 만들어져 있었다.
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
     * <b>거르는 조건이 안에 있다.</b> {@code status = 'ISSUED'} 를 여기 두어, 후보를 읽은
     * 뒤에 사용된 건은 매치되지 않고 조용히 빠진다.
     *
     * <p><b>후보는 밖에서 읽는다</b>({@code NEXT_CANDIDATES}) — 잠글 재고 행을 쓰기 전에
     * 알아야 하기 때문이다. 한때 그 방식을 <i>"그 건들 때문에 진도가 안 나가는 청크가
     * 생긴다"</i> 로 반려했는데, 그때는 종료 신호가 이 문장의 반환값이었다. 지금은 후보
     * 0건이 종료 신호라 넘긴 것이 0 이어도 진도가 나간다.
     *
     * <p>{@code useAffectedRows=false} 라 드라이버가 <b>매치된 행 수</b>를 돌려준다.
     * 여기서는 매치된 행이 전부 실제로 바뀌므로 두 값이 같다 — 같은 값을 다시 쓰는
     * {@code UPDATE} 를 여기 추가하면 그 등식이 깨진다.
     *
     * <p><b>이 문장은 이제 {@code (afterId, lastId]} 와 회차로 닫혀 있다.</b> 범위를 정하는
     * 것은 {@code NEXT_CANDIDATES} 이고, 그쪽을 받치는 인덱스는 형상에 따라 갈린다 —
     * 만료 대상이 살아 있는 {@code ISSUED} 중 일부면 {@code V11 (status, expires_at)},
     * 대부분이면 {@code (status, id)}. 옵티마이저가 고른다. 수치는
     * {@code docs/12-expire-lock-measurement.md} §11 에 있다.
     *
     * <p>한때는 이 문장이 {@code ORDER BY id LIMIT} 로 직접 후보를 골랐고, 그때
     * {@code V11} 이 없으면 옵티마이저가 PRIMARY 를 끝까지 훑어 <b>신규 발급 INSERT 를
     * 죽였다</b> — 실측(200,000행 중 뒤 1,000건만 대상): 읽은 행 201,000 → <b>2,001</b>.
     *
     * <p><b>인덱스만으로는 발급 봉쇄가 안 풀렸다.</b> 막던 것이 스캔 범위가 아니라 보조 인덱스의
     * gap 이라, 만료 Step 의 격리를 READ COMMITTED 로 내려서 풀었다({@code ExpireJobConfig}).
     * 둘은 서로 다른 문제를 푼다 — 인덱스는 스캔 비용, 격리는 가용성.
     * 수치와 재현 방법은 {@code docs/12-expire-lock-measurement.md} 에 있다.
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
               AND coupon_id = :couponId
               AND id > :afterId AND id <= :lastId
            """;

    /**
     * 후보를 <b>id 오름차순</b>으로 준다. 그 순서가 {@code ExpireChunk.from} 의 계약이다.
     *
     * <p><b>{@code NOT IN (:blockedCoupons)} 이 여기 있다.</b> 예전에는 {@code EXPIRE_BATCH} 에
     * 있었는데, 이제 그 문장이 회차 하나로 좁혀져서 걸 자리가 없다 — 막힌 회차는 후보에
     * 아예 안 들어와야 한다. 들어오면 그 회차가 청크를 잡고, 재고를 못 빼서 죽는다.
     */
    private static final String NEXT_CANDIDATES = """
            SELECT id, coupon_id
              FROM issuances
             WHERE status = 'ISSUED'
               AND expires_at < :asOf
               AND id > :afterId
               AND coupon_id NOT IN (:blockedCoupons)
             ORDER BY id
             LIMIT :limit
            """;

    /**
     * <b>청크의 첫 쓰기 락이다.</b> 발급·취소·사용취소가 잠그는 그 행을 같은 방식으로 잡는다.
     *
     * <p>{@code coupon_id} 만 고른다 — 값을 안 읽는 것이 결정이다. 뺄 수 있는지는
     * {@code RELEASE_STOCK} 의 조건이 판단한다.
     */
    private static final String LOCK_STOCK = """
            SELECT coupon_id FROM coupon_stocks WHERE coupon_id = :couponId FOR UPDATE
            """;

    /**
     * 만료 대기 건수를 막힌 몫과 함께 한 번에 센다.
     *
     * <p>두 번 세면 그 사이에 값이 움직여 {@code total < blocked} 같은 조합이 나올 수 있다 —
     * 게이지 둘이 서로 어긋나면 알림 식({@code total - blocked})이 음수가 된다.
     *
     * <b>{@code committedAt} 이후에 이력이 붙은 행은 이 실행의 몫이 아니다.</b> 실행이 끝난
     * 뒤 {@code CANCEL_USE}({@code USED → ISSUED})로 돌아온 행이 그렇다 — 그때 그 행은
     * {@code ISSUED} 도 아니었으므로 만료가 건드릴 대상이 아니었다.
     *
     * <p>⚠️ <b>{@code updated_at <= :committedAt} 으로 안 쓴다.</b> 그것도 같은 행을 걸러
     * 내지만 <b>영영 안 걷히는 행까지 함께 숨긴다</b> — {@code updated_at} 이 미래인 행은
     * 만료 잡의 창({@code EXPIRE_BATCH} 의 {@code updated_at <= :committedAt})에도 안 걸려
     * <b>다음 실행에서도, 그다음에도 계속 건너뛴다.</b> 사람이 봐야 하는 상태인데 지표에서
     * 사라지면 아무도 모른다. 이력 축은 그 둘을 가른다 — 되돌아온 행은 이력이 있고,
     * 미래 시각으로 멈춘 행은 없다.
     *
     * <p><b>비용을 쟀다</b>(300만 발급 · 520만 이력 · 대기 115만이라는 최악 형상):
     *
     * <pre>
     *   updated_at 판        0.94초   ← 싸지만 위 사각지대가 생긴다
     *   NOT EXISTS 판        1.74초   ← 이것
     *   LEFT JOIN … IS NULL  2.20초   계획은 인덱스를 타는데 1.45M 조회라 더 느리다
     * </pre>
     *
     * {@code batch.metrics.expire-pending-timeout-ms}(기본 5,000)에 <b>3배 여유</b>다.
     * ⚠️ <b>다시 볼 기준</b> — 평상시 대기는 8,183건이라 이 값은 상한이지만, 이력이 더
     * 자라 이 질의가 3초에 닿으면 그때는 {@code idx_history_issuance}
     * ({@code 90_perf_indexes_optional.sql})를 처방하거나 축을 다시 고른다.
     */
    private static final String COUNT_PENDING = """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN coupon_id IN (:blockedCoupons) THEN 1 ELSE 0 END) AS blocked
              FROM issuances
             WHERE status = 'ISSUED'
               AND expires_at < :asOf
               AND (:committedAt IS NULL OR NOT EXISTS (
                       SELECT 1 FROM issuance_histories h
                        WHERE h.issuance_id = issuances.id
                          AND h.created_at > :committedAt))
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
     * {@code from_status} 를 {@code 'ISSUED'} 로 못 박는다. 넘어온 경로가 그것뿐이기 때문이다 —
     * {@code EXPIRE_BATCH} 가 {@code ISSUED} 만 매치한다.
     *
     * <p><b>방금 넘긴 집합을 {@code updated_at = :committedAt} 표식으로 되찾는다.</b>
     * MySQL 에 {@code UPDATE … RETURNING} 이 없어서다.
     *
     * <p><b>그 표식은 고유하지 않다 — 무엇이 그것을 메우는지 적어 둔다.</b>
     * {@code committedAt} 은 {@code datetime(6)} 시각일 뿐이라, <b>원리적으로는</b> 다른
     * 프로세스가 같은 마이크로초에 {@code EXPIRED} 를 써서 이 집합에 섞일 수 있다.
     * 지금 그것을 막는 것은 <b>셋</b>이다.
     *
     * <ol>
     *   <li><b>같은 {@code asOf} 로 두 번 못 돈다.</b> {@code asOf} 가 잡 파라미터라 스프링
     *       배치가 같은 파라미터의 실행을 거부한다. <b>단, 그 사실이 DB 에 남아야 성립한다</b> —
     *       {@code BatchJobRepositoryConfig} 가 없으면 저장소가
     *       {@code ResourcelessJobRepository} 라 인스턴스가 한 줄도 안 남고, 프로세스가 둘이면
     *       서로를 못 본다.</li>
     *   <li>{@code id > :afterId AND id <= :lastId} 가 구간 밖을 막는다
     *       (그 축은 {@code ExpirationJdbcAdapterTest} 가 실제로 남의 행을 심어 확인한다).</li>
     *   <li><b>{@code coupon_id = :couponId} 가 구간 <i>안</i>의 남의 회차를 막는다.</b>
     *       {@code (afterId, lastId]} 는 회차가 섞일 수 있다 — 연속부 자르기는 후보 목록
     *       안에서만 연속이라, 그 사이에 다른 회차의 행이 id 로 끼어 있을 수 있다.
     *       청크가 회차 하나로 정해진 뒤로는 이 조건을 거는 비용이 0 이라, 표식의
     *       유일성에 기댈 이유가 없어졌다.</li>
     * </ol>
     *
     * <p>그래도 남는 구멍은 <b>같은 마이크로초 · 같은 회차 · 구간 안쪽</b>뿐이다. 배치가 한 대인
     * 지금은 닿을 수 없고, 그 전제가 바뀌면 표식을 시각이 아니라 <b>실행 고유값</b>(run_id
     * 컬럼)으로 올려야 한다. 스키마 변경이라 이 티켓 밖이다.
     */
    private static final String APPEND_HISTORIES = """
            INSERT INTO issuance_histories
                        (issuance_id, event_type, from_status, to_status, reason, created_at)
            SELECT id, 'EXPIRE', 'ISSUED', 'EXPIRED', '만료 배치', :committedAt
              FROM issuances
             WHERE status = 'EXPIRED'
               AND updated_at = :committedAt
               AND expires_at < :asOf
               AND coupon_id = :couponId
               AND id > :afterId AND id <= :lastId
            """;

    /**
     * 넘어간 만큼 한 번에 뺀다. <b>회차가 하나라 접을 것이 없다</b> — 한때는
     * {@code JOIN … GROUP BY} 로 회차별 합계를 접었는데, 그때는 청크가 여러 회차에 걸쳤다.
     *
     * <p><b>{@code updated_at} 은 {@code GREATEST} 로 민다.</b> 우리가 이 행을 잠그기 전에
     * 다른 트랜잭션이 더 늦은 시각을 써 두고 커밋했을 수 있다.
     * 그것을 덮어써서 <b>시각이 뒤로 물러나면</b> 검증의 {@code hasStocksUpdatedAfter} 가
     * 그 변경을 못 본다 — 그 가드는 재고 시각이 단조 증가한다는 전제 위에 서 있고,
     * 이력 축과 달리 재고 축에는 백데이트 전용 가드가 따로 없다.
     * {@code VerificationSeed.syncStock} 이 같은 이유로 이미 {@code GREATEST} 를 쓴다 —
     * 시드가 지키는 규약을 운영 SQL 이 안 지키고 있었다.
     *
     * <p><b>{@code active_count >= :expired} 가 음수를 막는다.</b> {@code ck_stock_range} 에만
     * 기대면 그 제약을 떼어 낸 CORRUPT 스키마에서 음수가 그대로 커밋된다 — 불변식을 DB 제약으로
     * 표현한다는 원칙이 스키마에 따라 무력해지는 자리다. 조건으로도 걸어 두면 어느 스키마에서든
     * 갱신 행 수가 0 이 되고, 호출자가 그것으로 알아챈다.
     */
    private static final String RELEASE_STOCK = """
            UPDATE coupon_stocks
               SET active_count = active_count - :expired,
                   updated_at   = GREATEST(updated_at, :committedAt)
             WHERE coupon_id = :couponId
               AND active_count >= :expired
            """;


    private final JdbcClient jdbcClient;

    public ExpirationJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<ExpireCandidate> nextCandidates(LocalDateTime asOf, long afterId, int limit,
            List<Long> blockedCoupons) {
        return jdbcClient.sql(NEXT_CANDIDATES)
                .param("asOf", asOf)
                .param("afterId", afterId)
                .param("limit", limit)
                .param("blockedCoupons", withSentinel(blockedCoupons))
                .query((rs, rowNum) -> new ExpireCandidate(rs.getLong("id"), rs.getLong("coupon_id")))
                .list();
    }

    @Override
    public boolean lockStock(long couponId) {
        return !jdbcClient.sql(LOCK_STOCK)
                .param("couponId", couponId)
                .query(Long.class)
                .list()
                .isEmpty();
    }

    @Override
    public int expireBatch(LocalDateTime asOf, LocalDateTime committedAt, long afterId, long lastId,
            long couponId) {
        return jdbcClient.sql(EXPIRE_BATCH)
                .param("asOf", asOf)
                .param("committedAt", committedAt)
                .param("afterId", afterId)
                .param("lastId", lastId)
                .param("couponId", couponId)
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
    public PendingExpiration countPending(LocalDateTime asOf, LocalDateTime committedAt,
            List<Long> blockedCouponIds) {
        return jdbcClient.sql(COUNT_PENDING)
                .param("asOf", asOf)
                .param("committedAt", committedAt)
                .param("blockedCoupons", withSentinel(blockedCouponIds))
                .query((rs, rowNum) -> new PendingExpiration(rs.getLong(1), rs.getLong(2)))
                .single();
    }


    @Override
    public int appendExpireHistories(LocalDateTime asOf, LocalDateTime committedAt,
            long afterId, long lastId, long couponId) {
        return jdbcClient.sql(APPEND_HISTORIES)
                .param("asOf", asOf)
                .param("committedAt", committedAt)
                .param("afterId", afterId)
                .param("lastId", lastId)
                .param("couponId", couponId)
                .update();
    }


    @Override
    public int releaseStock(long couponId, int expired, LocalDateTime committedAt) {
        return jdbcClient.sql(RELEASE_STOCK)
                .param("couponId", couponId)
                .param("expired", expired)
                .param("committedAt", committedAt)
                .update();
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

}
