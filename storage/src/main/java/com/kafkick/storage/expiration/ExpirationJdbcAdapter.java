// 만료 처리 SQL 여섯입니다. 전부 집합 단위로 돌고, 행을 미리 골라 두는 잠금 읽기를 쓰지 않습니다.
package com.kafkick.storage.expiration;

import java.time.LocalDateTime;

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
 * <p><b>상한이 빠지면 발급이 멈춘다.</b> {@code INSERT … SELECT}(이력)와
 * {@code UPDATE … JOIN}(재고)의 원본 읽기는 consistent read 가 아니라 <b>공유 next-key 잠금
 * 읽기</b>다. 위쪽이 열려 있으면 supremum 까지 잠겨 {@code issuances} 로의 신규 INSERT 가
 * 대기하다 오류 1205 로 죽는다. 실측(5,000행 중 1,000건 만료): 상한 없음 락 5,020(S 4,016) ·
 * 발급 INSERT 실패 → {@code id <= :lastId} 추가 시 락 1,004 · 발급 통과.
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
     * <p><b>{@code status}·{@code expires_at} 에 인덱스가 없다.</b> 그래서 옵티마이저가 PRIMARY 를
     * id 순으로 훑으면서 <b>지나간 행을 전부 X 락으로 잡는다.</b> 넘길 것이 하나도 없는 실행이
     * 최악인데, {@code LIMIT} 이 발동할 일이 없어 끝까지 훑은 뒤 0 을 돌려주기 때문이다.
     * 5분 주기라 하루 288회 중 대부분이 그 경로이고, 그동안 만료 대상이 아닌 발급건까지 걸린다.
     *
     * <p><b>이것은 알고 두는 상태다.</b> 보조 인덱스를 뺀 것은 누락이 아니라, 느린 쿼리를 직접
     * 겪고 개선폭을 수치로 재서 도입 시점을 정하기 위해서다(cy-seed 의
     * {@code ddl/90_perf_indexes_optional.sql}). 지금 무엇을 얼마나 잠그는지는
     * {@code ExpirationLockScopeTest} 가 수치로 못 박아 둔다 — 도입을 판단할 근거가 거기 있다.
     */
    private static final String EXPIRE_BATCH = """
            UPDATE issuances
               SET status = 'EXPIRED', updated_at = :committedAt
             WHERE status = 'ISSUED'
               AND expires_at < :asOf
               AND id > :afterId
             ORDER BY id
             LIMIT :limit
            """;

    /**
     * <b>이 문장만 상한이 없다.</b> 상한을 구하는 것이 이 문장의 일이라서다.
     *
     * <p><b>대신 {@code expires_at < :asOf} 는 나머지와 똑같이 건다.</b> 이 문장이 만드는
     * {@code lastId} 가 뒤 문장 전부의 창이 되므로, 여기서 남의 행이 {@code MAX(id)} 를
     * 밀어 올리면 <b>그 창이 통째로 넓어진다.</b> 다른 다섯에만 걸고 여기를 빼 두면
     * 겹을 하나 세워 놓고 문을 열어 두는 셈이다. 대신 평범한
     * {@code SELECT} 라 REPEATABLE READ 의 consistent read 로 돌고 <b>락을 잡지 않는다</b> —
     * 뒤 문장들과 달리 발급을 막지 않는다. 남는 것은 스캔 비용뿐이고, 그것은 인덱스가
     * 들어오는 날 함께 해결된다.
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
                   s.updated_at   = :committedAt
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
    public int expireBatch(LocalDateTime asOf, LocalDateTime committedAt, long afterId, int limit) {
        return jdbcClient.sql(EXPIRE_BATCH)
                .param("asOf", asOf)
                .param("committedAt", committedAt)
                .param("afterId", afterId)
                .param("limit", limit)
                .update();
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
