// 회차 상태 전이를 id 하나씩 조건부로 바꿉니다.
package com.kafkick.storage.db.coupon.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.coupon.port.CouponRoundRepository;

/**
 * <b>JPA 를 안 쓴다.</b> 하는 일이 조건부 {@code UPDATE} 의 매치 건수를 보는 것뿐이라
 * 엔티티도 매퍼도 필요 없다 — 영속성 컨텍스트를 얹으면 <b>읽고-바꾸고-쓰는</b> 모양이 되어
 * 조건부 갱신의 원자성이 사라진다. {@code repository/} 에 두는 것은 README 가 그 패키지를
 * <i>"JpaRepository + core port 구현체"</i> 로 정의하기 때문이다.
 *
 * <p><b>SQL 리터럴을 {@link CouponStatus} 에서 뽑는다.</b> {@code WHERE}·{@code SET} 에 들어가는
 * 값이라 바인딩할 자리가 아니지만, 문자열을 손으로 적으면 열거형과 <b>코드로 이어지지 않는다</b> —
 * 그 상태에서 열거형은 죽은 코드이고, 이름을 바꾸면 여기가 조용히 안 맞는다.
 *
 * <p><b>전이마다 짧은 트랜잭션을 연다.</b> 한 회차 = 한 트랜잭션이라 <i>"tick 하나가 대상
 * 전부를 잠근 채로"</i> 가 되지 않는다. 여는 이유는 <b>둘</b>이다.
 *
 * <ul>
 *   <li><b>데드라인.</b> 트랜잭션 밖이면 {@code DataSourceUtils} 가 {@code queryTimeout} 을
 *       안 붙여 <b>끊을 수단이 없다</b> — 발급이 그 회차 행을 쥐고 있으면
 *       {@code innodb_lock_wait_timeout}(기본 50초)까지 스케줄러 스레드 하나와 커넥션 하나를
 *       물고 있게 된다. 형제 되읽기 넷이 같은 이유로 같은 모양이다.</li>
 *   <li><b>{@code READ COMMITTED} — 이것이 발급을 살리는 결정이다.</b> 기본 격리수준에서는
 *       이 테이블을 훑는 {@code UPDATE} 가 스캔한 레코드 전부와 갭에 X 락을 잡아 발급 전
 *       상태 확인과 재고 소진 마감이 <b>둘 다 {@code ERROR 1205}</b> 였다(실측 표는 포트
 *       javadoc). 회차 하나만 보는 문장이라 팬텀을 신경 쓸 이유도 없다.</li>
 * </ul>
 */
@Repository
public class CouponRoundJdbcAdapter implements CouponRoundRepository {

    /**
     * <b>재고 행을 {@code EXISTS} 로 확인한다.</b> ⚠️ FK 는 <b>있다</b>
     * ({@code V1:635} — {@code coupon_stocks.coupon_id → coupons.id}). 다만 그 방향은
     * <i>재고 행이 회차를 가리키는</i> 것이라 <b>회차마다 재고 행이 있음을 강제하지 않는다.</b>
     * 그래서 여기서 직접 본다 — <i>"FK 를 걸면 이 {@code EXISTS} 를 뺄 수 있다"</i> 로 읽지 마라.
     * 빼면 재고 행 없는 회차가 열려 발급 경로가 그 회차에서 죽는다.
     *
     * <p><b>{@code close_at > :now} 도 함께 건다.</b> 창이 통째로 지난 회차를 열면
     * <b>마감 시각이 지난 회차에서 발급이 나간다</b> — 각 {@code UPDATE} 가 즉시 커밋되므로
     * 여는 루프가 도는 동안 그 회차는 실제로 {@code OPEN} 이고, 닫는 루프가 자기 차례에
     * 올 때까지 발급이 통과한다.
     *
     * <p>{@code ORDER BY id} 는 <b>오래된 회차부터</b>다. {@code open_at} 으로 정렬하지 않는
     * 이유는 하나 — 그 컬럼에 인덱스가 없어 정렬이 filesort 가 되는데, id 는 PK 라
     * 인덱스가 그대로 순서를 준다. 회차 id 는 생성 순이므로 {@code open_at} 순과 사실상 같다.
     */
    private static final String ROUNDS_TO_OPEN = """
            SELECT c.id
              FROM coupons c
             WHERE c.status = '%s'
               AND c.open_at <= :now
               AND c.close_at > :now
               AND EXISTS (SELECT 1 FROM coupon_stocks s WHERE s.coupon_id = c.id)
             ORDER BY c.id
            """.formatted(CouponStatus.SCHEDULED);

    private static final String ROUNDS_TO_CLOSE = """
            SELECT id
              FROM coupons
             WHERE status = '%s'
               AND close_at <= :now
             ORDER BY id
            """.formatted(CouponStatus.OPEN);

    /**
     * <b>{@code close_at} 을 손대지 않는다.</b> {@code SET} 절에 {@code status} 하나만 있는
     * 것이 계약이다 — {@code docs/02} F5.
     *
     * <p><b>{@code close_at > :now} 를 가드에도 넣는다.</b> 조회와 갱신 사이에 시각이 흐르는
     * 것이 아니라(같은 {@code now} 를 쓴다) <b>가드 하나만 봐도 안전한 것</b>이 계약이기
     * 때문이다 — 다음 사람이 조회를 고칠 때 이 문장이 스스로를 지킨다.
     */
    private static final String OPEN_ROUND = """
            UPDATE coupons SET status = '%s'
             WHERE id = :couponId AND status = '%s' AND close_at > :now
            """.formatted(CouponStatus.OPEN, CouponStatus.SCHEDULED);

    /**
     * <b>여는 문장과 같은 계약이다 — 가드 하나만 봐도 안전해야 한다.</b> 조회가 이미
     * {@code close_at <= now} 로 걸러 주지만, 다음 사람이 {@link #ROUNDS_TO_CLOSE} 에서 그
     * 조건을 떨어뜨리는 날 <b>진행 중인 회차가 전부 닫힌다</b>. 그때 이 문장이 스스로를 지킨다.
     *
     * <p>재고 소진 마감은 이 경로가 아니다(발급 경로가 인라인으로 한다) — 그래서
     * {@code close_at} 이 미래인 회차를 이 문장이 닫아야 할 이유가 없다.
     */
    private static final String CLOSE_ROUND = """
            UPDATE coupons SET status = '%s'
             WHERE id = :couponId AND status = '%s' AND close_at <= :now
            """.formatted(CouponStatus.CLOSED, CouponStatus.OPEN);

    /**
     * <b>넷을 한 문장으로 센다.</b> 문장을 나누면 {@code READ COMMITTED} 에서 문장마다
     * read view 가 새로 잡혀 네 값이 서로 다른 시점을 본다 — 그 사이 재고 행이 생기면
     * 회차 하나가 <b>어느 게이지에도 안 나온다</b>(포트 javadoc 에 시나리오를 적었다).
     *
     * <p>{@code SUM(조건)} 은 MySQL 에서 불리언을 1/0 으로 더한다. 대상이 없으면
     * {@code SUM} 이 {@code NULL} 이라 {@code COALESCE} 로 0 을 만든다 — 그러지 않으면
     * 회차가 하나도 없는 스키마에서 게이지가 통째로 {@code NaN} 이 되어
     * <b>되읽기 실패와 구분이 안 된다.</b>
     */
    private static final String COUNT_PENDING = """
            SELECT
              COALESCE(SUM(c.status = '%1$s' AND c.open_at <= :now AND c.close_at > :now
                           AND     EXISTS (SELECT 1 FROM coupon_stocks s
                                            WHERE s.coupon_id = c.id)), 0) AS pending_open,
              COALESCE(SUM(c.status = '%2$s' AND c.close_at <= :now), 0)   AS pending_close,
              COALESCE(SUM(c.status = '%1$s' AND c.close_at <= :now), 0)   AS missed_window,
              COALESCE(SUM(c.status = '%1$s' AND c.open_at <= :now AND c.close_at > :now
                           AND NOT EXISTS (SELECT 1 FROM coupon_stocks s
                                            WHERE s.coupon_id = c.id)), 0) AS blocked_no_stock
            FROM coupons c
            """.formatted(CouponStatus.SCHEDULED, CouponStatus.OPEN);

    private final JdbcClient jdbcClient;

    /**
     * <b>전이 한 건(쓰기)의 데드라인.</b> 되읽기의 손잡이와 <b>따로 판다</b> — 이쪽은 발급
     * 경로와 같은 행을 다투는 쓰기고 그쪽은 읽기라, 무거워지는 이유가 다르다.
     */
    private final TransactionTemplate perRoundWrite;

    /**
     * <b>조회용.</b> 같은 데드라인을 쓰되 {@code readOnly} 다.
     *
     * <p>⚠️ <b>되읽기가 부를 때는 이 설정이 안 걸린다.</b> {@code TransactionTemplate} 의
     * 기본 전파는 {@code REQUIRED} 라, {@code CouponRoundPendingRefresher} 의 트랜잭션 안에서
     * 불리면 <b>바깥 것에 참여</b>하고 이쪽의 격리·타임아웃·readOnly 는 무시된다
     * ({@code validateExistingTransaction} 기본값이 {@code false} 라 예외도 안 난다).
     * 그래서 이 손잡이가 실제로 사는 자리는 <b>스케줄러가 직접 부르는 대상 조회</b>다.
     */
    private final TransactionTemplate perRoundRead;

    public CouponRoundJdbcAdapter(JdbcClient jdbcClient,
            PlatformTransactionManager transactionManager,
            @Value("${batch.schedule.coupon-round-tx-timeout-ms:5000}") long txTimeoutMillis) {
        // 스프링 트랜잭션 타임아웃이 초 단위라 999 이하는 0 으로 잘리는데, 0 은 "무제한" 이
        // 아니라 데드라인이 이미 지났음이다 — 첫 문장에서 만료된다. 형제들과 같은 가드다.
        if (txTimeoutMillis < 1_000 || txTimeoutMillis % 1_000 != 0) {
            throw new IllegalArgumentException(
                    "batch.schedule.coupon-round-tx-timeout-ms 는 1000 이상이면서 1000 의 "
                            + "배수여야 합니다. 초 단위로 내림하기 때문에 999 이하는 0 초가 되고, "
                            + "그러면 첫 문장에서 트랜잭션이 만료됩니다. 받은 값=" + txTimeoutMillis);
        }
        this.jdbcClient = jdbcClient;
        int timeoutSeconds = Math.toIntExact(txTimeoutMillis / 1_000);
        this.perRoundWrite = new TransactionTemplate(transactionManager);
        // 회차 하나만 보는 문장이라 팬텀을 신경 쓸 이유가 없다. RR 이면 스냅샷을 새로 잡느라
        // 언두를 더 오래 붙든다 — 형제 되읽기들이 같은 판단을 했다.
        this.perRoundWrite.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.perRoundWrite.setTimeout(timeoutSeconds);
        this.perRoundRead = new TransactionTemplate(transactionManager);
        this.perRoundRead.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.perRoundRead.setTimeout(timeoutSeconds);
        this.perRoundRead.setReadOnly(true);
    }

    @Override
    public List<Long> roundsToOpen(LocalDateTime now) {
        return ids(ROUNDS_TO_OPEN, now);
    }

    @Override
    public List<Long> roundsToClose(LocalDateTime now) {
        return ids(ROUNDS_TO_CLOSE, now);
    }

    @Override
    public boolean open(long couponId, LocalDateTime now) {
        return transition(OPEN_ROUND, couponId, now);
    }

    @Override
    public boolean close(long couponId, LocalDateTime now) {
        return transition(CLOSE_ROUND, couponId, now);
    }

    @Override
    public PendingCounts countPending(LocalDateTime now) {
        return perRoundRead.execute(ignored -> jdbcClient.sql(COUNT_PENDING)
                .param("now", now)
                .query((rs, rowNum) -> new PendingCounts(
                        rs.getInt("pending_open"),
                        rs.getInt("pending_close"),
                        rs.getInt("missed_window"),
                        rs.getInt("blocked_no_stock")))
                .single());
    }

    /**
     * <b>조회도 데드라인 안에서 돈다.</b> 밖에 두면 스케줄러 스레드가 무기한 블록되고,
     * 그러면 호출자의 {@code catch} 에 도달하지 못해 <b>그 tick 이 조용히 사라진다</b>.
     */
    private List<Long> ids(String sql, LocalDateTime now) {
        return perRoundRead.execute(ignored ->
                jdbcClient.sql(sql).param("now", now).query(Long.class).list());
    }

    private boolean transition(String sql, long couponId, LocalDateTime now) {
        return Boolean.TRUE.equals(perRoundWrite.execute(ignored -> jdbcClient.sql(sql)
                .param("couponId", couponId)
                .param("now", now)
                .update() == 1));
    }
}
