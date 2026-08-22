// 만료시킬 수 없는 회차를 어떻게 가르는지 확인합니다.
package com.kafkick.storage.db.expiration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.expiration.PendingExpiration;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>재고가 어긋난 회차를 창 밖으로 빼는 것이 이 티켓의 본체다.</b>
 *
 * <p>예전에는 넘긴 뒤에 가드가 그것을 발견하고 청크를 통째로 되돌렸다 — 오염 회차 하나가
 * 같은 청크의 남의 회차까지 되돌리고, 진도가 실행 사이로 안 넘어가니 다음 주기도 같은
 * 자리에서 죽어 <b>그 뒤 id 의 만료가 영구히 밀렸다.</b> 설계는
 * <i>"데이터가 틀렸다는 판정이 나와도 배치는 정상 종료"</i> 로 정했는데 그 반대였다.
 *
 * <p><b>막힘을 청크와 무관하게 정의하는 것이 핵심이다.</b> 청크 기준으로 정의하면 제외한 만큼
 * {@code LIMIT} 자리가 비어 다른 행이 창 안으로 들어오는데, 그 회차는 판정한 적이 없어
 * 또 막혀 있을 수 있다 — 재고 없이 만료된 상태가 커밋된다. 남은 대기 <b>전체</b>와 견주면
 * 제외 대상이 창 구성과 무관해진다.
 */
@RepositoryTest
@Import(ExpirationJdbcAdapter.class)
class BlockedCouponTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final LocalDateTime EXPIRED_AT = AS_OF.minusDays(1);
    private static final LocalDateTime WROTE_AT = AS_OF.plusMinutes(3);
    private static final int LIMIT_ABOVE_FIXTURE = 1000;

    @Autowired
    private ExpirationJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    @Test
    @DisplayName("재고 행이 없는 회차를 막힌 것으로 가른다")
    void marksCouponWithoutStockRow() {
        long broken = seed.newCoupon();
        expiring();
        seed.removeStock();

        assertThat(adapter.blockedCoupons(AS_OF))
                .as("**LEFT JOIN 이라야 보인다.** 안쪽 조인이면 재고 행 없는 회차가 조용히 빠져 "
                        + "가려는 두 경우 중 하나를 통째로 못 본다")
                .containsExactly(broken);
    }

    @Test
    @DisplayName("남은 대기를 다 빼면 음수가 되는 회차를 막힌 것으로 가른다")
    void marksCouponWhoseStockCannotCoverPending() {
        long broken = seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(1);

        assertThat(adapter.blockedCoupons(AS_OF))
                .as("대기 2건에 재고 1이면 부분 처리가 아니라 통째로 뺀다 — 1건만 넘기면 "
                        + "재고가 정확히 0이 되어 **이상이 안 보이게** 된다")
                .containsExactly(broken);
    }

    @Test
    @DisplayName("재고가 대기와 같으면 막힌 것이 아니다 — 경계는 열려 있다")
    void doesNotMarkCouponWhoseStockExactlyCoversPending() {
        seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(2);

        assertThat(adapter.blockedCoupons(AS_OF))
                .as("`<` 를 `<=` 로 바꾸면 정확히 맞는 회차까지 빠져 만료가 통째로 멈춘다")
                .isEmpty();
    }

    @Test
    @DisplayName("기한이 남은 건은 대기로 안 센다")
    void countsOnlyExpiredCandidatesAsPending() {
        seed.newCoupon();
        expiring();
        alive();
        alive();
        seed.overwriteStock(1);

        assertThat(adapter.blockedCoupons(AS_OF))
                .as("기한 남은 건까지 대기로 세면 성한 회차가 막힌 것으로 잡혀 만료가 멈춘다")
                .isEmpty();
    }

    /**
     * <b>막힌 회차를 넘기면 그 회차의 건은 안 넘어간다.</b> 그리고 <b>나머지는 넘어간다</b> —
     * 그것이 예전 동작(청크 통째 롤백)과 갈리는 지점이다.
     */
    @Test
    @DisplayName("막힌 회차는 제외하고 나머지는 그대로 넘긴다")
    void expiresHealthyCouponsWhileSkippingBlockedOnes() {
        long broken = seed.newCoupon();
        long brokenIssuance = expiring();
        seed.overwriteStock(0);

        seed.newCoupon();
        long healthyIssuance = expiring();
        seed.overwriteStock(1);

        List<Long> blocked = adapter.blockedCoupons(AS_OF);
        assertThat(blocked).containsExactly(broken);

        int expired = adapter.expireBatch(AS_OF, WROTE_AT, 0L, LIMIT_ABOVE_FIXTURE, blocked);

        assertThat(expired)
                .as("막힌 회차의 건만 빠지고 나머지는 넘어간다")
                .isEqualTo(1);
        assertThat(statusOf(brokenIssuance)).isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(statusOf(healthyIssuance))
                .as("**여기가 예전 동작과 갈린다.** 오염 회차 하나가 남의 회차까지 되돌리지 않는다")
                .isEqualTo(IssuanceStatus.EXPIRED.name());
    }

    /**
     * <b>목록이 비는 것이 정상이다.</b> 오염이 없는 날이 대부분이고, {@code NOT IN ()} 은
     * 문법 오류(1064)다. 센티널이 빠지면 만료가 <b>매 실행 SQL 오류로 죽는다.</b>
     */
    @Test
    @DisplayName("막힌 회차가 없어도 만료가 돈다 — NOT IN 빈 목록이 아니다")
    void expiresNormallyWhenNothingIsBlocked() {
        seed.newCoupon();
        long target = expiring();
        seed.overwriteStock(1);

        assertThat(adapter.blockedCoupons(AS_OF)).isEmpty();
        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, LIMIT_ABOVE_FIXTURE, List.of()))
                .isEqualTo(1);
        assertThat(statusOf(target)).isEqualTo(IssuanceStatus.EXPIRED.name());
    }

    /**
     * <b>게이지를 가르지 않으면 알림이 영원히 울린다.</b> 막힌 회차의 건은 설계상 계속
     * {@code ISSUED} 로 남으므로, 합쳐서 알림을 걸면 사람이 재고를 고칠 때까지 24시간 울리고
     * 그 알림은 곧 무시된다. 갈라야 <i>"배치가 일을 안 한다"</i> 와
     * <i>"데이터가 어긋나 있다"</i> 가 서로 다른 알림이 된다.
     */
    @Test
    @DisplayName("남은 대기를 막힌 몫과 나머지로 가른다")
    void splitsPendingIntoBlockedAndUnexplained() {
        long broken = seed.newCoupon();
        expiring();
        expiring();
        seed.overwriteStock(0);

        seed.newCoupon();
        expiring();
        seed.overwriteStock(1);

        PendingExpiration pending = adapter.countPending(AS_OF, List.of(broken));

        assertThat(pending.total()).isEqualTo(3);
        assertThat(pending.blocked()).isEqualTo(2);
        assertThat(pending.unexplained())
                .as("이 값이 0 이 아니면 **서버를 봐야 한다** — 배치가 처리했어야 하는 몫이다")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("막힌 회차가 없으면 남은 대기가 전부 설명 안 되는 몫이다")
    void countsEverythingAsUnexplainedWhenNothingIsBlocked() {
        seed.newCoupon();
        expiring();
        seed.overwriteStock(1);

        PendingExpiration pending = adapter.countPending(AS_OF, List.of());

        assertThat(pending.blocked())
                .as("센티널이 실수로 회차 id 와 겹치면 여기가 1 이 된다")
                .isZero();
        assertThat(pending.unexplained()).isEqualTo(1);
    }

    /**
     * <b>회귀 테스트다 — 캡처 창을 여기에 걸면 이 단언이 깨진다.</b>
     *
     * <p>{@code expireBatch} 는 {@code updated_at <= committedAt} 로 창을 닫고, 그
     * {@code committedAt} 은 <b>청크마다 새로 잡힌다.</b> 제외 판정에도 같은 창을 걸면
     * 판정은 첫 청크 시각으로 고정되고 만료는 그보다 넓은 창으로 도는데, 그 틈에 있는 행은
     * <b>대기로 안 세졌는데 만료는 된다.</b> 그러면 회차별 차감 합계가 판정이 본 대기를 넘어
     * {@code active_count} 를 초과하고 {@code STOCK_UNDERFLOW} 로 잡이 죽는다 —
     * 이 제외 논리가 없애려던 바로 그 실패다.
     *
     * <p>창을 빼면 이 값이 <b>그 실행이 넘길 수 있는 모든 행의 상계</b>가 되어, 어떤
     * {@code committedAt} 수열에서도 부등식이 선다.
     */
    @Test
    @DisplayName("판정 시각 뒤에 갱신된 행도 대기로 센다 — 캡처 창을 안 건다")
    void countsRowsUpdatedAfterTheCaptureWindow() {
        long broken = seed.newCoupon();
        expiring();
        long later = expiring();
        seed.overwriteStock(1);

        // 첫 청크가 잡은 시각보다 뒤에 상태가 움직인 행. 뒤 청크의 창에는 들어온다.
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", WROTE_AT.plusSeconds(30))
                .param("id", later)
                .update();

        assertThat(adapter.blockedCoupons(AS_OF))
                .as("창을 걸면 대기가 1로 보여 재고 1이 덮는 것처럼 되고 이 회차가 빠진다. "
                        + "그러면 뒤 청크가 2건을 넘겨 재고가 음수가 되고 잡이 죽는다")
                .containsExactly(broken);
    }

    /** 기한이 지난 발급건 하나. 현재 회차에 붙는다. */
    private long expiring() {
        return withExpiry(EXPIRED_AT);
    }

    /** 기한이 남은 발급건 하나. */
    private long alive() {
        return withExpiry(AS_OF.plusDays(30));
    }

    private long withExpiry(LocalDateTime expiresAt) {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", expiresAt)
                .param("id", id)
                .update();
        return id;
    }

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }
}
