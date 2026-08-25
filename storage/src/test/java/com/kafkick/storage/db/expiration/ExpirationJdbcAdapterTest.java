// 만료 SQL 여섯이 무엇을 건드리고 무엇을 안 건드리는지 확인합니다.
package com.kafkick.storage.db.expiration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.CouponStateMachine;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.expiration.ExpireChunk;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>이 잡은 재고를 쓴다.</b> 그래서 "무엇을 넘겼나" 만큼 <b>"무엇을 안 건드렸나"</b> 가 중요하다.
 * 사용된 건을 만료로 넘기면 재고가 두 번 돌아오고, 그 어긋남은 검증이 잡을 때까지 안 보인다.
 *
 * <p>{@code updated_at = committedAt} 표식과 {@code (afterId, lastId]} 구간으로 방금 넘긴
 * 집합을 다시 찾는 설계라, <b>앞 청크와 섞이지 않는지</b>도 여기서 본다.
 */
@RepositoryTest
@Import(ExpirationJdbcAdapter.class)
class ExpirationJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final LocalDateTime EXPIRED_AT = AS_OF.minusDays(1);
    private static final LocalDateTime ALIVE_AT = AS_OF.plusDays(1);

    /** 실제로 쓴 시각. asOf 보다 뒤다 — 잡은 asOf 를 정해 놓고 그 뒤에 돈다. */
    private static final LocalDateTime WROTE_AT = AS_OF.plusMinutes(3);
    /** 픽스처보다 크다 — 청크가 잘리지 않게 하려는 것이지 무제한이라는 뜻이 아니다. */
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

    /** 기한을 직접 세운다. 시드는 발급 시각만 받아서 만료 시각을 따로 정할 수단이 없다. */
    private long issuance(IssuanceStatus status, LocalDateTime expiresAt) {
        long id = seed.issuance(status);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", expiresAt)
                .param("id", id)
                .update();
        return id;
    }

    /**
     * <b>잡의 청크 한 번을 그대로 밟는다</b> — 후보 → 연속부 → 재고 잠금 → 만료.
     *
     * <p>여기서 {@code adapter.expireBatch} 를 곧장 부르면 <b>재고 행을 안 잠근 채</b> 발급건을
     * 먼저 건드리게 되고, 그건 이 저장소가 없애려는 바로 그 순서다. 테스트가 운영과 다른
     * 순서를 밟으면 계약을 재는 것이 아니라 계약을 비켜 가는 것이 된다.
     *
     * <p>{@link #chunkBoundary} 로 경계를 함께 남긴다 — 뒤 문장들(이력·재고)이 <b>같은
     * {@code (afterId, lastId]}</b> 를 봐야 하는데, 테스트마다 손으로 넘기면 한 곳만 빠뜨려도
     * 그 문장이 테이블 끝까지 훑는다.
     */
    private int expireChunk(long afterId, int limit) {
        ExpireChunk chunk = ExpireChunk.from(
                adapter.nextCandidates(AS_OF, afterId, limit, List.of()));
        chunkBoundary = chunk.lastId();
        chunkCouponId = chunk.couponId();
        if (chunk.isEmpty()) {
            return 0;
        }
        assertThat(adapter.lockStock(chunk.couponId()))
                .as("재고 행이 없으면 잡이 STOCK_ROW_MISSING 으로 죽는다. "
                        + "이 픽스처는 재고 행이 있어야 한다")
                .isTrue();
        return adapter.expireBatch(AS_OF, WROTE_AT, afterId, chunk.lastId(), chunk.couponId());
    }

    /** 마지막 {@link #expireChunk} 가 잡은 상한. 이력·재고 문장이 이 값을 받는다. */
    private long chunkBoundary;

    /** 마지막 {@link #expireChunk} 가 고른 회차. 재고 차감이 이 값을 받는다. */
    private long chunkCouponId;

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int activeCount() {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", seed.currentCouponId())
                .query(Integer.class)
                .single();
    }

    private int historyCount(long issuanceId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM issuance_histories "
                        + "WHERE issuance_id = :id AND event_type = 'EXPIRE'")
                .param("id", issuanceId)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>거르는 조건이 UPDATE 안에 있다는 것이 이 테스트의 요점이다.</b> 사용된 건과 아직 기한이
     * 남은 건은 매치 자체가 되지 않는다. 밖에서 후보를 뽑아 걸러내는 방식이었다면
     * 그 둘을 빼먹었을 때 여기서 안 드러난다.
     */
    @Test
    @DisplayName("기한 지난 ISSUED 만 넘긴다 — 사용된 건과 기한 남은 건은 그대로")
    void expireOnlyIssuedPastDue() {
        long target = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        long used = issuance(IssuanceStatus.USED, EXPIRED_AT);
        long alive = issuance(IssuanceStatus.ISSUED, ALIVE_AT);

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE)).isEqualTo(1);

        assertThat(statusOf(target)).isEqualTo("EXPIRED");
        assertThat(statusOf(used)).as("사용된 건을 넘기면 재고가 두 번 돌아온다").isEqualTo("USED");
        assertThat(statusOf(alive)).as("기한이 남았다").isEqualTo("ISSUED");
    }

    /**
     * <b>0 은 실패가 아니라 종료 신호다.</b> 조건이 UPDATE 안에 있어서, 0 이 나왔다는 것은
     * 곧 남은 대상이 없다는 뜻이다. 이 성질이 깨지면 잡이 같은 자리를 맴돈다.
     */
    @Test
    @DisplayName("남은 대상이 없으면 0 을 돌려준다")
    void returnZeroWhenNothingLeft() {
        issuance(IssuanceStatus.ISSUED, ALIVE_AT);

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE)).isZero();
    }

    /**
     * <b>앞 청크의 표식과 섞이면 안 된다.</b> 같은 실행 안에서 {@code asOf} 가 같으므로
     * {@code updated_at} 만으로는 청크를 못 가른다. {@code id > afterId} 가 그 경계다.
     */
    @Test
    @DisplayName("두 청크로 나눠 돌리면 뒤 청크가 앞 청크 것을 다시 세지 않는다")
    void secondChunkDoesNotRecountTheFirst() {
        long first = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        long second = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);

        seed.overwriteStock(2);

        // 청크 하나가 하는 일 전부를 순서대로 돌린다. 경계가 없으면 뒤 청크가 앞 청크 것까지
        // 다시 처리하는데, 그 피해는 경계값이 아니라 **이력과 재고**에서 드러난다.
        assertThat(expireChunk(0L, 1)).isEqualTo(1);
        long boundary = chunkBoundary;
        assertThat(boundary).isEqualTo(first);
        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, chunkBoundary, chunkCouponId)).isEqualTo(1);
        adapter.releaseStock(chunkCouponId, 1, WROTE_AT);

        assertThat(expireChunk(boundary, 1)).isEqualTo(1);
        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, boundary, chunkBoundary, chunkCouponId))
                .as("앞 청크 것을 다시 세면 2 가 되고 이력이 중복된다")
                .isEqualTo(1);
        adapter.releaseStock(chunkCouponId, 1, WROTE_AT);

        assertThat(historyCount(first)).as("한 건에 이력은 하나다").isEqualTo(1);
        assertThat(historyCount(second)).isEqualTo(1);
        assertThat(activeCount())
                .as("둘을 넘겼으니 2 에서 둘만 빠져 0 이다. 경계가 없으면 세 번 빠져 음수가 된다")
                .isZero();
    }

    /**
     * <b>이력이 없으면 검증이 그 발급건을 "이력 없는 발급건" 으로 잡는다.</b>
     * 상태만 바꾸고 이력을 빠뜨리면 원인이 이 잡이라는 것이 안 드러난다.
     */
    @Test
    @DisplayName("넘긴 건마다 EXPIRE 이력이 하나씩 남는다")
    void writeOneHistoryPerExpired() {
        long target = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        expireChunk(0L, LIMIT_ABOVE_FIXTURE);

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, chunkBoundary, chunkCouponId)).isEqualTo(1);
        assertThat(historyCount(target)).isEqualTo(1);
    }

    /**
     * <b>빼는 것이 맞다.</b> {@code active_count} 는 ISSUED + USED 합계라 만료는 거기서 빠진다.
     * 방향을 반대로 잡으면 완판 판정이 조용히 뒤집힌다 — 부호 하나가 대시보드를 거짓말하게 만든다.
     */
    @Test
    @DisplayName("넘긴 만큼 재고가 준다")
    void releaseStockByExpiredCount() {
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(2);

        expireChunk(0L, LIMIT_ABOVE_FIXTURE);
        assertThat(adapter.releaseStock(chunkCouponId, 2, WROTE_AT))
                .as("그 회차의 재고 행 하나를 갱신한다").isEqualTo(1);

        assertThat(activeCount()).as("2 에서 둘을 빼 0 이다").isZero();
    }

    /**
     * <b>이력 시각을 백데이트하면 리플레이가 순서를 뒤집는다.</b>
     *
     * <p>리플레이는 {@code ORDER BY issuance_id, created_at, id} 로 접는다 —
     * {@code created_at} 이 1순위다. 잡이 도는 동안 사용자가 사용을 취소하면 그 이력의
     * 시각은 실제 시각인데, 우리가 {@code asOf} 로 백데이트하면 <b>나중에 일어난 만료가
     * 먼저 접힌다.</b> 그러면 {@code USED → EXPIRE} 가 되는데 전이표에 없는 조합이라
     * V4 가 불법 전이를 올리고, 최종 상태까지 갈려 V3 도 같이 운다.
     *
     * <p>더미데이터에 <i>"USED 중 20% 복원"</i> 이 있어 이 상황은 반드시 발생한다.
     * 정상셋 0건이 원천적으로 불가능해지는 자리다.
     */
    @Test
    @DisplayName("만료 이력은 실제로 쓴 시각을 갖는다 — asOf 로 백데이트하면 순서가 뒤집힌다")
    void writeHistoryAtRealTimeNotAsOf() {
        long target = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        // 잡이 도는 사이 사용자가 취소해 이력이 하나 들어왔다. 시각은 asOf 보다 뒤다.
        seed.history(target, IssuanceEventType.CANCEL_USE, IssuanceStatus.USED,
                IssuanceStatus.ISSUED, AS_OF.plusMinutes(1));

        expireChunk(0L, LIMIT_ABOVE_FIXTURE);
        adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, chunkBoundary, chunkCouponId);

        LocalDateTime expireAt = jdbcClient.sql("SELECT created_at FROM issuance_histories "
                        + "WHERE issuance_id = :id AND event_type = 'EXPIRE'")
                .param("id", target)
                .query(LocalDateTime.class)
                .single();

        assertThat(expireAt)
                .as("**넘긴 committedAt 그대로여야 한다.** asOf 로 찍으면 취소(asOf+1분)보다 "
                        + "앞서 리플레이가 EXPIRE 를 먼저 접는다. 느슨하게 isAfter 만 보면 "
                        + "어댑터가 SQL NOW() 로 바뀌어도 통과하는데, 그러면 같은 asOf 재실행이 "
                        + "다른 값을 남겨 결정론이 깨진다")
                .isEqualTo(WROTE_AT);
    }

    /**
     * <b>표식만으로는 남의 행을 못 가른다 — id 구간이 그것을 가른다.</b> 예전에는
     * <i>"{@code EXPIRED} 를 쓰는 곳이 이 잡뿐"</i> 이라는 규칙에 기대야 했고, 그 규칙이 깨지면
     * 남의 발급건에 {@code EXPIRE} 이력이 붙었다. 이제 {@code (afterId, lastId]} 밖은 애초에
     * 매치되지 않는다.
     *
     * <p><b>겹이 둘이라 방향을 나눠 확인한다.</b> 남의 행이 우리 구간 <b>밖</b>이면 id 상한이
     * 막고, 우연히 구간 <b>안</b>이면 {@code expires_at < asOf} 가 막는다. 하나만 있으면
     * 나머지 방향이 뚫린다 — 인덱스가 들어와 락 범위가 좁아지는 날 특히 그렇다.
     */
    @Test
    @DisplayName("남이 같은 시각에 EXPIRED 를 써도 우리 구간 밖이면 안 섞인다")
    void markerIgnoresWritersAboveTheBoundary() {
        long ours = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        long theirs = issuance(IssuanceStatus.ISSUED, ALIVE_AT);

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE)).isEqualTo(1);
        long boundary = chunkBoundary;

        // 런타임이 상태를 직접 넘긴 것을 흉내낸다. 우리 청크가 끝난 뒤 생긴 행이다.
        jdbcClient.sql("UPDATE issuances SET status = 'EXPIRED', updated_at = :at WHERE id = :id")
                .param("at", WROTE_AT)
                .param("id", theirs)
                .update();

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundary, chunkCouponId))
                .as("id 상한이 남의 행을 잘라 낸다. 2 가 되면 상한이 빠진 것이다")
                .isEqualTo(1);
        assertThat(historyCount(ours)).isEqualTo(1);
        assertThat(historyCount(theirs))
                .as("우리가 넘기지 않은 건에는 이력이 안 붙는다")
                .isZero();
    }

    /**
     * <b>두 번째 겹.</b> 남의 행이 우리 구간 <b>안</b>에 있으면 id 상한이 못 막는다.
     * 그때는 {@code expires_at < asOf} 가 가른다 — 기한이 남은 건은 애초에 이 잡의 대상이 아니다.
     *
     * <p>지금은 무인덱스 풀스캔이라 우리 {@code UPDATE} 가 구간 전체를 X 락으로 쥐고 있어
     * 이 상황 자체가 잘 안 생긴다. 인덱스가 들어오면 매치한 행만 잠그므로 <b>그때부터 생긴다.</b>
     */
    @Test
    @DisplayName("구간 안이어도 기한이 남은 건은 우리 집합이 아니다")
    void markerIgnoresRowsStillWithinTerm() {
        long theirs = issuance(IssuanceStatus.ISSUED, ALIVE_AT);
        long ours = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE)).isEqualTo(1);
        long boundary = chunkBoundary;
        assertThat(theirs).as("남의 행이 우리 구간 안에 있어야 이 겹을 시험한다").isLessThan(boundary);

        jdbcClient.sql("UPDATE issuances SET status = 'EXPIRED', updated_at = :at WHERE id = :id")
                .param("at", WROTE_AT)
                .param("id", theirs)
                .update();

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundary, chunkCouponId))
                .as("기한이 남은 건은 expires_at 조건이 잘라 낸다")
                .isEqualTo(1);
        assertThat(historyCount(ours)).isEqualTo(1);
        assertThat(historyCount(theirs)).isZero();
    }

    /**
     * <b>재고 행이 없으면 발급건을 건드리기 전에 멈춘다.</b>
     *
     * <p>한때는 이것을 <b>넘긴 뒤에</b> 알았다 — 만료 {@code UPDATE} 를 돌리고 나서
     * {@code expiredCouponCount} 와 {@code stockRowCount} 를 견줘 갈렸다. 그 사이에 이미
     * <i>"재고 없이 만료된 상태"</i> 가 트랜잭션 안에 만들어져 있었고, 되돌리는 것은 롤백이었다.
     *
     * <p>이제 재고 행을 <b>먼저</b> 잠그므로 그 자리에서 알 수 있다. 아무것도 안 쓴 채로
     * 멈추는 편이 낫다 — 그리고 두 값을 대조하던 조회 둘이 통째로 없어졌다.
     */
    @Test
    @DisplayName("재고 행이 없는 회차는 잠금 단계에서 걸린다 — 발급건을 건드리기 전이다")
    void lockStockFailsWithoutStockRow() {
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        long couponId = seed.currentCouponId();
        seed.removeStock();

        ExpireChunk chunk = ExpireChunk.from(
                adapter.nextCandidates(AS_OF, 0L, LIMIT_ABOVE_FIXTURE, List.of()));
        assertThat(chunk.couponId())
                .as("후보에는 들어온다 — 거르는 것은 blockedCoupons 의 몫이고, "
                        + "그것이 놓친 경우가 이 테스트다")
                .isEqualTo(couponId);

        assertThat(adapter.lockStock(chunk.couponId()))
                .as("false 가 유일한 신호다. 잡은 이것을 STOCK_ROW_MISSING 으로 올린다")
                .isFalse();
        assertThat(statusOf(chunk.lastId()))
                .as("아직 아무것도 안 넘겼다 — 이 순서의 값이 여기 있다")
                .isEqualTo("ISSUED");
    }

    /**
     * <b>회차마다 자기 몫만 빠져야 한다.</b> 한 회차가 남의 몫까지 빼면 그 회차는 완판으로,
     * 다른 회차는 재고가 남은 것으로 보인다 — 그리고 <b>잡은 초록으로 끝난다.</b>
     * 검증이 재고 불일치로 잡을 때까지 아무도 모른다.
     *
     * <p><b>그것을 가르던 장치가 바뀌었다.</b> 예전에는 파생테이블의 {@code GROUP BY coupon_id}
     * 하나였다 — 청크가 여러 회차에 걸쳤기 때문이다. 이제는 {@link ExpireChunk} 가 청크를
     * 회차 하나로 자르므로 섞일 자리가 애초에 없다. <b>같은 것을 재되 무엇이 지키는지가
     * 달라졌으므로</b>, 이 테스트는 청크가 실제로 갈라지는지를 본다.
     */
    @Test
    @DisplayName("후보에 회차 둘이 섞이면 청크가 갈라 각자 자기 몫만 뺀다")
    void chunkSplitsAtCouponBoundary() {
        long couponA = seed.newCoupon();
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(5);

        long couponB = seed.newCoupon();
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(4);

        // 첫 청크 — LIMIT 이 넷을 다 담아도 회차 A 에서 끊긴다.
        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE))
                .as("후보 넷이 다 들어와도 회차 A 의 셋까지만 넘긴다")
                .isEqualTo(3);
        assertThat(chunkCouponId).isEqualTo(couponA);
        assertThat(adapter.releaseStock(chunkCouponId, 3, WROTE_AT)).isEqualTo(1);
        long afterA = chunkBoundary;

        // 둘째 청크 — 남은 회차 B.
        assertThat(expireChunk(afterA, LIMIT_ABOVE_FIXTURE)).isEqualTo(1);
        assertThat(chunkCouponId).isEqualTo(couponB);
        assertThat(adapter.releaseStock(chunkCouponId, 1, WROTE_AT)).isEqualTo(1);

        assertThat(activeCountOf(couponA)).as("5 에서 셋이 빠진다").isEqualTo(2);
        assertThat(activeCountOf(couponB)).as("4 에서 하나가 빠진다").isEqualTo(3);
    }

    /**
     * <b>재고가 이미 어긋난 채로 만료가 오면 빼는 쪽이 음수가 된다.</b> 그 회차를 갱신에서
     * 빼는 것은 {@code active_count >= :expired} 조건이고, 갱신 행 수 0 이 그 사실을 알린다.
     *
     * <p><b>재고 행이 <i>없는</i> 경우와 갈린다.</b> 그쪽은 {@code lockStock} 이 먼저 false 로
     * 잡는다. 두 원인이 다른 자리에서 나오므로 대조할 조회를 따로 둘 필요가 없어졌다.
     *
     * <p><b>{@code ck_stock_range} 에만 기대면 안 되는 이유가 여기 있다.</b> 그 CHECK 는 CLEAN
     * 스키마에만 걸린다 — 오염셋은 제약을 떼어 내고 만들기 때문에, 거기서는 DB 가 안 막아 준다.
     * 조건으로도 걸어 두면 <b>어느 스키마에서든 같은 자리에서 멈춘다.</b>
     */
    @Test
    @DisplayName("뺄 재고가 모자란 회차는 갱신되지 않는다 — CHECK 가 없는 스키마에서도 막힌다")
    void skipCouponWithInsufficientStock() {
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(1);

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE))
                .as("재고 행 자체는 있다 — lockStock 이 통과했다는 뜻이고, "
                        + "없는 경우와 원인이 갈린다")
                .isEqualTo(2);

        assertThat(adapter.releaseStock(chunkCouponId, 2, WROTE_AT))
                .as("둘을 빼면 -1 이 되므로 이 회차는 갱신되지 않는다")
                .isZero();
        assertThat(activeCount())
                .as("갱신이 안 됐으니 값도 그대로다")
                .isEqualTo(1);
    }

    private int activeCountOf(long couponId) {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", couponId)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>{@code APPEND_HISTORIES} 는 {@code from_status} 를 {@code 'ISSUED'} 로 못 박는다.</b>
     * 그 상수가 참인 근거는 SQL 안이 아니라 <b>전이표</b>에 있다 — {@code EXPIRE} 를 받을 수 있는
     * 상태가 {@code ISSUED} 하나뿐이라서다.
     *
     * <p><b>둘이 떨어져 있으면 조용히 갈라진다.</b> 전이표에 {@code (CANCELLED, EXPIRE)} 가
     * 추가되는 날, SQL 은 그것도 넘기면서 이력에는 여전히 {@code 'ISSUED'} 를 적는다.
     * 그러면 리플레이가 실제와 다른 상태를 재구성하고, 검증은 그 어긋남을 <b>데이터 문제로</b>
     * 보고한다 — 원인이 이 상수라는 것은 나오지 않는다. 그래서 여기서 둘을 붙여 둔다.
     *
     * <p>기존 {@code expireOnlyIssuedPastDue} 는 {@code USED} 만 본다.
     * 여기서는 <b>네 상태를 전수로</b> 확인한다 — 새 상태가 생기면 그것도 자동으로 걸린다.
     */
    @Test
    @DisplayName("from_status 상수의 근거는 전이표다 — EXPIRE 를 받는 상태가 ISSUED 뿐이다")
    void fromStatusConstantFollowsStateMachine() {
        assertThat(Arrays.stream(IssuanceStatus.values())
                .filter(status -> CouponStateMachine.next(status, IssuanceEventType.EXPIRE)
                        .isPresent())
                .toList())
                .as("여기가 늘어나면 APPEND_HISTORIES 의 'ISSUED' 상수가 거짓말이 된다")
                .containsExactly(IssuanceStatus.ISSUED);

        Map<IssuanceStatus, Long> planted = new EnumMap<>(IssuanceStatus.class);
        for (IssuanceStatus status : IssuanceStatus.values()) {
            planted.put(status, issuance(status, EXPIRED_AT));
        }
        // 미리 심은 EXPIRED 건이 이번 청크의 표식(updated_at = WROTE_AT)과 겹치지 않게 민다.
        // 겹치면 남의 행이 우리 집합에 섞여, 무엇을 재는지 알 수 없게 된다.
        jdbcClient.sql("UPDATE issuances SET updated_at = :at")
                .param("at", EXPIRED_AT)
                .update();

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE))
                .as("넘어가는 것은 ISSUED 하나뿐이다")
                .isEqualTo(1);

        planted.forEach((status, id) -> assertThat(statusOf(id))
                .as("%s 는 기한이 지나도 넘어가지 않는다", status)
                .isEqualTo(status == IssuanceStatus.ISSUED
                        ? IssuanceStatus.EXPIRED.name() : status.name()));

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, chunkBoundary, chunkCouponId)).isEqualTo(1);
        assertThat(fromStatusOf(planted.get(IssuanceStatus.ISSUED)))
                .as("전이표가 허용한 그 상태가 이력에 적혀야 한다")
                .isEqualTo(IssuanceStatus.ISSUED.name());
    }

    private String fromStatusOf(long issuanceId) {
        return jdbcClient.sql("SELECT from_status FROM issuance_histories "
                        + "WHERE issuance_id = :id AND event_type = 'EXPIRE'")
                .param("id", issuanceId)
                .query(String.class)
                .single();
    }

    /**
     * <b>이 순서 변경이 실제로 푼 정체다.</b>
     *
     * <p>예전 종료 신호는 만료 {@code UPDATE} 가 0 을 돌려주는 것이었다. 그러면 <b>후보가
     * 전부 사용된 청크</b>에서 잡이 <i>"남은 대상이 없다"</i> 로 읽고 끝난다 — 그 뒤 id 에
     * 진짜 만료 대상이 남아 있어도 그날은 못 넘긴다. 다음 주기도 같은 자리에서 같은 판단을
     * 하므로 <b>영영 안 넘어간다.</b>
     *
     * <p>이제 후보를 먼저 읽으므로 넘어간 것이 0 이어도 진도는 그 구간만큼 나가고, 다음 청크가
     * 그 뒤를 집는다.
     */
    @Test
    @DisplayName("청크가 통째로 사용돼 하나도 못 넘겨도 진도는 나간다")
    void progressAdvancesEvenWhenNothingExpires() {
        long used = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        long target = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(2);

        // 후보를 읽은 뒤 UPDATE 전에 사용된 상황. 후보 질의가 락을 안 잡으므로 실제로 생긴다.
        ExpireChunk first = ExpireChunk.from(adapter.nextCandidates(AS_OF, 0L, 1, List.of()));
        assertThat(first.lastId()).isEqualTo(used);
        jdbcClient.sql("UPDATE issuances SET status = 'USED' WHERE id = :id")
                .param("id", used)
                .update();

        assertThat(adapter.lockStock(first.couponId())).isTrue();
        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, first.lastId(), first.couponId()))
                .as("그 사이 USED 가 됐으니 조건부 UPDATE 가 안 잡는다")
                .isZero();

        // 잡은 여기서 끝내지 않고 afterId 를 first.lastId() 로 밀고 이어 간다.
        assertThat(expireChunk(first.lastId(), LIMIT_ABOVE_FIXTURE))
                .as("진도를 안 밀면 다음 청크가 같은 자리를 다시 집어 영원히 못 넘어간다")
                .isEqualTo(1);
        assertThat(statusOf(target)).isEqualTo("EXPIRED");
    }

    /**
     * <b>상한({@code id <= lastId})을 지키는 유일한 축이다.</b>
     *
     * <p>예전에는 {@code ExpirationLockScopeTest} 가 락 수로 이것을 지켰다. READ COMMITTED 로
     * 내린 뒤로는 <b>뒤 문장들이 락을 아예 안 잡아서</b> 상한을 통째로 지워도 락 수가 그대로다
     * — 실측으로 확인했다(RC: 상한 있음 40 · 없음 40, RR: 62 · 546). 그 축이 죽었으므로
     * 상한이 지금 지키는 것을 직접 잰다: <b>남의 행이 우리 집합에 안 섞인다.</b>
     *
     * <p>구간 <b>위</b>에 같은 표식을 가진 행을 심는다. 상한이 빠지면 뒤 문장들이 그것까지
     * 세어 이력 수가 만료 건수와 어긋나고, 잡은 {@code EXPIRE_HISTORY_COUNT_MISMATCH} 로 멈춘다.
     */
    @Test
    @DisplayName("구간 위에 같은 표식이 있어도 뒤 문장들이 세지 않는다")
    void boundaryExcludesRowsAboveLastId() {
        long ours = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        long above = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);

        assertThat(expireChunk(0L, 1)).isEqualTo(1);
        long boundary = chunkBoundary;
        assertThat(boundary).as("첫 건만 넘어갔어야 구간 위가 생긴다").isEqualTo(ours);

        // 남이 같은 표식으로 구간 위의 행을 넘긴 것을 흉내낸다.
        jdbcClient.sql("UPDATE issuances SET status = 'EXPIRED', updated_at = :at WHERE id = :id")
                .param("at", WROTE_AT)
                .param("id", above)
                .update();

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundary, chunkCouponId))
                .as("상한이 빠지면 2 가 되고, 잡이 이력 짝 검사에서 멈춘다")
                .isEqualTo(1);
        assertThat(historyCount(above))
                .as("우리가 넘기지 않은 건에는 이력이 안 붙는다")
                .isZero();
    }

    /**
     * <b>SQL 이 전이표를 두 번째로 인코딩한다.</b> {@code EXPIRE_BATCH} 의
     * {@code WHERE status='ISSUED' … SET status='EXPIRED'} 와 {@code APPEND_HISTORIES} 의
     * {@code 'ISSUED','EXPIRED'} 상수가 그것이다. {@code CouponStateMachine} 을 참조하지 않는다.
     *
     * <p>지금은 두 벌이 일치하지만 그 일치를 확인하는 것이 없었다. 전이표가 바뀌는 날
     * 리플레이와 검증은 새 규칙을 따르고 만료 SQL 만 옛 규칙으로 남는다 —
     * 그러면 V4(불법 전이)가 만료 배치가 쓴 이력을 검출로 잡거나, 잡아야 할 것을 놓친다.
     */
    @Test
    @DisplayName("만료 SQL 이 인코딩한 전이가 전이표와 같다")
    void sqlMatchesStateMachine() {
        assertThat(CouponStateMachine.next(IssuanceStatus.ISSUED, IssuanceEventType.EXPIRE))
                .as("EXPIRE_BATCH 의 SET status='EXPIRED' 와 APPEND_HISTORIES 의 to_status 근거")
                .contains(IssuanceStatus.EXPIRED);
        assertThat(CouponStateMachine.next(IssuanceStatus.USED, IssuanceEventType.EXPIRE))
                .as("USED 에도 EXPIRE 가 열리면 EXPIRE_BATCH 의 status='ISSUED' 필터를 함께 고쳐야 한다")
                .isEmpty();
        assertThat(IssuanceEventType.EXPIRE.name())
                .as("**APPEND_HISTORIES 가 박는 event_type 리터럴이다.** 컬럼이 자유 varchar 라 "
                        + "이름을 바꿔도 INSERT 는 성공한다 — 그러면 리플레이가 만료 이력 전체를 "
                        + "모르는 사건으로 읽고 V3·V4 가 한꺼번에 운다")
                .isEqualTo("EXPIRE");
    }

    /**
     * <b>{@code asOf} 백데이트 금지의 쌍둥이다.</b> 그쪽은 컷을, 이쪽은 <b>시각을 잡은 뒤의
     * 창</b>을 막는다.
     *
     * <p>잡은 {@code committedAt} 을 먼저 잡고 그 뒤에 스캔을 돈다. 그 사이 런타임이 상태를
     * 바꾼 행까지 매치하면 그 행에 <b>과거 시각이 찍히고</b>, 리플레이 정렬
     * {@code (issuance_id, created_at, id)} 에서 우리 이력이 나중 사건보다 앞서게 된다.
     * 더미데이터에 <i>"USED 중 20% 복원"</i> 이 있어 그 조합은 반드시 나온다.
     */
    @Test
    @DisplayName("시각을 잡은 뒤에 바뀐 행은 이번 청크에서 빠진다")
    void skipRowsChangedAfterCommittedAt() {
        long target = issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        // 우리가 committedAt 을 잡은 뒤 런타임이 이 행을 건드린 것을 흉내낸다.
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", WROTE_AT.plusSeconds(1))
                .param("id", target)
                .update();

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE))
                .as("이번 청크는 건너뛴다. 다음 주기가 새 committedAt 으로 집는다")
                .isZero();
        assertThat(statusOf(target)).isEqualTo(IssuanceStatus.ISSUED.name());
    }

    /**
     * <b>재고 시각은 뒤로 물러나면 안 된다.</b> 검증의 {@code hasStocksUpdatedAfter} 가
     * 그 단조성 위에 서 있고, 이력 축과 달리 재고 축에는 백데이트 전용 가드가 없다.
     *
     * <p>{@code RELEASE_STOCK} 은 청크의 마지막 문장이라, 앞 다섯이 도는 동안 다른 트랜잭션이
     * 더 늦은 시각을 써 둘 수 있다. {@code VerificationSeed.syncStock} 이 같은 이유로 이미
     * {@code GREATEST} 를 쓴다 — 시드가 지키는 규약을 운영 SQL 이 안 지키고 있었다.
     */
    @Test
    @DisplayName("재고 시각이 뒤로 물러나지 않는다")
    void stockUpdatedAtNeverMovesBackward() {
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(3, WROTE_AT.plusSeconds(5));

        assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE)).isEqualTo(1);
        assertThat(adapter.releaseStock(chunkCouponId, 1, WROTE_AT)).isEqualTo(1);

        assertThat(stockUpdatedAt())
                .as("committedAt 으로 덮어쓰면 검증이 그 사이의 변경을 못 본다")
                .isEqualTo(WROTE_AT.plusSeconds(5));
        assertThat(activeCount()).as("값은 정상으로 빠진다").isEqualTo(2);
    }

    private LocalDateTime stockUpdatedAt() {
        return jdbcClient.sql("SELECT updated_at FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", seed.currentCouponId())
                .query(LocalDateTime.class)
                .single();
    }
}
