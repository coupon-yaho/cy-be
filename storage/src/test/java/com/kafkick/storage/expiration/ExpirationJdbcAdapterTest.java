// 만료 SQL 여섯이 무엇을 건드리고 무엇을 안 건드리는지 확인합니다.
package com.kafkick.storage.expiration;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.EnumMap;
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
    private static final int NO_LIMIT = 1000;

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
     * 청크 하나를 끝까지 돌린다 — 넘기고, 경계를 찾고, 뒤 문장들을 그 경계로 부른다.
     *
     * <p>뒤 문장이 <b>전부 같은 {@code (afterId, lastId]} 를 봐야</b> 하는데, 테스트마다 손으로
     * 넘기면 한 곳만 빠뜨려도 그 문장이 테이블 끝까지 훑는다 — 그때 깨지는 것은 이 테스트가
     * 아니라 운영의 발급 경로다. 경계를 여기서 한 번만 구해 넘긴다.
     */
    private long boundaryAfter(long afterId) {
        return adapter.lastExpiredId(AS_OF, WROTE_AT, afterId);
    }

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

        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT)).isEqualTo(1);

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

        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT)).isZero();
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
        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, 1)).isEqualTo(1);
        long boundary = adapter.lastExpiredId(AS_OF, WROTE_AT, 0L);
        assertThat(boundary).isEqualTo(first);
        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundaryAfter(0L))).isEqualTo(1);
        adapter.releaseStock(AS_OF, WROTE_AT, 0L, boundaryAfter(0L));

        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, boundary, 1)).isEqualTo(1);
        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, boundary, boundaryAfter(boundary)))
                .as("앞 청크 것을 다시 세면 2 가 되고 이력이 중복된다")
                .isEqualTo(1);
        adapter.releaseStock(AS_OF, WROTE_AT, boundary, boundaryAfter(boundary));

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
        adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT);

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundaryAfter(0L))).isEqualTo(1);
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

        adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT);
        assertThat(adapter.releaseStock(AS_OF, WROTE_AT, 0L, boundaryAfter(0L))).as("회차 하나를 갱신한다").isEqualTo(1);

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

        adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT);
        adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundaryAfter(0L));

        LocalDateTime expireAt = jdbcClient.sql("SELECT created_at FROM issuance_histories "
                        + "WHERE issuance_id = :id AND event_type = 'EXPIRE'")
                .param("id", target)
                .query(LocalDateTime.class)
                .single();

        assertThat(expireAt)
                .as("asOf 로 찍으면 취소(asOf+1분)보다 앞서 리플레이가 EXPIRE 를 먼저 접는다")
                .isAfter(AS_OF.plusMinutes(1));
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

        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT)).isEqualTo(1);
        long boundary = boundaryAfter(0L);

        // 런타임이 상태를 직접 넘긴 것을 흉내낸다. 우리 청크가 끝난 뒤 생긴 행이다.
        jdbcClient.sql("UPDATE issuances SET status = 'EXPIRED', updated_at = :at WHERE id = :id")
                .param("at", WROTE_AT)
                .param("id", theirs)
                .update();

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundary))
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

        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT)).isEqualTo(1);
        long boundary = boundaryAfter(0L);
        assertThat(theirs).as("남의 행이 우리 구간 안에 있어야 이 겹을 시험한다").isLessThan(boundary);

        jdbcClient.sql("UPDATE issuances SET status = 'EXPIRED', updated_at = :at WHERE id = :id")
                .param("at", WROTE_AT)
                .param("id", theirs)
                .update();

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundary))
                .as("기한이 남은 건은 expires_at 조건이 잘라 낸다")
                .isEqualTo(1);
        assertThat(historyCount(ours)).isEqualTo(1);
        assertThat(historyCount(theirs)).isZero();
    }

    /**
     * <b>재고 행이 없으면 JOIN 이 조용히 건너뛴다.</b> 발급건은 만료로 넘어갔는데 되돌릴 재고가
     * 없는 상태다. 세지 않으면 아무도 모르므로 호출자가 두 값을 대조할 수 있어야 한다.
     */
    @Test
    @DisplayName("재고 행이 없는 회차는 갱신되지 않는다 — 회차 수와 갱신 수가 갈린다")
    void countAndUpdateDivergeWithoutStockRow() {
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.removeStock();

        adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT);

        assertThat(adapter.expiredCouponCount(AS_OF, WROTE_AT, 0L, boundaryAfter(0L))).isEqualTo(1);
        assertThat(adapter.releaseStock(AS_OF, WROTE_AT, 0L, boundaryAfter(0L)))
                .as("갱신할 재고 행이 없다. 두 값이 갈리는 것이 유일한 신호다")
                .isZero();
    }

    /**
     * <b>짝 검사가 못 보는 자리다.</b> 잡은 {@code releaseStock} 이 갱신한 <i>회차 수</i>와
     * {@code expiredCouponCount} 를 맞춰 보는데, 그 둘이 같아도 <b>회차마다 얼마씩 뺐는지</b>는
     * 아무도 안 본다. 파생테이블에서 {@code GROUP BY coupon_id} 로 접는 것이 그 몫을 가르는
     * 유일한 장치이고, 그것이 무너지면 어느 회차는 남의 몫까지 빼고 어느 회차는 덜 뺀다.
     *
     * <p>회차 수는 그대로라 잡은 초록으로 끝난다. 검증이 재고 불일치로 잡을 때까지 안 보인다.
     */
    @Test
    @DisplayName("회차가 섞이면 회차마다 자기 몫만 빠진다")
    void releaseStockPerCoupon() {
        long couponA = seed.newCoupon();
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(5);

        long couponB = seed.newCoupon();
        issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        seed.overwriteStock(4);

        adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT);
        assertThat(adapter.releaseStock(AS_OF, WROTE_AT, 0L, boundaryAfter(0L))).as("회차 둘을 갱신한다").isEqualTo(2);

        assertThat(activeCountOf(couponA)).as("5 에서 셋이 빠진다").isEqualTo(2);
        assertThat(activeCountOf(couponB)).as("4 에서 하나가 빠진다").isEqualTo(3);
    }

    /**
     * <b>재고가 이미 어긋난 채로 만료가 오면 빼는 쪽이 음수가 된다.</b> 그 회차를 갱신에서
     * 빼는 것은 {@code active_count >= 차감량} 조건이고, {@code stockRowCount} 와의 차이가
     * 그 사실을 호출자에게 알린다.
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

        adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT);
        long boundary = boundaryAfter(0L);

        assertThat(adapter.stockRowCount(AS_OF, WROTE_AT, 0L, boundary))
                .as("재고 행 자체는 있다 — 없는 것과 구분돼야 원인이 갈린다")
                .isEqualTo(1);
        assertThat(adapter.releaseStock(AS_OF, WROTE_AT, 0L, boundary))
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

        assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT))
                .as("넘어가는 것은 ISSUED 하나뿐이다")
                .isEqualTo(1);

        planted.forEach((status, id) -> assertThat(statusOf(id))
                .as("%s 는 기한이 지나도 넘어가지 않는다", status)
                .isEqualTo(status == IssuanceStatus.ISSUED
                        ? IssuanceStatus.EXPIRED.name() : status.name()));

        assertThat(adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, boundaryAfter(0L))).isEqualTo(1);
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
     * <b>계약만 있고 그것을 지키는 것이 없던 자리다.</b> 포트는 "넘어간 것이 없으면
     * {@code afterId} 를 그대로 돌려준다" 고 적었고 어댑터는 {@code COALESCE} 로 구현했는데,
     * 유일한 호출자인 잡은 {@code expired == 0} 이면 먼저 빠져나가므로 <b>빈 집합으로 부를
     * 경로가 없다.</b> 도달할 수 없는 가드는 없는 가드와 같다는 것이 이 저장소의 기준이라,
     * 계약을 지우는 대신 여기서 도달시킨다.
     *
     * <p>0 을 돌려주면 다음 청크가 {@code id > 0} 부터 다시 훑는다 — 진도가 뒤로 가는 셈이다.
     */
    @Test
    @DisplayName("넘어간 것이 없으면 진도를 그대로 돌려준다 — 0 으로 되돌리지 않는다")
    void keepProgressWhenNothingExpired() {
        assertThat(adapter.lastExpiredId(AS_OF, WROTE_AT, 42L))
                .as("빈 집합에서 0 을 주면 다음 청크가 앞 구간을 다시 훑는다")
                .isEqualTo(42L);
    }
}
