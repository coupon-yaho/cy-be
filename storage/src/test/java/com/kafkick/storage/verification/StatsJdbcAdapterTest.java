// 통계 스냅샷이 시드 구현과 같은 값을 내는지 확인합니다.
package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.HourlyIssued;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>{@code contract.json} 에 통계 조항이 없다.</b> checksum·지문과 달리 값이 계약으로 고정돼
 * 있지 않아, 배치와 시드가 갈려도 자동으로 잡히지 않는다. 그래서 이 클래스가 <b>시드 구현의 규칙을
 * 독립적으로 재구현해</b> 대조한다 — {@code DatasetFingerprintTest} 가 계약 공식을 손으로 만들어
 * 대조하는 것과 같은 방식이다.
 *
 * <p>지키는 규칙 넷:
 * <pre>
 * 회차 전체     발급 0건인 회차도 행을 쓴다        (시드는 카탈로그 전체를 돈다)
 * 완판 판정     issued_total >= total_quantity   (active_count 가 아니다)
 * 등급 쌍       존재하는 것만                     (없는 조합에 0 행을 만들지 않는다)
 * 요일·시각     168행 전부 · 리플레이와 같은 창
 * </pre>
 */
@RepositoryTest
@Import({StatsJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class StatsJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 회차의 {@code open_at} 이자 픽스처의 기본 발급 시각. 2025-01-01 은 <b>수요일</b>이다. */
    private static final LocalDateTime OPEN_AT = LocalDateTime.of(2025, 1, 1, 0, 0);

    /** 얼린 이력 상한을 넘는 값. 창을 안 좁히는 테스트에서 쓴다. */
    private static final long NO_LIMIT = Long.MAX_VALUE;

    @Autowired
    private StatsJdbcAdapter adapter;

    @Autowired
    private VerificationRunJdbcAdapter runs;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;
    private long runId;

    @BeforeEach
    void setUp() {
        seed = new VerificationSeed(jdbcClient);
        runId = runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 1, AS_OF)).id();
    }

    /**
     * 재고 상한을 낮춘다. 픽스처가 100 으로 고정해 두어 완판을 100건 없이 만들 수 없다.
     *
     * <p><b>발급 뒤에 불러야 한다.</b> 재고 행은 첫 발급이 만들고, 그 전에는 갱신할 행이 없다.
     */
    private void capStockAt(int totalQuantity) {
        jdbcClient.sql("UPDATE coupon_stocks SET total_quantity = :total")
                .param("total", totalQuantity)
                .update();
    }

    private Map<String, Object> couponStatRow(long couponId) {
        return jdbcClient.sql("""
                        SELECT issued_total, issued, used, cancelled, expired, sold_out_seconds
                          FROM coupon_stats WHERE run_id = :runId AND coupon_id = :couponId
                        """)
                .param("runId", runId)
                .param("couponId", couponId)
                .query()
                .singleRow();
    }

    /**
     * <b>발급이 0건인 회차도 행을 받는다.</b> 없는 행과 0인 행은 다르다 — 빼면 대시보드가
     * "데이터 없음" 과 "0건" 을 구분할 수 없다. 시드도 카탈로그 전체를 돈다.
     */
    @Test
    @DisplayName("발급이 없는 회차도 0으로 채운 행을 받는다")
    void writeRowForCouponWithoutIssuance() {
        long withIssuance = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED);
        long empty = seed.newCoupon();

        assertThat(adapter.aggregateCouponStats(runId))
                .as("회차 수와 같아야 한다. issuances 를 드라이빙으로 잡으면 빈 회차가 빠진다")
                .isEqualTo(2);
        assertThat(couponStatRow(empty))
                .containsEntry("issued_total", 0)
                .containsEntry("issued", 0)
                .containsEntry("sold_out_seconds", null);
        assertThat(couponStatRow(withIssuance)).containsEntry("issued_total", 1);
    }

    @Test
    @DisplayName("상태별로 나눠 센다 — 리플레이 상태가 아니라 issuances.status 다")
    void countByStoredStatus() {
        long couponId = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED);
        seed.issuance(IssuanceStatus.ISSUED);
        seed.issuance(IssuanceStatus.USED);
        seed.issuance(IssuanceStatus.CANCELLED);
        seed.issuance(IssuanceStatus.EXPIRED);

        adapter.aggregateCouponStats(runId);

        assertThat(couponStatRow(couponId))
                .containsEntry("issued_total", 5)
                .containsEntry("issued", 2)
                .containsEntry("used", 1)
                .containsEntry("cancelled", 1)
                .containsEntry("expired", 1);
    }

    /**
     * <b>완판은 {@code total_quantity} 로 판정한다.</b> 시드가 완판을 정하는 식이
     * {@code issue_count >= total_quantity} 다.
     */
    @Test
    @DisplayName("완판이면 첫 발급까지 걸린 초를 적는다")
    void recordSoldOutSeconds() {
        long couponId = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED, OPEN_AT.plusSeconds(90));
        // 발급 뒤에 낮춘다. 재고 행은 첫 발급이 만들므로 그 전에 UPDATE 하면 0행이다.
        capStockAt(1);

        adapter.aggregateCouponStats(runId);

        assertThat(couponStatRow(couponId))
                .as("마지막 발급 시각 − open_at 의 초")
                .containsEntry("sold_out_seconds", 90);
    }

    /**
     * <b>이 테스트가 이 파일의 핵심이다.</b> {@code active_count} 로 완판을 판정하면 여기서 뒤집힌다.
     *
     * <p>취소된 발급 하나뿐인 회차는 {@code active_count = 0} 이지만 <b>미달</b>이다
     * ({@code issued_total 1 < total_quantity 100}). {@code active_count == 0} 을 완판으로 읽으면
     * 이 회차에 소요 시간이 찍히고, 대시보드의 "완판까지 걸린 시간" 이 취소가 많은 회차로 오염된다.
     *
     * <p>{@code active_count} 는 <i>현재 보유량</i>(ISSUED + USED)이라 취소·만료로 줄어든다 —
     * 이 저장소가 반복해 경계하는 "누적 발급 수로 착각" 의 변종이다.
     */
    @Test
    @DisplayName("취소로 재고가 0이 된 미달 회차는 완판이 아니다")
    void distinguishSoldOutFromEmptiedStock() {
        long couponId = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.CANCELLED);

        adapter.aggregateCouponStats(runId);

        assertThat(jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", couponId)
                .query(Integer.class)
                .single())
                .as("취소는 보유량에 안 들어간다 — 이 값이 0 이어야 이 테스트가 뜻을 갖는다")
                .isZero();
        assertThat(couponStatRow(couponId))
                .as("발급 1건 / 재고 100 이라 미달이다")
                .containsEntry("sold_out_seconds", null);
    }

    /**
     * <b>존재하는 쌍만 쓴다.</b> 시드는 누적된 키만 쓰므로, 없는 {@code (회차, 등급)} 조합에
     * 0 행을 만들면 행 수가 어긋난다. 등급은 {@code issued_grade} 스냅샷이다.
     */
    @Test
    @DisplayName("등급 집계는 존재하는 쌍만 쓴다")
    void aggregateOnlyExistingGradePairs() {
        long couponId = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.USED, "VIP");
        seed.issuance(IssuanceStatus.ISSUED, "VIP");
        seed.issuance(IssuanceStatus.ISSUED, "GOLD");

        assertThat(adapter.aggregateGradeStats(runId))
                .as("등급 네 종이 있어도 쓰인 두 종만 행이 된다")
                .isEqualTo(2);
        assertThat(jdbcClient.sql("""
                        SELECT grade, issued_total, used_total FROM grade_stats
                         WHERE run_id = :runId AND coupon_id = :couponId ORDER BY grade
                        """)
                .param("runId", runId)
                .param("couponId", couponId)
                .query((rs, n) -> rs.getString("grade") + ":" + rs.getInt("issued_total")
                        + "/" + rs.getInt("used_total"))
                .list())
                .containsExactly("GOLD:1/0", "VIP:2/1");
    }

    /**
     * <b>168행을 전부 쓴다.</b> 빈 칸을 빼면 대시보드가 "그 시각에 데이터가 없다" 와
     * "0건이다" 를 구분할 수 없다.
     *
     * <p>요일 표기는 시드와 같은 세 글자다. 2025-01-01 은 수요일이라 {@code WED} 여야 한다 —
     * {@code DAYNAME()} 을 쓰면 {@code Wednesday} 가 되고 {@code varchar(3)} 에서 잘린다.
     */
    @Test
    @DisplayName("요일·시각은 168행을 채우고 없는 칸은 0이다")
    void fillEveryHourSlot() {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(13));

        List<HourlyIssued> measured = adapter.issuedByHour(NO_LIMIT, AS_OF);
        adapter.appendHourlyStats(runId, HourlyIssued.fillAll(measured));

        assertThat(measured)
                .as("2025-01-01 은 수요일이다")
                .containsExactly(new HourlyIssued("WED", 13, 1));
        assertThat(jdbcClient.sql("SELECT COUNT(*) FROM hourly_stats WHERE run_id = :runId")
                .param("runId", runId)
                .query(Integer.class)
                .single())
                .isEqualTo(7 * 24);
        assertThat(jdbcClient.sql("""
                        SELECT issued_total FROM hourly_stats
                         WHERE run_id = :runId AND day_of_week = 'WED' AND hour = 13
                        """)
                .param("runId", runId)
                .query(Integer.class)
                .single())
                .isEqualTo(1);
    }

    /** {@code ISSUE} 가 아닌 이력은 세지 않는다 — 그러면 사용·취소가 발급으로 집계된다. */
    @Test
    @DisplayName("ISSUE 가 아닌 이력은 요일 집계에 안 들어간다")
    void countOnlyIssueEvents() {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(1));
        seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, OPEN_AT.plusHours(2));

        assertThat(adapter.issuedByHour(NO_LIMIT, AS_OF))
                .containsExactly(new HourlyIssued("WED", 1, 1));
    }

    /**
     * <b>창이 리플레이와 같아야 한다.</b> {@code created_at <= asOf} 만 걸면 다시 재는 것이라,
     * 얼린 상한보다 큰 id 인데 시각이 과거인 <b>백데이트 이력</b>을 리플레이는 못 읽고 통계는 읽는다.
     * CY-193 에서 {@code dataset_fingerprint} 가 앓았던 병이 그것이고
     * {@code hasHistoriesAddedAbove} 가드가 그래서 생겼다.
     */
    @Test
    @DisplayName("얼린 상한보다 뒤에 들어온 이력은 세지 않는다 — 시각이 과거여도")
    void respectFrozenHistoryBoundary() {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        long frozen = seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(3));
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(4));

        assertThat(adapter.issuedByHour(frozen, AS_OF))
                .as("두 번째 이력은 asOf 이하지만 얼린 상한 밖이다 — 리플레이도 안 읽었다")
                .containsExactly(new HourlyIssued("WED", 3, 1));
    }

    /** 재시작하면 앞 실행의 부분 결과가 남는다. 그 위에 다시 쓰면 중복키로 죽는다. */
    @Test
    @DisplayName("같은 실행의 앞 스냅샷을 지우고 다시 쓴다")
    void clearBeforeRewrite() {
        seed.issuance(IssuanceStatus.ISSUED);
        adapter.aggregateCouponStats(runId);

        adapter.clear(runId);
        adapter.aggregateCouponStats(runId);

        assertThat(jdbcClient.sql("SELECT COUNT(*) FROM coupon_stats WHERE run_id = :runId")
                .param("runId", runId)
                .query(Integer.class)
                .single())
                .isEqualTo(1);
    }
}
