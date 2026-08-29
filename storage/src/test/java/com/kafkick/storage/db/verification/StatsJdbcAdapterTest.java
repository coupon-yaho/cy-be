// 통계 스냅샷이 시드 구현과 같은 값을 내는지 확인합니다.
package com.kafkick.storage.db.verification;

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

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
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
    private void capStockAt(long couponId, int totalQuantity) {
        // 회차를 지목한다. WHERE 없이 두면 이름은 "이 회차의 상한" 인데 테이블 전체를 갱신해,
        // 회차 둘을 쓰는 테스트가 늘어나는 날 다른 회차의 완판 판정이 조용히 따라 바뀐다.
        jdbcClient.sql("UPDATE coupon_stocks SET total_quantity = :total "
                        + "WHERE coupon_id = :couponId")
                .param("total", totalQuantity)
                .param("couponId", couponId)
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

        assertThat(adapter.aggregateCouponStats(runId, AS_OF))
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

        adapter.aggregateCouponStats(runId, AS_OF);

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
    @DisplayName("완판이면 마지막 발급까지 걸린 초를 적는다 — MAX(issued_at) − open_at")
    void recordSoldOutSeconds() {
        long couponId = seed.currentCouponIdOrCreate();
        // 두 건을 넣어야 MIN 과 MAX 가 갈린다. 한 건이면 SQL 을 MIN(issued_at) 으로
        // 바꿔도 테스트가 통과해, 이름만 있고 지키는 것이 없다.
        seed.issuance(IssuanceStatus.ISSUED, OPEN_AT.plusSeconds(30));
        seed.issuance(IssuanceStatus.ISSUED, OPEN_AT.plusSeconds(90));
        // 발급 뒤에 낮춘다. 재고 행은 첫 발급이 만들므로 그 전에 UPDATE 하면 0행이다.
        capStockAt(couponId, 2);

        adapter.aggregateCouponStats(runId, AS_OF);

        assertThat(couponStatRow(couponId))
                .as("30 이 나오면 MIN 을 쓰고 있다")
                .containsEntry("sold_out_seconds", 90);
    }

    /**
     * <b>절삭 방향이 시드와 같아야 한다.</b> 시드는
     * {@code int((last_issue_at - open_at).total_seconds())} 로 <b>버리고</b>, 배치는
     * {@code TIMESTAMPDIFF(SECOND, …)} 를 쓴다. 반올림이면 완판 회차의 값이 1초씩 갈린다.
     *
     * <p>{@code issued_at} 은 {@code datetime(6)} 이고 {@code open_at} 은 초 단위라
     * 소수부는 발급 시각에서만 온다.
     */
    @Test
    @DisplayName("소수 초는 버린다 — 반올림이면 시드와 1초 갈린다")
    void truncateSubSecondLikeSeed() {
        long couponId = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED, OPEN_AT.plusSeconds(90).withNano(900_000_000));
        capStockAt(couponId, 1);

        adapter.aggregateCouponStats(runId, AS_OF);

        assertThat(couponStatRow(couponId))
                .as("90.9초다. 반올림이면 91 이 된다")
                .containsEntry("sold_out_seconds", 90);
    }

    /**
     * <b>통계 Step 은 얼림 확인보다 뒤에 있다.</b> 그래서 컷이 없으면 집계 도중에 들어온 발급이
     * 섞이는데, {@code rejectRunningExpire} 는 배치 메타의 만료 실행만 보므로 api 프로세스가
     * 살아 있으면 실제로 벌어진다.
     */
    @Test
    @DisplayName("asOf 이후에 갱신된 발급건은 집계에서 빠진다")
    void cutIssuancesAtAsOf() {
        long couponId = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED);
        long late = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF.plusSeconds(1))
                .param("id", late)
                .update();

        adapter.aggregateCouponStats(runId, AS_OF);

        assertThat(couponStatRow(couponId))
                .as("컷이 없으면 2 가 된다")
                .containsEntry("issued_total", 1);
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

        adapter.aggregateCouponStats(runId, AS_OF);

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

        assertThat(adapter.aggregateGradeStats(runId, AS_OF))
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
     * <b>회차가 하나뿐이면 루프를 못 잰다.</b> 등급 집계는 CY-470 에서 <b>회차 단위 147회</b>로
     * 쪼개졌다(근거는 {@code AGGREGATE_GRADE_STATS_FOR_COUPON} javadoc — 한 문장으로 묶으면
     * 검증용 DB 서버가 강제 종료됐다). 회차가 하나면 루프가 한 번만
     * 돌아, <b>첫 회차만 돌고 나머지를 빠뜨리는 구현</b>도 그대로 통과한다.
     *
     * <p>돌려주는 값이 <b>회차별 합</b>이라는 것도 여기서 잰다 — 한 문장이던 시절과 같은
     * 값이어야 호출부의 {@code contribution.incrementWriteCount} 와 종료 설명이 안 어긋난다.
     */
    @Test
    @DisplayName("등급 집계는 회차마다 따로 귀속된다 — 회차 단위로 쪼개도 합이 같다")
    void aggregateGradePairsPerCoupon() {
        long firstCoupon = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED, "VIP");
        seed.issuance(IssuanceStatus.USED, "VIP");

        long secondCoupon = seed.newCoupon();
        seed.issuance(IssuanceStatus.ISSUED, "GOLD");

        assertThat(adapter.aggregateGradeStats(runId, AS_OF))
                .as("회차 둘이 각각 한 쌍씩 — 합이 2다")
                .isEqualTo(2);
        assertThat(gradeRowsOf(firstCoupon))
                .as("첫 회차만 돌고 멈추면 둘째가 비고, 반대면 첫째가 빈다")
                .containsExactly("VIP:2/1");
        assertThat(gradeRowsOf(secondCoupon)).containsExactly("GOLD:1/0");
    }

    /**
     * <b>회차 컷이 살아 있어야 한다.</b> 쪼개기 전에는 {@code JOIN coupons … WHERE
     * c.created_at <= :asOf} 가 이 컷을 졌는데, 회차 단위로 나누면서 그 조인이 사라지고
     * <b>회차 목록을 뜨는 질의</b>가 대신 진다. 목록 질의에서 컷이 빠지면
     * {@code asOf} 시점에 없던 회차가 스냅샷에 들어와 <b>같은 {@code asOf} 재실행이 다른
     * 행 수</b>를 낸다 — 결정론이 깨지는 자리다.
     */
    @Test
    @DisplayName("asOf 뒤에 만들어진 회차는 등급 집계에서 빠진다")
    void excludeCouponCreatedAfterAsOf() {
        long visible = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED, "VIP");

        long future = seed.newCoupon();
        seed.issuance(IssuanceStatus.ISSUED, "GOLD");
        jdbcClient.sql("UPDATE coupons SET created_at = :createdAt WHERE id = :couponId")
                .param("createdAt", AS_OF.plusDays(1))
                .param("couponId", future)
                .update();

        assertThat(adapter.aggregateGradeStats(runId, AS_OF)).isEqualTo(1);
        assertThat(gradeRowsOf(visible)).containsExactly("VIP:1/0");
        assertThat(gradeRowsOf(future))
                .as("회차가 asOf 시점에 없었으므로 그 발급도 스냅샷에 없다")
                .isEmpty();
    }

    private List<String> gradeRowsOf(long couponId) {
        return jdbcClient.sql("""
                        SELECT grade, issued_total, used_total FROM grade_stats
                         WHERE run_id = :runId AND coupon_id = :couponId ORDER BY grade
                        """)
                .param("runId", runId)
                .param("couponId", couponId)
                .query((rs, n) -> rs.getString("grade") + ":" + rs.getInt("issued_total")
                        + "/" + rs.getInt("used_total"))
                .list();
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
     * <b>리플레이 정렬의 전제를 잰다.</b> {@code (created_at, id)} 로 접는데 그 시각이
     * 커밋 시각이 아니라 <b>멱등 선점 시각</b>이라({@code REQUIRES_NEW}) 역전이 가능하다.
     * 역전이 있으면 리플레이가 인과와 다른 순서로 접어 <b>정상 데이터에 오탐</b>을 낸다.
     */
    @Test
    @DisplayName("id 는 뒤인데 created_at 이 앞서면 역전으로 센다")
    void countsOutOfOrderHistoryPairs() {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(1));
        // 뒤에 들어왔는데(=id 가 크다) 시각은 앞선다 — 선점 시각이 백데이트된 모양이다.
        long later = seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, OPEN_AT);

        assertThat(adapter.countOutOfOrderHistoryPairs(AS_OF, later))
                .as("이 쌍을 못 보면 리플레이가 USE 를 먼저 접어 전이표 위반을 오탐한다")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("순서가 맞으면 0 이다 — 정상 데이터가 실행을 죽이면 안 된다")
    void countsNothingWhenOrderIsConsistent() {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT);
        long later = seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, OPEN_AT.plusHours(1));

        assertThat(adapter.countOutOfOrderHistoryPairs(AS_OF, later)).isZero();
    }

    @Test
    @DisplayName("다른 발급건끼리는 안 센다 — 순서 계약은 발급건 안에서만 있다")
    void ignoresPairsAcrossIssuances() {
        long first = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(first, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(5));
        long second = seed.issuance(IssuanceStatus.ISSUED);
        long later = seed.history(second, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT);

        assertThat(adapter.countOutOfOrderHistoryPairs(AS_OF, later))
                .as("리플레이는 발급건마다 따로 접으므로 건너 비교하면 오탐이다")
                .isZero();
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

    /**
     * <b>완판이 조용히 미달로 뒤집히면 안 된다.</b> 컷을 {@code ON} 절에 두면 {@code LEFT JOIN}
     * 이라 회차는 남고 {@code total_quantity} 만 {@code NULL} 이 되어, 한 행에서 앞 다섯 컬럼은
     * 맞고 {@code sold_out_seconds} 만 거짓이 된다.
     *
     * <p>{@code WHERE} 로 옮기면 그 회차는 <b>행이 아예 안 써지고</b> 잡 수준의
     * {@code couponRows != couponCount} 검사가 {@code DATASET_MUTATED_DURING_RUN} 으로 잡는다.
     */
    @Test
    @DisplayName("asOf 이후에 갱신된 재고를 가진 회차는 행을 받지 않는다")
    void excludeCouponWhenStockUpdatedAfterAsOf() {
        seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED, OPEN_AT.plusSeconds(90));
        seed.overwriteStock(1, AS_OF.plusSeconds(1));

        assertThat(adapter.aggregateCouponStats(runId, AS_OF))
                .as("값을 뭉개지 말고 행을 빼야 행 수 검사가 잡는다")
                .isZero();
    }

    /** 재고 행이 <b>없는</b> 회차는 살려 둔다 — V1 이 그 회차를 잡는 것이 존재 이유다. */
    @Test
    @DisplayName("재고 행이 없는 회차는 행을 받는다 — 완판 판정만 NULL 이다")
    void keepCouponWithoutStockRow() {
        long couponId = seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED);
        seed.removeStock();

        assertThat(adapter.aggregateCouponStats(runId, AS_OF)).isEqualTo(1);
        assertThat(couponStatRow(couponId))
                .containsEntry("issued_total", 1)
                .containsEntry("sold_out_seconds", null);
    }

    /** {@code asOf} 뒤에 만들어진 회차는 스냅샷에 없다 — 같은 asOf 재실행이 갈리면 안 된다. */
    @Test
    @DisplayName("asOf 이후에 만들어진 회차는 집계에서 빠진다")
    void cutCouponsAtAsOf() {
        seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED);
        long late = seed.newCoupon();
        jdbcClient.sql("UPDATE coupons SET created_at = :at WHERE id = :id")
                .param("at", AS_OF.plusSeconds(1))
                .param("id", late)
                .update();

        assertThat(adapter.aggregateCouponStats(runId, AS_OF)).isEqualTo(1);
        assertThat(adapter.couponCount(AS_OF))
                .as("두 자리에 같은 컷이 있어야 등식이 뜻을 갖는다")
                .isEqualTo(1);
    }

    /**
     * <b>이력 없는 발급건을 짝으로 찾는다.</b> 이 사각은 CY-196 이 기록해 둔 것이다 —
     * 이력이 없는 발급건은 {@code asof_state} 에 안 실려 V3·V5 의 시야 밖이고, V4 는 반대
     * 방향(고아 이력)만 본다. 규칙 여섯 중 아무도 이 방향을 안 본다.
     */
    @Test
    @DisplayName("ISSUE 이력이 없는 발급건을 찾는다")
    void findIssuanceWithoutIssueHistory() {
        long withHistory = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(withHistory, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(1));
        long orphan = seed.issuance(IssuanceStatus.ISSUED);

        assertThat(adapter.countIssuancesWithBrokenIssueHistory(AS_OF, NO_LIMIT)).isEqualTo(1);
        assertThat(adapter.sampleIssuancesWithBrokenIssueHistory(AS_OF, NO_LIMIT, 10))
                .as("메시지에 실을 표본이 그 발급건을 지목해야 한다")
                .containsExactly(orphan);
    }

    /**
     * <b>총합 비교가 놓치는 자리다.</b> 한때 이 검산을
     * {@code COUNT(issuances) == SUM(hourly)} 로 뒀는데, 아래 데이터는 <b>총합이 같다</b> —
     * 발급건 2건, {@code ISSUE} 이력 2건. 그래서 그 형태로는 그냥 통과했다.
     *
     * <p>이 저장소가 {@code finding_count} 비교를 거부한 것과 정확히 같은 형태다 —
     * <i>"오탐 400 + 누락 400 도 800"</i>. 짝으로 봐야 잡힌다.
     */
    @Test
    @DisplayName("이력 없는 발급건 하나 + 이력 둘인 발급건 하나 — 총합은 같지만 둘 다 잡는다")
    void catchAsymmetricMismatchThatSumsHide() {
        long twoHistories = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(twoHistories, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(1));
        seed.history(twoHistories, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(2));
        long orphan = seed.issuance(IssuanceStatus.ISSUED);

        int issueHistories = adapter.issuedByHour(NO_LIMIT, AS_OF).stream()
                .mapToInt(HourlyIssued::issuedTotal)
                .sum();
        assertThat(issueHistories)
                .as("발급건 2건 · ISSUE 이력 2건 — 총합 비교는 여기서 통과한다")
                .isEqualTo(2);
        assertThat(adapter.countIssuancesWithBrokenIssueHistory(AS_OF, NO_LIMIT))
                .as("두 발급건이 다 깨져 있다. 없는 쪽만 보면 하나만 나온다")
                .isEqualTo(2);
        assertThat(adapter.sampleIssuancesWithBrokenIssueHistory(AS_OF, NO_LIMIT, 10))
                .containsExactlyInAnyOrder(twoHistories, orphan);
    }

    /**
     * <b>중복만 있는 경우.</b> {@code NOT EXISTS} 로만 두면 이 데이터가 <b>통째로 통과한다</b> —
     * 이력 없는 발급건이 하나도 없어 짝이 다 맞는 것처럼 보인다.
     *
     * <p>그런데 {@code hourly_stats} 는 <b>이력 행</b>을 세므로 발급 1건에 2가 적힌다.
     * 대시보드에서는 데이터 파손이 아니라 "그 시각에 발급이 많았다" 로 보이고, 스냅샷은
     * {@code COMPLETE} 로 닫혀 뷰에 걸린다.
     */
    @Test
    @DisplayName("ISSUE 이력이 둘인 발급건도 잡는다 — hourly 가 과대 집계된다")
    void catchDuplicateIssueHistory() {
        long duplicated = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(duplicated, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(1));
        seed.history(duplicated, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(2));

        assertThat(adapter.issuedByHour(NO_LIMIT, AS_OF).stream()
                .mapToInt(HourlyIssued::issuedTotal)
                .sum())
                .as("발급은 1건인데 hourly 는 2를 적는다 — 이것이 막으려는 상태다")
                .isEqualTo(2);
        assertThat(adapter.countIssuancesWithBrokenIssueHistory(AS_OF, NO_LIMIT))
                .as("이력 없는 발급건은 하나도 없다. 없는 쪽만 보면 0 이라 통과한다")
                .isEqualTo(1);
        assertThat(adapter.sampleIssuancesWithBrokenIssueHistory(AS_OF, NO_LIMIT, 10))
                .containsExactly(duplicated);
    }

    /** 창은 리플레이와 같다. 얼린 상한 밖의 이력은 "있다" 로 세지 않는다. */
    @Test
    @DisplayName("얼린 상한 밖의 ISSUE 이력은 짝으로 안 세어진다")
    void respectFrozenBoundaryWhenPairing() {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        long frozen = seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, OPEN_AT.plusHours(1));

        assertThat(adapter.countIssuancesWithBrokenIssueHistory(AS_OF, frozen)).isZero();
        assertThat(adapter.countIssuancesWithBrokenIssueHistory(AS_OF, frozen - 1))
                .as("그 이력이 창 밖이면 이 발급건은 이력이 없는 것과 같다")
                .isEqualTo(1);
    }

    /** 재시작하면 앞 실행의 부분 결과가 남는다. 그 위에 다시 쓰면 중복키로 죽는다. */
    @Test
    @DisplayName("같은 실행의 앞 스냅샷을 지우고 다시 쓴다")
    void clearBeforeRewrite() {
        seed.issuance(IssuanceStatus.ISSUED);
        adapter.aggregateCouponStats(runId, AS_OF);
        adapter.aggregateGradeStats(runId, AS_OF);
        adapter.appendHourlyStats(runId, HourlyIssued.fillAll(List.of()));

        adapter.clear(runId);

        // 세 테이블을 다 본다. 하나만 보면 나머지 둘의 DELETE 가 빠져도 초록이다 —
        // 그때 재실행이 (run_id, coupon_id) 중복키로 죽는다.
        assertThat(rowsIn("coupon_stats")).isZero();
        assertThat(rowsIn("grade_stats")).isZero();
        assertThat(rowsIn("hourly_stats")).isZero();

        assertThat(adapter.aggregateCouponStats(runId, AS_OF))
                .as("지운 뒤 다시 쓸 수 있어야 한다")
                .isEqualTo(1);
    }

    private int rowsIn(String table) {
        return jdbcClient.sql("SELECT COUNT(*) FROM " + table + " WHERE run_id = :runId")
                .param("runId", runId)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>등급 집계도 회차 집합이 같아야 한다.</b> 회차 컷이 이쪽에만 없으면 {@code asOf} 뒤에
     * 만들어진 회차의 발급건이 {@code grade_stats} 에만 남는다 — 회차 수 등식은 두 문장이 그
     * 회차를 함께 제외해 침묵하므로 아무도 못 잡는다.
     */
    @Test
    @DisplayName("asOf 이후에 만들어진 회차는 등급 집계에서도 빠진다")
    void cutCouponsAtAsOfInGradeStats() {
        seed.currentCouponIdOrCreate();
        seed.issuance(IssuanceStatus.ISSUED);
        long late = seed.newCoupon();
        jdbcClient.sql("UPDATE coupons SET created_at = :at WHERE id = :id")
                .param("at", AS_OF.plusSeconds(1))
                .param("id", late)
                .update();
        // 늦은 회차에 달린 발급건은 asOf 이하다 — 그래야 발급건 컷만으로는 안 걸린다.
        seed.issuance(IssuanceStatus.ISSUED);

        assertThat(adapter.aggregateGradeStats(runId, AS_OF))
                .as("늦은 회차의 등급 쌍은 빠진다")
                .isEqualTo(1);
    }
}
