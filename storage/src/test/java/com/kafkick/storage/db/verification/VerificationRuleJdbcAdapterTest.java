package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import javax.sql.DataSource;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.replay.ReplayResult;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

@RepositoryTest
@Import({VerificationRuleJdbcAdapter.class, AsOfStateJdbcAdapter.class,
        VerificationRunJdbcAdapter.class})
class VerificationRuleJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);
    private static final int LIMIT = 1000;

    @Autowired
    private AsOfStateJdbcAdapter asOfStates;

    @Autowired
    private VerificationRunJdbcAdapter runAdapter;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private VerificationRuleJdbcAdapter adapter;

    private VerificationSeed data;
    private long runId;

    @BeforeEach
    void setUp() {
        data = new VerificationSeed(jdbcClient);
        runId = newRun(1);
    }

    private long newRun(int attempt) {
        return runAdapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, AS_OF)).id();
    }

    // ─────────────────────────── V3 리플레이 대조 ───────────────────────────

    @Test
    @DisplayName("접은 상태와 저장된 상태가 같으면 검출이 없다 — 정상셋 0건이 성립해야 한다")
    void findNoReplayMismatchWhenStatesAgree() {
        replayed(IssuanceStatus.USED, IssuanceStatus.USED, 1);

        assertThat(adapter.findReplayMismatches(runId, AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("이력은 USED 인데 저장값이 ISSUED 면 잡는다 — 오염 유형 2 의 모양이다")
    void findReplayMismatchForStaleStatus() {
        long issuanceId = replayed(IssuanceStatus.USED, IssuanceStatus.ISSUED, 1);

        assertThat(adapter.findReplayMismatches(runId, AS_OF, LIMIT)).singleElement()
                .satisfies(finding -> {
                    assertThat(finding.type()).isEqualTo(FindingType.REPLAY_MISMATCH);
                    assertThat(finding.targetKey()).isEqualTo("ISSUANCE:" + issuanceId);
                    assertThat(finding.issuanceId()).isEqualTo(issuanceId);
                    assertThat(finding.expected()).isEqualTo("replay=USED");
                    assertThat(finding.actual()).isEqualTo("issuances.status=ISSUED");
                });
    }

    @ParameterizedTest
    @EnumSource(IssuanceStatus.class)
    @DisplayName("네 상태 모두 저장값과 어긋나면 잡힌다")
    void findReplayMismatchForEveryState(IssuanceStatus replayed) {
        IssuanceStatus stored = replayed == IssuanceStatus.ISSUED
                ? IssuanceStatus.USED
                : IssuanceStatus.ISSUED;
        replayed(replayed, stored, replayed == IssuanceStatus.USED ? 1 : 0);

        assertThat(adapter.findReplayMismatches(runId, AS_OF, LIMIT)).hasSize(1);
    }

    @Test
    @DisplayName("다른 run 의 행은 보지 않는다")
    void ignoreOtherRunOnReplayMismatch() {
        long otherRunId = newRun(2);
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);
        asOfStates.appendAll(otherRunId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.USED, 1L, AS_OF, List.of())));

        assertThat(adapter.findReplayMismatches(runId, AS_OF, LIMIT)).isEmpty();
    }

    // ─────────────────────────── V5 사용 실적 정합 ───────────────────────────

    @Test
    @DisplayName("USED 에 활성 사용 1건이면 정상이다")
    void findNoUsageMismatchForUsedWithOneUsage() {
        replayed(IssuanceStatus.USED, IssuanceStatus.USED, 1);

        assertThat(adapter.findUsageMismatches(runId, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("ISSUED 인데 활성 사용이 남아 있으면 잡는다 — 오염 유형 7 의 모양이다")
    void findUsageMismatchForIssuedWithActiveUsage() {
        long issuanceId = replayed(IssuanceStatus.ISSUED, IssuanceStatus.ISSUED, 1);

        assertThat(adapter.findUsageMismatches(runId, LIMIT)).singleElement()
                .satisfies(finding -> {
                    assertThat(finding.type()).isEqualTo(FindingType.USAGE_MISMATCH);
                    assertThat(finding.targetKey()).isEqualTo("ISSUANCE:" + issuanceId);
                    assertThat(finding.expected()).isEqualTo("active_usage=0");
                    assertThat(finding.actual()).isEqualTo("active_usage=1");
                });
    }

    @Test
    @DisplayName("USED 인데 활성 사용이 없으면 잡는다 — 반대 방향도 봐야 한다")
    void findUsageMismatchForUsedWithoutUsage() {
        replayed(IssuanceStatus.USED, IssuanceStatus.USED, 0);

        assertThat(adapter.findUsageMismatches(runId, LIMIT)).singleElement()
                .satisfies(finding -> {
                    assertThat(finding.expected()).isEqualTo("active_usage=1");
                    assertThat(finding.actual()).isEqualTo("active_usage=0");
                });
    }

    @Test
    @DisplayName("USED 에 활성 사용이 둘이면 잡는다 — 한 발급건의 이중 사용이다")
    void findUsageMismatchForDoubleUse() {
        replayed(IssuanceStatus.USED, IssuanceStatus.USED, 2);

        assertThat(adapter.findUsageMismatches(runId, LIMIT)).singleElement()
                .extracting(VerificationFinding::actual)
                .isEqualTo("active_usage=2");
    }

    @ParameterizedTest
    @EnumSource(value = IssuanceStatus.class, names = {"ISSUED", "CANCELLED", "EXPIRED"})
    @DisplayName("USED 가 아닌 상태는 활성 사용이 0이어야 한다")
    void requireZeroUsageForNonUsedStates(IssuanceStatus state) {
        replayed(state, state, 0);

        assertThat(adapter.findUsageMismatches(runId, LIMIT)).isEmpty();
    }

    // ─────────────────────────── 상한 ───────────────────────────

    @Test
    @DisplayName("상한만큼만 돌려준다 — 검증기가 망가지면 위반이 수백만 건으로 튄다")
    void capResultsAtLimit() {
        replayed(IssuanceStatus.USED, IssuanceStatus.ISSUED, 1);
        replayed(IssuanceStatus.USED, IssuanceStatus.ISSUED, 1);

        assertThat(adapter.findReplayMismatches(runId, AS_OF, 1)).hasSize(1);
    }

    @Test
    @DisplayName("asOf 이후에 갱신된 발급건은 비교하지 않는다 — 배치가 도는 동안 런타임이 건드린 것이다")
    void ignoreIssuanceUpdatedAfterAsOf() {
        long issuanceId = replayed(IssuanceStatus.USED, IssuanceStatus.ISSUED, 1);
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF.plusSeconds(1))
                .param("id", issuanceId)
                .update();

        assertThat(adapter.findReplayMismatches(runId, AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("asOf 와 같은 시각에 갱신된 발급건은 비교한다 — 경계는 포함이다")
    void compareIssuanceUpdatedExactlyAtAsOf() {
        long issuanceId = replayed(IssuanceStatus.USED, IssuanceStatus.ISSUED, 1);
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF)
                .param("id", issuanceId)
                .update();

        assertThat(adapter.findReplayMismatches(runId, AS_OF, LIMIT)).hasSize(1);
    }

    // ─────────────────────────── V1 재고 정합 ───────────────────────────

    @Test
    @DisplayName("접은 활성 건수와 재고가 같으면 검출이 없다 — 정상셋 0건이 성립해야 한다")
    void findNoStockMismatchWhenCountsAgree() {
        replayed(IssuanceStatus.ISSUED, IssuanceStatus.ISSUED, 0);
        replayed(IssuanceStatus.USED, IssuanceStatus.USED, 1);
        data.overwriteStock(2);

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("ISSUED 와 USED 만 활성이다 — CANCELLED·EXPIRED 는 재고로 돌아간 것이라 안 센다")
    void countOnlyIssuedAndUsedAsActive() {
        replayed(IssuanceStatus.ISSUED, IssuanceStatus.ISSUED, 0);
        replayed(IssuanceStatus.USED, IssuanceStatus.USED, 1);
        replayed(IssuanceStatus.CANCELLED, IssuanceStatus.CANCELLED, 0);
        replayed(IssuanceStatus.EXPIRED, IssuanceStatus.EXPIRED, 0);
        data.overwriteStock(2);

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("재고는 줄었는데 ISSUE 이력이 없으면 잡는다 — 오염 유형 1 의 모양이다")
    void findStockMismatchWhenStockDroppedWithoutIssuance() {
        data.overwriteStock(1);

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT))
                .singleElement()
                .satisfies(finding -> {
                    assertThat(finding.type()).isEqualTo(FindingType.STOCK_MISMATCH);
                    assertThat(finding.targetKey()).isEqualTo("COUPON:" + data.currentCouponId());
                    assertThat(finding.expected()).isEqualTo("replay=0");
                    assertThat(finding.actual()).isEqualTo("coupon_stocks.active_count=1");
                });
    }

    @Test
    @DisplayName("활성이 0인 회차도 검사한다 — asof_state 를 드라이빙으로 잡으면 통째로 빠지는 자리다")
    void inspectCouponsWithoutAnyActiveIssuance() {
        replayed(IssuanceStatus.CANCELLED, IssuanceStatus.CANCELLED, 0);
        data.overwriteStock(3);

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT))
                .singleElement()
                .satisfies(finding -> assertThat(finding.expected()).isEqualTo("replay=0"));
    }

    @Test
    @DisplayName("CANCEL_USE 이중 복원처럼 재고가 더 많아도 잡는다 — 양방향이다")
    void findStockMismatchWhenStockIsHigherThanReplay() {
        replayed(IssuanceStatus.ISSUED, IssuanceStatus.ISSUED, 0);
        replayed(IssuanceStatus.USED, IssuanceStatus.USED, 1);
        data.overwriteStock(5);

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT))
                .singleElement()
                .satisfies(finding -> {
                    assertThat(finding.expected()).isEqualTo("replay=2");
                    assertThat(finding.actual()).isEqualTo("coupon_stocks.active_count=5");
                });
    }

    @Test
    @DisplayName("재고 행이 없는데 활성 발급이 있으면 잡는다 — 재고 없이 발급이 쌓이는 것이 가장 위험하다")
    void findStockMismatchWhenStockRowIsMissing() {
        replayed(IssuanceStatus.ISSUED, IssuanceStatus.ISSUED, 0);
        data.removeStock();

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT))
                .as("coupon_stocks 를 드라이빙으로 잡으면 이 회차가 통째로 빠진다")
                .singleElement()
                .satisfies(finding -> {
                    assertThat(finding.expected()).isEqualTo("replay=1");
                    assertThat(finding.actual()).isEqualTo("coupon_stocks.active_count=0");
                });
    }

    @Test
    @DisplayName("재고 행도 활성도 없으면 검출이 없다 — 회차만 만들어진 상태는 정상이다")
    void findNoStockMismatchForEmptyCoupon() {
        data.removeStock();

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("다른 run 의 접기 결과는 세지 않는다")
    void ignoreOtherRunOnStockMismatch() {
        long otherRun = newRun(2);
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);   // 시드가 재고를 1 로 맞춘다
        asOfStates.appendAll(otherRun, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.ISSUED, 1L, AS_OF, List.of())));
        data.overwriteStock(1);

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT))
                .as("이 run 은 접은 것이 없으니 기대가 0 이고 재고 1 과 어긋난다")
                .singleElement()
                .satisfies(finding -> assertThat(finding.expected()).isEqualTo("replay=0"));
    }

    @Test
    @DisplayName("asOf 이후에 갱신된 재고는 비교하지 않는다 — 배치가 도는 동안 발급이 일어난 것이다")
    void ignoreStockUpdatedAfterAsOf() {
        data.overwriteStock(1, AS_OF.plusSeconds(1));

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("asOf 와 같은 시각에 갱신된 재고는 비교한다 — 경계는 포함이다")
    void compareStockUpdatedExactlyAtAsOf() {
        data.overwriteStock(1, AS_OF);

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT)).hasSize(1);
    }

    @Test
    @DisplayName("재고 규칙도 상한을 지킨다")
    void capStockResultsAtLimit() {
        data.overwriteStock(1);

        assertThat(adapter.findStockMismatches(runId, AS_OF, 1)).hasSize(1);
        assertThatThrownBy(() -> adapter.findStockMismatches(runId, AS_OF, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("어긋난 회차만 잡는다 — 회차가 둘일 때 귀속이 밀리지 않아야 한다")
    void attributeStockMismatchToTheRightCoupon() {
        replayed(IssuanceStatus.ISSUED, IssuanceStatus.ISSUED, 0);
        data.overwriteStock(1);                                   // 회차 A: 맞음
        long broken = data.newCoupon();
        replayed(IssuanceStatus.ISSUED, IssuanceStatus.ISSUED, 0);
        data.overwriteStock(4);                                   // 회차 B: 1 vs 4

        assertThat(adapter.findStockMismatches(runId, AS_OF, LIMIT))
                .as("GROUP BY 를 지우고 전체 합계를 내도 회차가 하나면 통과한다")
                .extracting(VerificationFinding::targetKey)
                .containsExactly("COUPON:" + broken);
    }

    // ─────────────────────────── V6 등급 자격 ───────────────────────────

    @Test
    @DisplayName("허용 등급으로 발급됐으면 검출이 없다 — 정상셋 0건이 성립해야 한다")
    void findNoGradeViolationForEligibleGrade() {
        data.restrictCouponTo(12);          // {GOLD, VIP}
        data.issuance(IssuanceStatus.ISSUED, "VIP");
        data.issuance(IssuanceStatus.ISSUED, "GOLD");

        assertThat(adapter.findGradeViolations(AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("허용 집합에 없는 등급으로 발급됐으면 잡는다")
    void findGradeViolationForIneligibleGrade() {
        data.restrictCouponTo(12);          // {GOLD, VIP}
        long issuanceId = data.issuance(IssuanceStatus.ISSUED, "SILVER");

        assertThat(adapter.findGradeViolations(AS_OF, LIMIT))
                .singleElement()
                .satisfies(finding -> {
                    assertThat(finding.type()).isEqualTo(FindingType.GRADE_VIOLATION);
                    assertThat(finding.targetKey()).isEqualTo("ISSUANCE:" + issuanceId);
                    assertThat(finding.expected()).isEqualTo("eligible_grades_mask=12");
                    assertThat(finding.actual()).isEqualTo("issued_grade=SILVER bit=2");
                });
    }

    @Test
    @DisplayName("마스크는 순서가 아니라 집합이다 — 중간을 건너뛴 마스크에서 갈린다")
    void treatMaskAsSetNotThreshold() {
        data.restrictCouponTo(9);           // {WELCOME, VIP} — SILVER·GOLD 를 건너뛴다
        data.issuance(IssuanceStatus.ISSUED, "WELCOME");
        data.issuance(IssuanceStatus.ISSUED, "VIP");
        long silver = data.issuance(IssuanceStatus.ISSUED, "SILVER");
        long gold = data.issuance(IssuanceStatus.ISSUED, "GOLD");

        assertThat(adapter.findGradeViolations(AS_OF, LIMIT))
                .as("등급 순서로 판정하면 WELCOME 이 잡히고 GOLD 가 빠져 정반대가 된다")
                .extracting(VerificationFinding::targetKey)
                .containsExactly("ISSUANCE:" + silver, "ISSUANCE:" + gold);
    }

    @ParameterizedTest(name = "마스크 {0} 에서 {1} 은 위반이 아니다")
    @CsvSource({"1, WELCOME", "2, SILVER", "4, GOLD", "8, VIP", "15, VIP", "15, WELCOME"})
    @DisplayName("자기 비트가 마스크에 있으면 통과한다")
    void passWhenBitIsInMask(int mask, String grade) {
        data.restrictCouponTo(mask);
        data.issuance(IssuanceStatus.ISSUED, grade);

        assertThat(adapter.findGradeViolations(AS_OF, LIMIT)).isEmpty();
    }

    /**
     * <b>CLEAN 스키마에서는 FK 가 이 상태를 물리적으로 막는다.</b>
     * 그래도 판정 SQL 은 {@code LEFT JOIN} 으로 둔다 — 오염셋은 제약을 떼고 심으므로
     * 그때 살아나고, {@code INNER JOIN} 이면 그 행이 조용히 빠져 미검출이 된다.
     *
     * <p>이 테스트는 그 FK 가 아직 있다는 것을 고정한다. 누가 떼면
     * {@code LEFT JOIN} 가지가 <b>도달 가능해지므로 그때는 실제 검출 테스트가 필요하다</b> —
     * 이 테스트가 빨개지는 것이 그 신호다.
     */
    @Test
    @DisplayName("grades 에 없는 등급은 CLEAN 에서 FK 가 막는다 — LEFT JOIN 가지는 오염셋용이다")
    void unknownGradeIsBlockedByForeignKeyInCleanSchema() {
        assertThatThrownBy(() -> data.issuance(IssuanceStatus.ISSUED, "PLATINUM"))
                .as("FK 가 사라지면 이 단언이 실패한다. 그때 LEFT JOIN 가지에 검출 테스트를 붙여야 한다")
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("현재 회원 등급이 아니라 발급 시점 스냅샷으로 판정한다 — 강등돼도 정상이다")
    void judgeByIssuedGradeSnapshotNotCurrentMemberGrade() {
        data.restrictCouponTo(8);           // {VIP}
        data.issuance(IssuanceStatus.ISSUED, "VIP");
        jdbcClient.sql("UPDATE members SET membership_grade = 'WELCOME'").update();

        assertThat(adapter.findGradeViolations(AS_OF, LIMIT))
                .as("members 를 조인하면 강등되는 순간 정상 발급이 전부 위반으로 잡힌다")
                .isEmpty();
    }

    @Test
    @DisplayName("asOf 이후에 갱신된 발급건은 보지 않는다 — V3 와 같은 기준이다")
    void ignoreIssuanceUpdatedAfterAsOfOnGradeViolation() {
        data.restrictCouponTo(12);
        long issuanceId = data.issuance(IssuanceStatus.ISSUED, "SILVER");
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF.plusSeconds(1))
                .param("id", issuanceId)
                .update();

        assertThat(adapter.findGradeViolations(AS_OF, LIMIT)).isEmpty();
    }

    @Test
    @DisplayName("등급 규칙도 상한을 지킨다")
    void capGradeResultsAtLimit() {
        data.restrictCouponTo(8);
        data.issuance(IssuanceStatus.ISSUED, "SILVER");
        data.issuance(IssuanceStatus.ISSUED, "GOLD");

        assertThat(adapter.findGradeViolations(AS_OF, 1)).hasSize(1);
        assertThatThrownBy(() -> adapter.findGradeViolations(AS_OF, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("발급건이 속한 회차의 마스크로 판정한다 — 회차가 둘일 때 엉뚱한 마스크를 읽으면 안 된다")
    void judgeAgainstOwnCouponMask() {
        data.restrictCouponTo(15);                       // 회차 A: 전 등급 허용
        data.issuance(IssuanceStatus.ISSUED, "SILVER");
        data.newCoupon();
        data.restrictCouponTo(12);                       // 회차 B: {GOLD, VIP}
        long violating = data.issuance(IssuanceStatus.ISSUED, "SILVER");

        assertThat(adapter.findGradeViolations(AS_OF, LIMIT))
                .as("JOIN coupons 가 엉뚱한 회차를 읽으면 A 쪽이 잡히거나 둘 다 잡힌다")
                .extracting(VerificationFinding::targetKey)
                .containsExactly("ISSUANCE:" + violating);
    }

    /**
     * {@code LEFT JOIN} 가지는 오염셋용이지만 <b>지금도 도달 가능하다</b> —
     * 세션 변수로 FK 를 잠깐 끄면 된다. 그 가지와 매퍼의 {@code wasNull()} 분기가
     * 실제로 도는지 확인해야, 제약 없는 스키마가 붙는 날 처음 알게 되는 일이 없다.
     */
    @Test
    @DisplayName("grades 에 없는 등급은 위반이다 — 제약을 뗀 오염 스키마의 모양이다")
    void findGradeViolationForUnknownGrade() {
        jdbcClient.sql("SET SESSION foreign_key_checks = 0").update();
        try {
            long issuanceId = data.issuance(IssuanceStatus.ISSUED, "PLATINUM");

            assertThat(adapter.findGradeViolations(AS_OF, LIMIT))
                    .singleElement()
                    .satisfies(finding -> {
                        assertThat(finding.targetKey()).isEqualTo("ISSUANCE:" + issuanceId);
                        assertThat(finding.actual())
                                .isEqualTo("issued_grade=PLATINUM grades 에 없는 등급");
                    });
        } finally {
            jdbcClient.sql("SET SESSION foreign_key_checks = 1").update();
        }
    }

    /**
     * <b>어댑터의 다섯 규칙을 한 테스트로 돈다.</b> 규칙마다 따로 쓰면 새 규칙이 붙을 때
     * 이 가드만 빠뜨리기 쉽다 — 실제로 V2 를 붙이면서 {@code requireLimit} 한 줄이 빠졌고
     * 규칙별 테스트로는 그것이 드러나지 않았다.
     *
     * <p>{@code LIMIT 0} 은 MySQL 에서 에러가 아니라 <b>0행</b>이다. 가드가 없으면 규칙이
     * 조용히 아무것도 안 잡고 잡은 성공으로 끝난다 — 정상셋 0건이 합격 조건이라
     * <b>그 침묵은 성공과 구분되지 않는다.</b>
     *
     * <p><b>V4 는 여기 없다.</b> 그 상한은 어댑터가 아니라 {@code IllegalTransitionItemWriter}
     * 생성자에 있고 batch 모듈이라 storage 테스트에서 못 부른다 — 대칭 테스트가 그쪽에 있다.
     */
    @Test
    @DisplayName("어댑터 규칙 다섯이 모두 상한 0 과 음수를 거부한다")
    void rejectNonPositiveLimit() {
        Map<String, IntFunction<List<VerificationFinding>>> rules = new LinkedHashMap<>();
        rules.put("V1 findStockMismatches", limit -> adapter.findStockMismatches(1L, AS_OF, limit));
        rules.put("V2 findDuplicateIssuances", limit -> adapter.findDuplicateIssuances(AS_OF, limit));
        rules.put("V3 findReplayMismatches", limit -> adapter.findReplayMismatches(1L, AS_OF, limit));
        rules.put("V5 findUsageMismatches", limit -> adapter.findUsageMismatches(1L, limit));
        rules.put("V6 findGradeViolations", limit -> adapter.findGradeViolations(AS_OF, limit));

        rules.forEach((name, rule) -> {
            assertThatThrownBy(() -> rule.apply(0))
                    .as(name + " 이 상한 0 을 통과시킨다 — LIMIT 0 은 에러가 아니라 0행이다")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("검출 상한은 1 이상");
            assertThatThrownBy(() -> rule.apply(-1))
                    .as(name + " 이 음수 상한을 통과시킨다")
                    .isInstanceOf(IllegalArgumentException.class);
        });
    }

    // ─────────────────────────── 회차 정책 축 가드 ───────────────────────────

    @Test
    @DisplayName("마스크가 바뀌면 정책 지문이 달라진다")
    void detectMaskChangeInPolicyDigest() {
        data.currentCouponIdOrCreate();
        String before = adapter.policyDigest();

        data.restrictCouponTo(12);

        assertThat(adapter.policyDigest()).isNotEqualTo(before);
    }

    /**
     * 나머지 지문 테스트가 전부 <b>달라지는 것</b>만 본다. 가드의 실제 판정은 두 번 계산해
     * 같은지 보는 것이라, 식에 비결정적 재료가 섞이면 <b>정상 데이터에서 매 실행이 거부된다</b> —
     * 그 사고는 여기서만 걸린다.
     */
    @Test
    @DisplayName("데이터가 그대로면 지문이 같다 — 정상 실행이 거부되면 안 된다")
    void keepPolicyDigestStableWhenNothingChanges() {
        data.issuance(IssuanceStatus.ISSUED);

        assertThat(adapter.policyDigest()).isEqualTo(adapter.policyDigest());
    }

    @Test
    @DisplayName("등급 비트가 재배치되면 정책 지문이 달라진다 — 마스크와 AND 되는 반대쪽 피연산자다")
    void detectGradeBitChangeInPolicyDigest() {
        data.issuance(IssuanceStatus.ISSUED);   // grades 행은 발급을 세울 때 들어간다
        String before = adapter.policyDigest();

        jdbcClient.sql("UPDATE grades SET bit_value = 16 WHERE code = 'SILVER'").update();

        assertThat(adapter.policyDigest())
                .as("coupons 만 접으면 여기서 지문이 같아 V6 검출만 조용히 달라진다")
                .isNotEqualTo(before);
    }

    @Test
    @DisplayName("회차가 늘면 정책 지문이 달라진다 — XOR 만으로는 못 잡는 경우가 있다")
    void detectCouponInsertInPolicyDigest() {
        data.currentCouponIdOrCreate();
        String before = adapter.policyDigest();

        data.newCoupon();

        assertThat(adapter.policyDigest()).isNotEqualTo(before);
    }

    /**
     * <b>{@code GROUP_CONCAT} 으로 짜면 여기서 빨개진다.</b> CLEAN 147행이 실측 920 바이트, CORRUPT 291행이면 약 1820 바이트로
     * {@code group_concat_max_len} 기본값 1024 의 90% 다. 여기서는 그 선을 넘겨,
     * 잘린 뒤 회차의 변경이 지문에 안 나타나는 것을 잡는다 —
     * MySQL 은 경고만 내므로 <b>가드가 열린 채로 실패한다.</b>
     */
    @Test
    @DisplayName("지문이 길이 한계에 걸리지 않는다 — GROUP_CONCAT 은 1024 바이트에서 조용히 잘린다")
    void detectChangeBeyondGroupConcatLimit() {
        long last = 0;
        for (int i = 0; i < 220; i++) {
            last = data.newCoupon();
        }
        String before = adapter.policyDigest();

        jdbcClient.sql("UPDATE coupons SET eligible_grades_mask = 3 WHERE id = :id")
                .param("id", last)
                .update();

        assertThat(adapter.policyDigest())
                .as("잘린 지문이면 마지막 회차의 변경이 안 보인다")
                .isNotEqualTo(before);
    }

    // ─────────────────────────── 재고 축 가드 ───────────────────────────

    @Test
    @DisplayName("asOf 이후에 갱신된 재고가 있으면 알려준다")
    void detectStocksUpdatedAfterAsOf() {
        data.overwriteStock(1, AS_OF.plusSeconds(1));

        assertThat(adapter.hasStocksUpdatedAfter(AS_OF)).isTrue();
    }

    /**
     * <b>{@code V2026082503__coupon_stock_precision.sql} 을 되돌리면 이 테스트가 빨개진다.</b>
     * {@code datetime(0)} 에서는 {@code .6} 이 <b>올림</b>돼 +1초가 되므로 첫 단언이 뒤집힌다.
     *
     * <p>그 마이그레이션이 막는 것은 둘 다 침묵 실패다 — 올림이면 정상 데이터인데 실행이 거부되고,
     * 내림이면 진짜 갱신이 가드를 통과한다. 테스트가 없으면 누가 되돌려도 게이트 날까지 모른다.
     */
    @Test
    @DisplayName("재고 시각의 소수 초가 살아 있다 — 반올림되면 정상 데이터가 거부된다")
    void keepSubSecondPrecisionOnStockUpdatedAt() {
        LocalDateTime precise = AS_OF.minusHours(1).withNano(600_000_000);
        data.overwriteStock(1, precise);

        assertThat(adapter.hasStocksUpdatedAfter(precise))
                .as("datetime(0) 이면 .6 이 +1초로 올림돼 true 가 된다")
                .isFalse();
        assertThat(adapter.hasStocksUpdatedAfter(precise.minusNanos(1_000)))
                .as("바로 앞 시각 기준으로는 갱신으로 보여야 한다")
                .isTrue();
    }

    @Test
    @DisplayName("asOf 와 같은 시각까지는 갱신으로 보지 않는다 — 경계는 포함이다")
    void treatStockUpdatedAtAsOfAsFrozen() {
        data.overwriteStock(1, AS_OF);

        assertThat(adapter.hasStocksUpdatedAfter(AS_OF)).isFalse();
    }

    /** 발급건을 만들고 저장 상태와 접힌 상태를 따로 세운다. */
    private long replayed(IssuanceStatus replayedState, IssuanceStatus storedStatus, int usages) {
        long issuanceId = data.issuance(storedStatus);
        asOfStates.appendAll(runId, List.of(
                new ReplayResult(issuanceId, replayedState, 1L, AS_OF, List.of())));

        if (usages > 0) {
            jdbcClient.sql("""
                            UPDATE asof_state SET active_usage_count = :count
                             WHERE run_id = :runId AND coupon_id = :id
                            """)
                    .param("count", usages)
                    .param("runId", runId)
                    .param("id", issuanceId)
                    .update();
        }
        return issuanceId;
    }

    /**
     * <b>이 질의가 죽으면 {@code SchemaPresenceGuard} 가 조용히 무력해진다.</b> 가드는
     * 빈 목록을 "전부 있다" 로 읽으므로, 질의가 늘 빈 목록을 주는 형태로 망가지면
     * <b>기동은 언제나 통과</b>한다 — 가드가 있는데 아무것도 안 막는 상태다.
     *
     * <p>그래서 두 축을 함께 본다. 갖춰진 스키마에서 빈 목록인 것, 그리고 실재하지 않는
     * 이름은 실제로 <b>없다고</b> 답하는 것. 뒤 축이 없으면 {@code return List.of()} 로
     * 바꿔도 이 테스트가 초록이다.
     */
    @Test
    @DisplayName("갖춰진 스키마에서는 없는 테이블이 없다")
    void reportsNoMissingTableOnAMigratedSchema() {
        assertThat(adapter.missingCoreTables()).isEmpty();
    }

    /**
     * <b>검출 쪽 축이다.</b> 위 테스트만 있으면 {@code return List.of()} 로 바꿔도 초록이라
     * 가드가 아무것도 안 막는 상태를 아무도 모른다 — 돌연변이로 확인한 사실이다.
     *
     * <p>핵심 테이블이 하나도 없는 스키마가 필요한데, 컨테이너에서 테이블을 지울 수는 없다
     * (같은 스키마를 쓰는 다른 테스트가 죽는다). {@code CREATE DATABASE} 도 안 쓴다 —
     * Testcontainers 계정 권한에 기대게 된다. 대신 {@code catalog} 만 갈아 끼운다.
     * {@code information_schema} 는 어디에나 있고 읽기 권한이 보장되며 배치의 핵심 테이블은
     * 당연히 없다. MySQL 에서 {@code setCatalog} 는 {@code USE} 와 같아 어댑터 질의의
     * {@code DATABASE()} 가 그것을 따라간다.
     *
     * <p><b>⚠️ 되돌리는 것은 아래 {@code finally} 하나뿐이다.</b> HikariCP 는
     * {@code catalog} 프로퍼티가 설정돼 있지 않으면 반환된 커넥션의 catalog 를
     * <b>복구하지 않는다</b>({@code PoolBase.resetConnectionState} 가 그 필드가 null 이면
     * 건너뛴다 — 바이트코드로 확인). 이 컨텍스트에는 그 프로퍼티가 없다. 그러니 저 줄을
     * 지우면 오염된 커넥션이 풀에 남고, {@code @RepositoryTest} 는 컨텍스트를 캐시해 풀을
     * 공유하므로 <b>다른 테스트 클래스가 실행마다 다르게 빨개진다</b> — 원인을 아무도 못 찾는다.
     * 그래서 복구를 마지막에 단언으로 못 박는다.
     */
    @Test
    @DisplayName("핵심 테이블이 없는 스키마에서는 여덟을 전부 없다고 답한다")
    void reportsEveryCoreTableMissingOnAForeignSchema() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            String original = connection.getCatalog();
            try {
                connection.setCatalog("information_schema");
                VerificationRuleJdbcAdapter probe = new VerificationRuleJdbcAdapter(
                        JdbcClient.create(new SingleConnectionDataSource(connection, true)));

                assertThat(probe.currentSchema())
                        .as("실제로 다른 스키마를 보고 있어야 아래 단언이 뜻을 갖는다")
                        .isEqualTo("information_schema");
                assertThat(probe.missingCoreTables())
                        .as("이게 비면 SchemaPresenceGuard 가 빈 DB 를 통과시킨다")
                        .containsExactlyInAnyOrder(
                                "issuances", "issuance_histories", "verification_runs", "coupon_stocks",
                                "BATCH_JOB_INSTANCE", "BATCH_JOB_EXECUTION", "BATCH_STEP_EXECUTION",
                                // CY-368 이 nextAttempt 의 필수 의존으로 승격시켰다.
                                "BATCH_JOB_EXECUTION_PARAMS");
            } finally {
                connection.setCatalog(original);
            }
            assertThat(connection.getCatalog())
                    .as("여기서 안 돌려놓으면 Hikari 도 안 돌려놓는다 — 풀에 오염된 커넥션이 남는다")
                    .isEqualTo(original);
        }
    }

    /**
     * <b>메타만 빈 상태가 정상 절차에서 실제로 생긴다.</b> 검증용 셋은 cy-seed 의
     * {@code ddl/} 로 만들어지는데 거기에 {@code BATCH_*} 가 하나도 없다. 데이터 넷은 다
     * 있고 메타만 없으면 기동은 통과하고 <b>첫 잡 실행에서</b> SQL 에러로 죽는다 —
     * 가드가 없애려는 바로 그 늦은 실패다.
     */
    @Test
    @DisplayName("메타 테이블도 함께 본다 — 검증용 셋에는 그것이 없다")
    void watchesBatchMetadataTablesToo() {
        assertThat(adapter.missingCoreTables())
                .as("이 컨테이너는 V2 까지 적용돼 있어 비어야 한다")
                .isEmpty();

        Integer metaTables = jdbcClient.sql("""
                        SELECT COUNT(*)
                          FROM information_schema.tables
                         WHERE table_schema = DATABASE()
                           AND table_name LIKE 'BATCH\\_%'
                        """)
                .query(Integer.class)
                .single();

        assertThat(metaTables)
                .as("메타가 애초에 없으면 위 단언이 '메타를 안 본다' 여도 통과한다")
                .isGreaterThanOrEqualTo(3);
    }

    /**
     * 컬럼 축도 두 방향을 본다. 갖춰진 스키마에서 비는 것, 그리고 그 컬럼이 없는 스키마에서
     * 실제로 <b>없다고</b> 답하는 것. 뒤 축이 없으면 {@code return List.of()} 로 바꿔도 초록이다.
     */
    @Test
    @DisplayName("갖춰진 스키마에서는 없는 컬럼이 없다")
    void reportsNoMissingColumnOnAMigratedSchema() {
        assertThat(adapter.missingCriticalColumns()).isEmpty();
    }

    @Test
    @DisplayName("그 컬럼이 없는 스키마에서는 없다고 답한다")
    void detectsAMissingCriticalColumn() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            String original = connection.getCatalog();
            try {
                connection.setCatalog("information_schema");
                VerificationRuleJdbcAdapter probe = new VerificationRuleJdbcAdapter(
                        JdbcClient.create(new SingleConnectionDataSource(connection, true)));

                assertThat(probe.missingCriticalColumns())
                        .as("이게 비면 origin 없는 구 데이터셋이 기동을 통과한다")
                        .containsExactly("verification_runs.origin");
            } finally {
                connection.setCatalog(original);
            }
            assertThat(connection.getCatalog()).isEqualTo(original);
        }
    }

    /**
     * 인덱스 축도 두 방향을 본다. {@code V2026082513}·{@code V2026082514} 가 판 둘이
     * 갖춰진 스키마에서 비는 것, 그리고 그 인덱스가 없는 스키마에서 실제로 <b>없다고</b>
     * 답하는 것. 뒤 축이 없으면 {@code return List.of()} 로 바꿔도 초록이다.
     */
    @Test
    @DisplayName("갖춰진 스키마에서는 없는 인덱스가 없다")
    void reportsNoMissingIndexOnAMigratedSchema() {
        assertThat(adapter.missingCriticalIndexes()).isEmpty();
    }

    @Test
    @DisplayName("그 인덱스가 없는 스키마에서는 없다고 답한다")
    void detectsMissingCriticalIndexes() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            String original = connection.getCatalog();
            try {
                connection.setCatalog("information_schema");
                VerificationRuleJdbcAdapter probe = new VerificationRuleJdbcAdapter(
                        JdbcClient.create(new SingleConnectionDataSource(connection, true)));

                assertThat(probe.missingCriticalIndexes())
                        .as("이게 비면 인덱스 없는 스키마가 기동을 통과하고, 조용히 느려진다")
                        .containsExactlyInAnyOrder(
                                "BATCH_JOB_EXECUTION.IX_JOB_EXEC_STATUS_END(STATUS,END_TIME)",
                                "BATCH_JOB_EXECUTION.IX_JOB_EXEC_CREATE_TIME(CREATE_TIME)");
            } finally {
                connection.setCatalog(original);
            }
            assertThat(connection.getCatalog()).isEqualTo(original);
        }
    }

    @Test
    @DisplayName("얼린 사용 상한 위로 asOf 이하 사용이 끼어들면 잡는다")
    void detectsUsagesAddedAboveFrozenBoundary() {
        long used = data.issuance(IssuanceStatus.USED);
        data.usage(used, AS_OF.minusHours(3), null);
        long frozen = adapter.latestUsageId(AS_OF);

        assertThat(adapter.hasUsagesAddedAbove(frozen, AS_OF))
                .as("아직 아무것도 안 끼어들었다")
                .isFalse();

        // 얼린 뒤에 들어온 행. 시각은 asOf 이하라 V5 의 답을 바꾼다.
        // **다른 발급건에 심는다** — uk_issuance_usages_active 가 발급건 하나에 활성
        // 사용 둘을 막는다(main 의 V8). 그 제약이 이 검사의 대상은 아니다.
        long other = data.issuance(IssuanceStatus.USED);
        data.usage(other, AS_OF.minusHours(1), null);

        assertThat(adapter.hasUsagesAddedAbove(frozen, AS_OF))
                .as("V5 는 얼린 상한까지 접은 값을 읽는데 이 행이 답을 바꾼다 — "
                        + "지문은 이 축을 안 봐서 같은 지문에 다른 검출이 나온다")
                .isTrue();
    }

    @Test
    @DisplayName("취소로 들어온 행도 잡는다 — 활성 판정 술어를 쓰면 안 되는 이유다")
    void detectsCanceledUsageAddedAboveFrozenBoundary() {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);
        long frozen = adapter.latestUsageId(AS_OF);

        // canceled_at 이 있어 "지금 활성" 은 아니지만, V5 가 세는 값은 바뀐다.
        data.usage(issuanceId, AS_OF.minusHours(2), AS_OF.minusHours(1));

        assertThat(adapter.hasUsagesAddedAbove(frozen, AS_OF))
                .as("canceled_at 을 함께 보면 이 행을 놓친다 — 그래서 활성 술어를 안 쓴다")
                .isTrue();
    }

    @Test
    @DisplayName("asOf 뒤에 쓰인 사용은 안 잡는다 — 그 축은 리플레이 밖이다")
    void ignoresUsagesAfterAsOf() {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);
        long frozen = adapter.latestUsageId(AS_OF);

        data.usage(issuanceId, AS_OF.plusHours(1), null);

        assertThat(adapter.hasUsagesAddedAbove(frozen, AS_OF))
                .as("asOf 이후 행은 어차피 이 실행의 판정 대상이 아니다")
                .isFalse();
    }

    @Test
    @DisplayName("사용이 하나도 없으면 상한이 0 이고, 그 위로 들어오는 것을 잡는다")
    void treatsMissingUsagesAsZeroBoundary() {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);

        assertThat(adapter.latestUsageId(AS_OF))
                .as("행이 없으면 0 — 건너뛰면 가드가 막으려던 상황에서 정확히 꺼진다")
                .isZero();

        data.usage(issuanceId, AS_OF.minusHours(1), null);

        assertThat(adapter.hasUsagesAddedAbove(0L, AS_OF)).isTrue();
    }

    @Test
    @DisplayName("접속 스키마 이름을 답한다 — 메시지가 원인을 가르는 근거다")
    void reportsTheSchemaItIsLookingAt() {
        assertThat(adapter.currentSchema()).isEqualTo("app");
    }
}
