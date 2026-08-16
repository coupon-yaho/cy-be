package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.replay.ReplayResult;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

@RepositoryTest
@Import({AsOfStateJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class VerificationRuleJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);
    private static final int LIMIT = 1000;

    @Autowired
    private AsOfStateJdbcAdapter asOfStates;

    @Autowired
    private VerificationRunJdbcAdapter runAdapter;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationRuleJdbcAdapter adapter;
    private VerificationSeed data;
    private long runId;

    @BeforeEach
    void setUp() {
        adapter = new VerificationRuleJdbcAdapter(jdbcClient, 600_000L);
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
    @DisplayName("상한이 0 이하면 거부한다")
    void rejectNonPositiveLimit() {
        assertThatThrownBy(() -> adapter.findReplayMismatches(runId, AS_OF, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("검출 상한은 1 이상");
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

    @Test
    @DisplayName("질의 상한이 0 이하면 거부한다")
    void rejectNonPositiveQueryTimeout() {
        assertThatThrownBy(() -> new VerificationRuleJdbcAdapter(jdbcClient, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("질의 상한은 1ms 이상");
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
}
