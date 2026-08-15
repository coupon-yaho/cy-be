package com.kafkick.storage.verification;

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
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.replay.ReplayResult;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

@RepositoryTest
@Import({AsOfStateJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class AsOfStateJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);
    private static final LocalDateTime EVENT_AT = LocalDateTime.of(2026, 8, 15, 13, 0);

    @Autowired
    private AsOfStateJdbcAdapter adapter;

    @Autowired
    private VerificationRunJdbcAdapter runAdapter;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed data;
    private long runId;

    @BeforeEach
    void setUp() {
        data = new VerificationSeed(jdbcClient);
        runId = newRun(1);
    }

    @Test
    @DisplayName("접기 결과를 쌓으면 asof_state 에 그대로 들어간다")
    void appendReplayResults() {
        long issuanceId = data.issuance(IssuanceStatus.USED);

        adapter.appendAll(runId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.USED, 42L, EVENT_AT, List.of())));

        assertThat(state(issuanceId)).isEqualTo("USED");
        assertThat(lastHistoryId(issuanceId)).isEqualTo(42L);
        assertThat(activeUsageCount(issuanceId)).isZero();
    }

    @Test
    @DisplayName("같은 발급건을 다시 쌓아도 죽지 않고 덮어쓴다 — 청크가 죽은 지점부터 다시 돈다")
    void rewriteOnRestart() {
        long issuanceId = data.issuance(IssuanceStatus.ISSUED);

        adapter.appendAll(runId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.ISSUED, 10L, EVENT_AT, List.of())));
        adapter.appendAll(runId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.USED, 11L, EVENT_AT, List.of())));

        assertThat(rowCount()).isEqualTo(1);
        assertThat(state(issuanceId)).isEqualTo("USED");
        assertThat(lastHistoryId(issuanceId)).isEqualTo(11L);
    }

    @Test
    @DisplayName("다시 쌓아도 이미 채운 사용 건수를 0 으로 되돌리지 않는다")
    void keepUsageCountOnRewrite() {
        long issuanceId = data.issuance(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(1), null);

        adapter.appendAll(runId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.USED, 10L, EVENT_AT, List.of())));
        adapter.applyActiveUsageCounts(runId, AS_OF);
        adapter.appendAll(runId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.USED, 11L, EVENT_AT, List.of())));

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("빈 목록은 아무것도 쓰지 않는다")
    void appendNothingForEmptyList() {
        adapter.appendAll(runId, List.of());

        assertThat(rowCount()).isZero();
    }

    @Test
    @DisplayName("취소되지 않은 사용은 활성으로 센다")
    void countUncanceledUsage() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(1), null);

        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("asOf 이후에 쓴 사용은 세지 않는다 — asOf 시점에는 아직 없던 행이다")
    void ignoreUsageAfterAsOf() {
        long issuanceId = seededState(IssuanceStatus.ISSUED);
        data.usage(issuanceId, AS_OF.plusMinutes(1), null);

        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(issuanceId)).isZero();
    }

    @Test
    @DisplayName("asOf 이전에 취소된 사용은 세지 않는다")
    void ignoreUsageCanceledBeforeAsOf() {
        long issuanceId = seededState(IssuanceStatus.ISSUED);
        data.usage(issuanceId, AS_OF.minusHours(2), AS_OF.minusHours(1));

        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(issuanceId)).isZero();
    }

    @Test
    @DisplayName("asOf 이후에 취소된 사용은 센다 — asOf 시점에는 살아 있었다")
    void countUsageCanceledAfterAsOf() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(2), AS_OF.plusHours(1));

        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("활성 사용이 여럿이면 그만큼 센다 — V5 가 이 숫자로 이중 사용을 잡는다")
    void countMultipleActiveUsages() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(3), null);
        data.usage(issuanceId, AS_OF.minusHours(2), null);

        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(2);
    }

    @Test
    @DisplayName("다른 run 의 행은 건드리지 않는다 — run 마다 재생성한다")
    void leaveOtherRunsAlone() {
        long issuanceId = data.issuance(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(1), null);

        long otherRunId = newRun(2);
        adapter.appendAll(runId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.USED, 10L, EVENT_AT, List.of())));
        adapter.appendAll(otherRunId, List.of(
                new ReplayResult(issuanceId, IssuanceStatus.USED, 10L, EVENT_AT, List.of())));

        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
        assertThat(activeUsageCountOf(otherRunId, issuanceId)).isZero();
    }

    @Test
    @DisplayName("활성 사용이 없는 발급건은 0 으로 남는다")
    void leaveZeroWhenNoActiveUsage() {
        long used = seededState(IssuanceStatus.USED);
        long untouched = seededState(IssuanceStatus.ISSUED);
        data.usage(used, AS_OF.minusHours(1), null);

        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(used)).isEqualTo(1);
        assertThat(activeUsageCount(untouched)).isZero();
    }

    @Test
    @DisplayName("두 번 채워도 값이 그대로다 — 재시작해도 사용 건수가 흔들리지 않는다")
    void applyUsageCountsTwice() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(1), null);

        adapter.applyActiveUsageCounts(runId, AS_OF);
        adapter.applyActiveUsageCounts(runId, AS_OF);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    private long seededState(IssuanceStatus status) {
        long issuanceId = data.issuance(status);
        adapter.appendAll(runId, List.of(
                new ReplayResult(issuanceId, status, 1L, EVENT_AT, List.of())));
        return issuanceId;
    }

    private long newRun(int attempt) {
        return runAdapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, AS_OF)).id();
    }

    private String state(long issuanceId) {
        return jdbcClient.sql("SELECT state FROM asof_state WHERE run_id = :runId AND coupon_id = :id")
                .param("runId", runId)
                .param("id", issuanceId)
                .query(String.class)
                .single();
    }

    private long lastHistoryId(long issuanceId) {
        return jdbcClient.sql(
                        "SELECT last_history_id FROM asof_state WHERE run_id = :runId AND coupon_id = :id")
                .param("runId", runId)
                .param("id", issuanceId)
                .query(Long.class)
                .single();
    }

    private int activeUsageCount(long issuanceId) {
        return activeUsageCountOf(runId, issuanceId);
    }

    private int activeUsageCountOf(long targetRunId, long issuanceId) {
        return jdbcClient.sql(
                        "SELECT active_usage_count FROM asof_state WHERE run_id = :runId AND coupon_id = :id")
                .param("runId", targetRunId)
                .param("id", issuanceId)
                .query(Integer.class)
                .single();
    }

    private int rowCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM asof_state WHERE run_id = :runId")
                .param("runId", runId)
                .query(Integer.class)
                .single();
    }
}
