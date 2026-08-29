package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.domain.IssuanceStatus;
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

    /** 상한 축을 안 재는 기존 검사들이 쓰는 값 — "천장 없음". 그 축은 아래 전용 검사가 잰다. */
    private static final long NO_CEILING = Long.MAX_VALUE;
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
        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);
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

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("asOf 이후에 쓴 사용은 세지 않는다 — asOf 시점에는 아직 없던 행이다")
    void ignoreUsageAfterAsOf() {
        long issuanceId = seededState(IssuanceStatus.ISSUED);
        data.usage(issuanceId, AS_OF.plusMinutes(1), null);

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isZero();
    }

    @Test
    @DisplayName("asOf 이전에 취소된 사용은 세지 않는다")
    void ignoreUsageCanceledBeforeAsOf() {
        long issuanceId = seededState(IssuanceStatus.ISSUED);
        data.usage(issuanceId, AS_OF.minusHours(2), AS_OF.minusHours(1));

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isZero();
    }

    @Test
    @DisplayName("asOf 이후에 취소된 사용은 센다 — asOf 시점에는 살아 있었다")
    void countUsageCanceledAfterAsOf() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(2), AS_OF.plusHours(1));

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("asOf 와 같은 시각에 쓴 사용은 센다 — used_at 경계는 포함이다")
    void countUsageExactlyAtAsOf() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF, null);

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("asOf 와 같은 시각에 취소된 사용은 세지 않는다 — canceled_at 경계는 배제다")
    void ignoreUsageCanceledExactlyAtAsOf() {
        long issuanceId = seededState(IssuanceStatus.ISSUED);
        data.usage(issuanceId, AS_OF.minusHours(1), AS_OF);

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isZero();
    }

    /**
     * <b>CLEAN 스키마에서는 심을 수가 없다(CY-744).</b> main 의 {@code V8} 이
     * {@code uk_issuance_usages_active} 로 <i>"발급건 하나에 활성 사용은 하나"</i> 를
     * DB 에서 막는다 — 그게 정확히 V5(DOUBLE_USE)가 검출하는 오염이라, 이제
     * <b>정상셋에서는 구조적으로 못 생긴다.</b>
     *
     * <p>그래서 세는 로직은 <b>취소된 사용을 섞어</b> 잰다 — 활성 하나 + 취소 하나를 심고
     * 활성만 세는지 본다. 여럿을 세는 갈래는 CORRUPT 스키마의 몫이고,
     * {@code V9999999999__drop_clean_only_constraints.sql} 이 그 제약을 뗀다.
     *
     * <p>⚠️ 이 변화는 <b>V5 규칙을 지우는 근거가 아니다.</b> 오염셋은 제약 없이 만들어지고,
     * 운영 DB 도 그 제약이 생기기 전 행을 갖고 있을 수 있다. 규칙은 그대로 둔다.
     */
    @Test
    @DisplayName("취소된 사용은 안 세고 활성만 센다 — CLEAN 은 활성 둘을 아예 못 만든다")
    void countOnlyActiveUsages() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(3), AS_OF.minusHours(2));
        data.usage(issuanceId, AS_OF.minusHours(2), null);

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
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

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
        assertThat(activeUsageCountOf(otherRunId, issuanceId)).isZero();
    }

    @Test
    @DisplayName("활성 사용이 없는 발급건은 0 으로 남는다")
    void leaveZeroWhenNoActiveUsage() {
        long used = seededState(IssuanceStatus.USED);
        long untouched = seededState(IssuanceStatus.ISSUED);
        data.usage(used, AS_OF.minusHours(1), null);

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(used)).isEqualTo(1);
        assertThat(activeUsageCount(untouched)).isZero();
    }

    @Test
    @DisplayName("두 번 채워도 값이 그대로다 — 재시작해도 사용 건수가 흔들리지 않는다")
    void applyUsageCountsTwice() {
        long issuanceId = seededState(IssuanceStatus.USED);
        data.usage(issuanceId, AS_OF.minusHours(1), null);

        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);
        adapter.applyActiveUsageCounts(runId, AS_OF, NO_CEILING);

        assertThat(activeUsageCount(issuanceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("시드를 비운 뒤에도 같은 인스턴스로 다시 심을 수 있다 — 캐시가 지운 FK 를 가리키면 안 된다")
    void reuseSeedAfterClear() {
        data.issuance(IssuanceStatus.ISSUED);

        data.clear();

        assertThatCode(() -> data.issuance(IssuanceStatus.ISSUED)).doesNotThrowAnyException();
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
