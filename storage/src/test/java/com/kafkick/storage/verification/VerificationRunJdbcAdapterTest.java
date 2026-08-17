package com.kafkick.storage.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.exception.VerificationErrorCode;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(VerificationRunJdbcAdapter.class)
class VerificationRunJdbcAdapterTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);
    private static final LocalDateTime STARTED_AT = LocalDateTime.of(2026, 8, 15, 14, 0, 1);

    @Autowired
    private VerificationRunJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    @DisplayName("실행을 저장하면 식별자가 채워진다 — asof_state 가 이 값을 FK 로 문다")
    void saveAssignsId() {
        VerificationRun saved = adapter.save(fullRun());

        assertThat(saved.id()).isNotNull().isPositive();
    }

    @Test
    @DisplayName("저장한 실행을 그대로 다시 읽는다")
    void findSavedRun() {
        VerificationRun saved = adapter.save(fullRun());

        Optional<VerificationRun> found = adapter.findById(saved.id());

        assertThat(found).isPresent();
        assertThat(found.get().asOf()).isEqualTo(AS_OF);
        assertThat(found.get().scope()).isEqualTo(ScopeType.FULL);
        assertThat(found.get().dataset()).isEqualTo(DatasetType.CLEAN);
        assertThat(found.get().attempt()).isEqualTo(1);
        assertThat(found.get().verdict()).isNull();
        assertThat(found.get().finishedAt()).isNull();
    }

    @Test
    @DisplayName("증분 실행의 시작 시각이 왕복해도 보존된다")
    void keepFromTsThroughRoundTrip() {
        LocalDateTime fromTs = AS_OF.minusMinutes(10);
        VerificationRun saved = adapter.save(VerificationRun.start(
                AS_OF, fromTs, ScopeType.INCREMENTAL, DatasetType.CLEAN, 1, STARTED_AT));

        Optional<VerificationRun> found = adapter.findById(saved.id());

        assertThat(found).isPresent();
        assertThat(found.get().fromTs()).isEqualTo(fromTs);
    }

    @Test
    @DisplayName("판정과 증적을 갱신하면 조회에 반영된다")
    void updateVerdictAndEvidence() {
        VerificationRun saved = adapter.save(fullRun());

        adapter.update(saved.finish(
                VerdictType.FAIL, 3, "c".repeat(64), "f".repeat(64),
                LocalDateTime.of(2026, 8, 15, 14, 3, 4)));

        VerificationRun found = adapter.findById(saved.id()).orElseThrow();
        assertThat(found.verdict()).isEqualTo(VerdictType.FAIL);
        assertThat(found.findingCount()).isEqualTo(3);
        assertThat(found.findingsChecksum()).isEqualTo("c".repeat(64));
        assertThat(found.datasetFingerprint()).isEqualTo("f".repeat(64));
        assertThat(found.finishedAt()).isNotNull();
    }

    @Test
    @DisplayName("판정과 통계 상태를 한 번의 update 로 함께 쓴다")
    void writeVerdictAndStatsStatusTogether() {
        VerificationRun saved = adapter.save(fullRun());

        adapter.update(saved.finish(VerdictType.PASS, 0, null, null, STARTED_AT)
                .withStatsStatus(StatsStatus.SKIPPED));

        VerificationRun found = adapter.findById(saved.id()).orElseThrow();
        assertThat(found.verdict()).isEqualTo(VerdictType.PASS);
        assertThat(found.statsStatus()).isEqualTo(StatsStatus.SKIPPED);
    }

    /**
     * <b>이름이 말하는 메서드를 실제로 부른다.</b> 위 테스트가 {@code updateStatsStatus} 라는
     * 이름을 달고 {@code update()} 만 불러, 통계 Step 이 쓸 전용 경로는 한 번도 안 돌았다.
     */
    @Test
    @DisplayName("통계 상태만 따로 갱신한다 — 판정은 건드리지 않는다")
    void updateStatsStatusAlone() {
        VerificationRun saved = adapter.save(fullRun());
        adapter.update(saved.finish(VerdictType.PASS, 0, null, null, STARTED_AT));

        adapter.updateStatsStatus(saved.id(), StatsStatus.COMPLETE);

        VerificationRun found = adapter.findById(saved.id()).orElseThrow();
        assertThat(found.statsStatus()).isEqualTo(StatsStatus.COMPLETE);
        assertThat(found.verdict())
                .as("통계 갱신이 판정을 지우면 게이트가 읽을 값이 사라진다")
                .isEqualTo(VerdictType.PASS);
    }

    /**
     * <b>조용히 넘어가면 판정이 안 써진 채 잡이 COMPLETED 로 끝난다.</b> 게이트가 읽는 것은
     * {@code verification_runs.verdict} 라, 그 행이 NULL 이면 판정 불가가 된다.
     *
     * <p>코드가 <b>500대</b>인 것도 함께 지킨다. 404 로 두면 클라이언트 입력 오류로 분류돼
     * 재고 소진 같은 정상 흐름 예외와 같은 취급을 받는다 — 이건 데이터 정합 사고다.
     */
    @Test
    @DisplayName("지워진 실행을 갱신하면 RUN_ROW_VANISHED 로 죽는다")
    void rejectUpdateOfDeletedRun() {
        VerificationRun saved = adapter.save(fullRun());
        jdbcClient.sql("DELETE FROM verification_runs WHERE id = :id")
                .param("id", saved.id())
                .update();

        assertThatThrownBy(() -> adapter.update(
                saved.finish(VerdictType.PASS, 0, null, null, STARTED_AT)))
                .isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode())
                .isEqualTo(VerificationErrorCode.RUN_ROW_VANISHED);
    }

    @Test
    @DisplayName("없는 실행의 통계 상태를 갱신해도 죽는다 — 조용히 넘어가면 통계가 빈 채 끝난다")
    void rejectStatsUpdateOfMissingRun() {
        assertThatThrownBy(() -> adapter.updateStatsStatus(999_999L, StatsStatus.SKIPPED))
                .isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode())
                .isEqualTo(VerificationErrorCode.RUN_ROW_VANISHED);
    }

    @Test
    @DisplayName("없는 실행을 찾으면 빈 값이다")
    void findMissingRun() {
        assertThat(adapter.findById(999_999L)).isEmpty();
    }

    @Test
    @DisplayName("식별자 없이 갱신하면 거부한다")
    void rejectUpdateWithoutId() {
        assertThatThrownBy(() -> adapter.update(fullRun()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("갱신하려면 검증 실행 ID가 필요합니다.");
    }

    @Test
    @DisplayName("같은 파라미터로 두 번 저장하면 uk_run_params 가 막는다 — attempt 를 올려야 재실행된다")
    void rejectDuplicateRunParams() {
        adapter.save(fullRun());

        assertThatThrownBy(() -> adapter.save(fullRun()))
                .isInstanceOf(org.springframework.dao.DuplicateKeyException.class);
    }

    @Test
    @DisplayName("attempt 를 올리면 같은 asOf 로 다시 실행된다 — 결정론 증명의 전제다")
    void allowSameAsOfWithNextAttempt() {
        adapter.save(fullRun());

        VerificationRun second = adapter.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 2, STARTED_AT));

        assertThat(second.id()).isNotNull();
        assertThat(second.attempt()).isEqualTo(2);
    }

    @Test
    @DisplayName("asof_state 가 실행을 FK 로 문다 — 실행 행이 먼저 있어야 한다")
    void asofStateReferencesRun() {
        VerificationRun saved = adapter.save(fullRun());

        int inserted = jdbcClient.sql("""
                        INSERT INTO asof_state (run_id, coupon_id, state, active_usage_count)
                        VALUES (:runId, 1, 'ISSUED', 0)
                        """)
                .param("runId", saved.id())
                .update();

        assertThat(inserted).isEqualTo(1);
    }

    private static VerificationRun fullRun() {
        return VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 1, STARTED_AT);
    }
}
