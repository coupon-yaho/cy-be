package com.kafkick.core.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class VerificationRunTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);
    private static final LocalDateTime STARTED_AT = LocalDateTime.of(2026, 8, 15, 14, 0, 1);

    @Test
    @DisplayName("전수 검증을 시작하면 판정과 증적이 아직 비어 있다")
    void startFullRun() {
        VerificationRun run = fullRun();

        assertThat(run.id()).isNull();
        assertThat(run.verdict()).isNull();
        assertThat(run.findingsChecksum()).isNull();
        assertThat(run.datasetFingerprint()).isNull();
        assertThat(run.findingCount()).isZero();
        assertThat(run.finishedAt()).isNull();
    }

    @Test
    @DisplayName("합격 판정을 지는 것은 전수 실행뿐이다")
    void onlyFullRunDecidesVerdict() {
        assertThat(fullRun().decidesVerdict()).isTrue();
        assertThat(incrementalRun().decidesVerdict()).isFalse();
    }

    /**
     * <b>종료가 시작보다 앞서면 소요 시간이 음수가 된다.</b> 가드는 있었지만 테스트가 없어,
     * 지우면 아무도 모르는 상태였다.
     *
     * <p><b>같은 시각은 합법이다</b> — {@code finalizeRunStep} 이 판정 Step 의 시작 시각을
     * 종료로 쓰므로 잡이 짧으면 시작과 같아질 수 있고, 이 저장소의 여러 테스트가 실제로
     * 그렇게 쓴다. 여기서 막는 것은 <b>앞선</b> 경우뿐이라 {@code isBefore} 가 맞다.
     * 1마이크로초가 {@code datetime(6)} 의 최소 단위다.
     */
    @Test
    @DisplayName("종료 시각이 시작보다 앞서면 거부한다")
    void rejectFinishedBeforeStarted() {
        VerificationRun run = fullRun();

        assertThatThrownBy(() -> run.finish(
                VerdictType.PASS, 0, "checksum", "fingerprint",
                run.startedAt().minusNanos(1_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("종료 시각은 시작 시각보다 앞설 수 없습니다");
    }

    @Test
    @DisplayName("판정을 확정하면 검출 건수와 증적이 채워진다")
    void finishRun() {
        VerificationRun finished = fullRun().finish(
                VerdictType.PASS, 0, "checksum", "fingerprint",
                LocalDateTime.of(2026, 8, 15, 14, 3, 4)
        );

        assertThat(finished.verdict()).isEqualTo(VerdictType.PASS);
        assertThat(finished.findingCount()).isZero();
        assertThat(finished.findingsChecksum()).isEqualTo("checksum");
        assertThat(finished.datasetFingerprint()).isEqualTo("fingerprint");
        assertThat(finished.finishedAt()).isNotNull();
    }

    @Test
    @DisplayName("통계 상태는 판정과 따로 채운다 — 오염셋은 통계를 건너뛴다")
    void keepStatsStatusSeparateFromVerdict() {
        VerificationRun corrupt = VerificationRun.start(
                        AS_OF, null, ScopeType.FULL, DatasetType.CORRUPT, 1, STARTED_AT)
                .finish(VerdictType.PASS, 800, "checksum", "fingerprint", STARTED_AT)
                .withStatsStatus(StatsStatus.SKIPPED);

        assertThat(corrupt.verdict()).isEqualTo(VerdictType.PASS);
        assertThat(corrupt.statsStatus()).isEqualTo(StatsStatus.SKIPPED);
    }

    @Test
    @DisplayName("증분 검증에 시작 시각이 없으면 거부한다")
    void rejectIncrementalWithoutFromTs() {
        assertThatThrownBy(() -> VerificationRun.start(
                AS_OF, null, ScopeType.INCREMENTAL, DatasetType.CLEAN, 1, STARTED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("증분 검증에는 시작 시각이 필요합니다.");
    }

    @Test
    @DisplayName("전수 검증에 시작 시각을 주면 거부한다 — 범위가 모순된다")
    void rejectFullWithFromTs() {
        assertThatThrownBy(() -> VerificationRun.start(
                AS_OF, AS_OF.minusMinutes(10), ScopeType.FULL, DatasetType.CLEAN, 1, STARTED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("전수 검증에는 시작 시각을 지정할 수 없습니다.");
    }

    @Test
    @DisplayName("재실행 횟수가 1 미만이면 거부한다 — 없으면 같은 asOf 로 재실행이 차단된다")
    void rejectAttemptBelowOne() {
        assertThatThrownBy(() -> VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 0, STARTED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("재실행 횟수는 1 이상이어야 합니다.");
    }

    @Test
    @DisplayName("검증 기준 시각이 없으면 거부한다")
    void rejectNullAsOf() {
        assertThatThrownBy(() -> VerificationRun.start(
                null, null, ScopeType.FULL, DatasetType.CLEAN, 1, STARTED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("검증 기준 시각이 필요합니다.");
    }

    @Test
    @DisplayName("기준 시각이 실행 시작보다 미래면 거부한다 — 아직 일어나지 않은 일을 기준으로 삼는다")
    void rejectAsOfAfterStartedAt() {
        assertThatThrownBy(() -> VerificationRun.start(
                STARTED_AT.plusSeconds(1), null, ScopeType.FULL, DatasetType.CLEAN, 1, STARTED_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("실행 시작 시각을 넘을 수 없습니다");
    }

    @Test
    @DisplayName("기준 시각이 실행 시작과 같으면 받는다 — 경계는 포함이다")
    void acceptAsOfEqualToStartedAt() {
        VerificationRun run = VerificationRun.start(
                STARTED_AT, null, ScopeType.FULL, DatasetType.CLEAN, 1, STARTED_AT);

        assertThat(run.asOf()).isEqualTo(STARTED_AT);
    }

    @Test
    @DisplayName("식별자 없이 복원하면 거부한다")
    void rejectRestoreWithoutId() {
        assertThatThrownBy(() -> VerificationRun.restore(
                null, AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 1,
                null, null, 0, null, null, STARTED_AT, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("복원하려면 검증 실행 ID가 필요합니다.");
    }

    private static VerificationRun fullRun() {
        return VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, 1, STARTED_AT);
    }

    private static VerificationRun incrementalRun() {
        return VerificationRun.start(
                AS_OF, AS_OF.minusMinutes(10), ScopeType.INCREMENTAL, DatasetType.CLEAN, 1, STARTED_AT);
    }
}
